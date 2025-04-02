package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/modfin/clix"
	"github.com/modfin/henry/mapz"
	"github.com/modfin/henry/slicez"

	"github.com/modfin/creek"
	"github.com/modfin/creek-pg-client/internal/agent"
	"github.com/modfin/creek-pg-client/internal/metrics"
	"github.com/modfin/creek-pg-client/internal/utils"
	"github.com/modfin/henry/chanz"
	"github.com/olekukonko/tablewriter"

	"github.com/modfin/creek-pg-client/internal/config"
	"github.com/modfin/creek-pg-client/internal/dao"

	"github.com/sirupsen/logrus"
	cli "github.com/urfave/cli/v3"
)

type EnumValue struct {
	Enum     []config.SnapMode
	Default  config.SnapMode
	selected config.SnapMode
}

func (e *EnumValue) Set(value string) error {
	for _, enum := range e.Enum {
		if string(enum) == value {
			e.selected = config.SnapMode(value)
			return nil
		}
	}

	return fmt.Errorf("allowed values are %s", strings.Join(slicez.Map(e.Enum, func(a config.SnapMode) string {
		return string(a)
	}), ", "))
}

func (e EnumValue) String() string {
	if e.selected == "" {
		return string(e.Default)
	}
	return string(e.selected)
}

func main() {

	ctx, cancel := context.WithCancel(context.Background())

	term := make(chan os.Signal, 1)
	signal.Notify(term, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-term
		cancel()
		<-time.After(2 * time.Second)
		os.Exit(1)
	}()

	cmd := &cli.Command{
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "nats-uri",
				Sources: cli.EnvVars("NATS_URI"),
			},
			&cli.StringFlag{
				Name:  "nats-namespace",
				Value: "CREEK",
			},
			&cli.StringFlag{
				Name:    "db-uri",
				Sources: cli.EnvVars("DB_URI"),
			},
			&cli.StringFlag{
				Name:    "db-namespace",
				Sources: cli.EnvVars("DB_NAMESPACE"),
				Value:   "_creek_consumer",
			},
			&cli.StringFlag{
				Name:    "log-level",
				Sources: cli.EnvVars("LOG_LEVEL"),
				Value:   "info",
			},
			&cli.FloatFlag{
				Name:    "log-rate",
				Value:   30,
				Sources: cli.EnvVars("LOG_RATE"),
			},
			&cli.IntFlag{
				Name:    "log-burst",
				Value:   10,
				Sources: cli.EnvVars("LOG_BURST"),
			},
		},
		Commands: []*cli.Command{
			{
				Name: "create-schemas",
				Description: "Creates tables in the consumer database with the latest schemas for the specified source tables. " +
					"The target database is implicit. Multiple schemas may be provided.",
				UsageText: "clientd create-schemas source_db.namespace.table:namespace.table source_db.namespace.table:namespace.table ...",
				Action:    CreateSchemas,
			},
			{
				Name: "snapshot",
				Description: "Requests and takes new snapshots of the specified tables and applies it to the specified target tables. " +
					"The target database is implicit. Multiple tables may be provided and will be run sequentially.",
				UsageText: "clientd snapshot source_db.namespace.table:namespace.table source_db.namespace.table:namespace.table ...",
				Action:    TakeSnapshot,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "mode",
						Value: string(config.DefaultSnapOption()),
						Validator: func(value string) error {
							if !slicez.Contains(config.ValidSnapOptions(), config.SnapMode(value)) {
								return fmt.Errorf("invalid snapshot mode: %s", value)
							}
							return nil
						},
					},
				},
			},
			{
				Name:   "apply-snapshot",
				Action: ApplySnapshot,
				Description: "Applies a specified snapshot from a source table to the specified target table. " +
					"The target database is implicit.",
				ArgsUsage: "source_db.namespace.table:namespace.table",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "name",
						Required: true,
						Usage:    "topic name",
					},
					&cli.StringFlag{
						Name:  "mode",
						Value: string(config.DefaultSnapOption()),
						Validator: func(value string) error {
							if !slicez.Contains(config.ValidSnapOptions(), config.SnapMode(value)) {
								return fmt.Errorf("invalid snapshot mode: %s", value)
							}
							return nil
						},
					},
				},
			},
			{
				Name:        "list-snapshots",
				Description: "Lists existing available snapshots for the specified table.",
				ArgsUsage:   "source_db.namespace.table",
				Action:      ListSnapshots,
			},
			{
				Name:        "list-tables",
				Description: "Lists tables that are configured to listen to.",
				Action:      ListTables,
			},
			{
				Name:        "add-tables",
				Description: "Adds tables to listen for wal events to.",
				ArgsUsage:   "source_db.namespace.table:namespace.table ...",
				Action:      AddTables,
			},
			{
				Name:        "remove-tables",
				Description: "Removes (sets inactive) tables to listen for wal events to.",
				ArgsUsage:   "namespace.table ...",
				Action:      RemoveTables,
			},
			{
				Name:        "serve",
				Description: "Listens to wal messages and applies changes to target tables.",
				Flags: []cli.Flag{
					&cli.IntFlag{
						Name:    "prometheus-port",
						Sources: cli.EnvVars("PROMETHEUS_PORT"),
						Value:   8080,
					},
				},
				Action: Serve,
			},
		},
	}

	if err := cmd.Run(ctx, os.Args); err != nil {
		logrus.Fatal(err)
	}
}

func RemoveTables(ctx context.Context, cmd *cli.Command) error {
	args := cmd.Args().Slice()
	if len(args) == 0 {
		return cli.Exit("Please provide a target table", 1)
	}
	cfg, err := initAndVerifyConfig(cmd)
	if err != nil {
		return err
	}

	var targets []config.Target
	for _, arg := range args {
		target, err := config.ParseTarget(arg)
		if err != nil {
			logrus.Errorf("failed to parse target %s: %v", arg, err)
			continue
		}
		targets = append(targets, target)
	}

	db, err := dao.New(ctx, cfg.DbURI)
	if err != nil {
		return cli.Exit(fmt.Errorf("failed to connect to database: %w", err), 1)
	}

	for _, target := range targets {
		err = db.SetActive(target, false)
		if err != nil {
			logrus.Errorf("failed to set stream to inactive: %v", err)
			continue
		}
		logrus.Infof("set %s to inactive", target)
	}

	return nil
}

// TODO: confusing name
func AddTables(ctx context.Context, cmd *cli.Command) error {
	args := cmd.Args().Slice()
	if len(args) != 1 {
		return cli.Exit("Please provide a source and target table", 1)
	}
	cfg, err := initAndVerifyConfig(cmd)
	if err != nil {
		return err
	}

	mappings := make(map[config.Source]config.Target)
	for _, arg := range args {
		source, target, err := config.ParseTable(arg)
		if err != nil {
			logrus.Errorf("failed to parse tables %s: %v", arg, err)
			continue
		}
		mappings[source] = target
	}

	db, err := dao.New(ctx, cfg.DbURI)
	if err != nil {
		return cli.Exit(fmt.Errorf("failed to connect to database: %w", err), 1)
	}

	for source, target := range mappings {
		lsn, _, err := db.GetStreamLocation(source, target)
		if err != nil {
			logrus.Errorf("failed to persist stream location: %v", err)
			continue
		}
		err = db.SetActive(target, true)
		if err != nil {
			logrus.Errorf("failed to set stream to active: %v", err)
			continue
		}
		if lsn != "0/0" {
			logrus.Infof("a stream to %s already exists, set stream to active", target)
			continue
		}
		logrus.Infof("added %s -> %s", source, target)
	}

	return nil
}

func ListTables(ctx context.Context, cmd *cli.Command) error {
	cfg, err := initAndVerifyConfig(cmd)
	if err != nil {
		return err
	}

	db, err := dao.New(ctx, cfg.DbURI)
	if err != nil {
		return err
	}

	activeStreams, err := db.GetActiveStreams()
	if err != nil {
		return err
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetAutoWrapText(false)
	table.SetHeader([]string{"Source", "Target"})
	for _, streams := range activeStreams {
		for source, target := range streams {
			table.Append([]string{source.String(), target.String()})
		}
	}

	table.Render()

	return nil
}

func ListSnapshots(ctx context.Context, cmd *cli.Command) error {
	args := cmd.Args().Slice()
	if len(args) != 1 {
		return cli.Exit("Please provide a source", 1)
	}
	cfg, err := initAndVerifyConfig(cmd)
	if err != nil {
		return err
	}

	source, err := config.ParseSource(args[0])
	if err != nil {
		logrus.Errorf("failed to parse source table: %v", err)
		return cli.Exit("Failed to parse source table", 1)
	}

	client := creek.NewClient(cfg.NatsURI, cfg.NatsNamespace, source.DB())
	snaps, err := client.ListSnapshots(ctx, source.DB(), source.Name())
	if err != nil {
		return err
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetAutoWrapText(false)
	table.SetHeader([]string{"Topic name", "Timestamp", "Rows"})
	for _, snap := range snaps {
		table.Append([]string{snap.Name, snap.At.String(), fmt.Sprintf("%d", snap.Messages)})
	}

	table.Render()

	return nil
}

func ApplySnapshot(ctx context.Context, cmd *cli.Command) error {
	ctx, cancel := context.WithCancel(ctx)
	dbCtx, dbCancel := context.WithCancel(context.Background())
	defer dbCancel()

	args := cmd.Args().Slice()
	if len(args) != 1 {
		return fmt.Errorf("please provide a source and target table")
	}

	cfg, err := initAndVerifyConfig(cmd)
	if err != nil {
		return err
	}

	source, target, err := config.ParseTable(args[0])
	if err != nil {
		return fmt.Errorf("failed to parse source and target table: %w", err)
	}

	db, err := dao.New(dbCtx, cfg.DbURI)
	if err != nil {
		return fmt.Errorf("failed to initialize database: %w", err)
	}
	logrus.Info("successfully connected to database")

	creekStream := agent.NewStream(ctx, *cfg, source.DB(), db)

	_, err = creekStream.ApplySnapshot(cfg.SnapMode, source, target, cmd.String("name"))
	if err != nil {
		return fmt.Errorf("failed to take snapshot: %w", err)
	}

	allDone := chanz.EveryDone(
		ctx.Done(),
		creekStream.Done(),
	)

	for {
		select {
		case <-allDone:
			dbCancel()
			os.Exit(0)
		case <-creekStream.SnapsDone():
			cancel()
		}
	}
}

func TakeSnapshot(ctx context.Context, cmd *cli.Command) error {
	ctx, cancel := context.WithCancel(ctx)
	dbCtx, dbCancel := context.WithCancel(context.Background())
	defer dbCancel()

	args := cmd.Args().Slice()
	if len(args) == 0 {
		return cli.Exit("Please supply parameters", 1)
	}

	cfg, err := initAndVerifyConfig(cmd)
	if err != nil {
		return err
	}

	mappings := make(map[config.Target]config.Source)
	for _, arg := range args {
		source, target, err := config.ParseTable(arg)
		if err != nil {
			logrus.Errorf("failed to parse tables %s: %v", arg, err)
			continue
		}
		mappings[target] = source
	}

	db, err := dao.New(dbCtx, cfg.DbURI)
	if err != nil {
		logrus.Panicln("failed to initialize database: ", err)
	}
	logrus.Info("successfully connected to database")

	streams := make(map[config.Target]*agent.Agent)

	for target, source := range mappings {
		// TODO: cleanup the created streams
		streams[target] = agent.NewStream(ctx, *cfg, source.DB(), db)
		_, err = streams[target].NewSnapshot(cfg.SnapMode, source, target)
		if err != nil {
			logrus.Errorf("failed to take snapshot: %v", err)
		}
	}

	snapsDone := chanz.EveryDone(slicez.Map(mapz.Values(streams), func(a *agent.Agent) <-chan struct{} {
		return a.SnapsDone()
	})...)

	streamsDone := chanz.EveryDone(slicez.Map(mapz.Values(streams), func(a *agent.Agent) <-chan struct{} {
		return a.Done()
	})...)

	allDone := chanz.EveryDone(
		ctx.Done(),
		streamsDone,
	)

	for {
		select {
		case <-allDone:
			dbCancel()
			return nil
		case <-snapsDone:
			cancel()
		case <-time.After(2 * time.Second):
			logrus.Info("waiting for snapshots to complete")
		}
	}
}

func CreateSchemas(ctx context.Context, cmd *cli.Command) error {

	args := cmd.Args().Slice()
	if len(args) == 0 {
		return cli.Exit("Please supply parameters", 1)
	}
	mappings := make(map[config.Source]config.Target)
	for _, arg := range args {
		source, target, err := config.ParseTable(arg)
		if err != nil {
			logrus.Errorf("failed to parse tables %s: %v", arg, err)
			continue
		}
		mappings[source] = target
	}

	cfg, err := initAndVerifyConfig(cmd)
	if err != nil {
		return err
	}

	db, err := dao.New(ctx, cfg.DbURI)
	if err != nil {
		logrus.Panicln("failed to initialize database: ", err)
	}
	logrus.Info("successfully connected to database")

	for source, target := range mappings {
		creekStream := agent.NewStream(ctx, *cfg, source.DB(), db)
		err = creekStream.CreateSchema(ctx, source, target)
		if err != nil {
			logrus.Errorf("failed to create schema %s: %v", target.Name(), err)
			continue
		}
		logrus.Infof("Created table %s", target)
	}

	return nil
}

func Serve(ctx context.Context, cmd *cli.Command) error {
	cfg, err := initAndVerifyConfig(cmd)
	if err != nil {
		return err
	}

	dbCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := dao.New(dbCtx, cfg.DbURI)
	if err != nil {
		logrus.Panicln("failed to initialize database: ", err)
	}
	logrus.Info("successfully connected to database")

	go metrics.Start(ctx, cfg.PrometheusPort)

	activeStreams, err := db.GetActiveStreams()
	if err != nil {
		return fmt.Errorf("failed to get active streams: %w", err)
	}

	// one stream per target db
	// this wont accept new tables from new dbs when started
	streams := make(map[string]*agent.Agent)

	for sourceDb, dbStreams := range activeStreams {
		streams[sourceDb] = agent.NewStream(ctx, *cfg, sourceDb, db)
		streams[sourceDb].StartListenAPI()

		for source, target := range dbStreams {
			err := streams[sourceDb].AddWALTable(ctx, source, target)
			if err != nil {
				logrus.Errorf("failed to start streaming wal for table %s", target)
			}
		}
		// TODO: handle all streams

	}

	streamsDone := chanz.EveryDone(slicez.Map(mapz.Values(streams), func(a *agent.Agent) <-chan struct{} {
		return a.Done()
	})...)

	allDone := chanz.EveryDone(
		ctx.Done(),
		streamsDone,
	)

	for {
		select {
		case <-allDone:
			cancel()
			return nil
		}
	}
}

func initAndVerifyConfig(cmd *cli.Command) (*config.Config, error) {
	cfg := clix.Parse[config.Config](clix.V3(cmd))
	ll, err := logrus.ParseLevel(cfg.LogLevel)
	if err != nil {
		ll = logrus.InfoLevel
	}
	limitedWriter := utils.NewRateLimitedWriter(cfg.LogRate, cfg.LogBurst, ll)
	prometheusHook := metrics.MustNewPrometheusHook()
	logrus.SetLevel(ll)
	logrus.AddHook(prometheusHook)
	logrus.AddHook(limitedWriter) // Writes to stdout with rate limit
	logrus.SetOutput(io.Discard)  // Discard messages

	if cfg.DbURI == "" {
		return nil, errors.New("pg-uri is required")
	}
	return &cfg, nil
}
