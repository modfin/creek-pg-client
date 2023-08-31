package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/modfin/henry/slicez"

	"github.com/modfin/creek"
	"github.com/modfin/creek-pg-client/internal/metrics"
	"github.com/modfin/creek-pg-client/internal/stream"
	"github.com/modfin/henry/chanz"
	"github.com/olekukonko/tablewriter"

	"github.com/modfin/creek-pg-client/internal/config"
	"github.com/modfin/creek-pg-client/internal/dao"
	"github.com/urfave/cli/v2"

	"github.com/sirupsen/logrus"
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

	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "nats-uri",
				EnvVars: []string{"NATS_URI"},
			},
			&cli.StringFlag{
				Name:  "nats-namespace",
				Value: "CREEK",
			},
			&cli.StringFlag{
				Name:    "db-uri",
				EnvVars: []string{"DB_URI"},
			},
			&cli.StringFlag{
				Name:    "db-namespace",
				EnvVars: []string{"DB_NAMESPACE"},
				Value:   "_creek_consumer",
			},
			&cli.StringFlag{
				Name:    "log-level",
				EnvVars: []string{"LOG_LEVEL"},
				Value:   "info",
			},
			&cli.Float64Flag{
				Name:  "log-rate",
				Value: 30,
			},
			&cli.IntFlag{
				Name:  "log-burst",
				Value: 10,
			},
		},
		Commands: []*cli.Command{
			{
				Name: "create-schemas",
				Description: "Creates tables in the consumer database with the latest schemas for the specified source tables. " +
					"The target database is implicit. Multiple schemas may be provided.",
				UsageText: "clientd create-schemas source_db.namespace.table:namespace.table source_db.namespace.table:namespace.table ...",
				Action:    CreateSchemas(ctx),
			},
			{
				Name: "snapshot",
				Description: "Requests and takes new snapshots of the specified tables and applies it to the specified target tables. " +
					"The target database is implicit. Multiple tables may be provided and will be run sequentially.",
				UsageText: "clientd snapshot source_db.namespace.table:namespace.table source_db.namespace.table:namespace.table ...",
				Action:    TakeSnapshot(ctx, cancel),
				Flags: []cli.Flag{
					&cli.GenericFlag{
						Name: "mode",
						Value: &EnumValue{
							Enum:    config.ValidSnapOptions(),
							Default: config.DefaultSnapOption(),
						},
					},
				},
			},
			{
				Name:   "apply-snapshot",
				Action: ApplySnapshot(ctx, cancel),
				Description: "Applies a specified snapshot from a source table to the specified target table. " +
					"The target database is implicit.",
				ArgsUsage: "source_db.namespace.table:namespace.table",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "name",
						Required: true,
						Usage:    "topic name",
					},
					&cli.GenericFlag{
						Name: "mode",
						Value: &EnumValue{
							Enum:    config.ValidSnapOptions(),
							Default: config.DefaultSnapOption(),
						},
					},
				},
			},
			{
				Name:        "list-snapshots",
				Description: "Lists existing available snapshots for the specified table.",
				ArgsUsage:   "source_db.namespace.table",
				Action:      ListSnapshots(),
			},
			{
				Name:        "list-tables",
				Description: "Lists tables that are configured to listen to.",
				Action:      ListTables(ctx),
			},
			{
				Name:        "add-tables",
				Description: "Adds tables to listen for wal events to.",
				ArgsUsage:   "source_db.namespace.table:namespace.table ...",
				Action:      AddTables(ctx),
			},
			{
				Name:        "remove-tables",
				Description: "Removes (sets inactive) tables to listen for wal events to.",
				ArgsUsage:   "namespace.table ...",
				Action:      RemoveTables(ctx),
			},
			{
				Name:        "serve",
				Description: "Listens to wal messages and applies changes to target tables.",
				Flags: []cli.Flag{
					&cli.IntFlag{
						Name:    "prometheus-port",
						EnvVars: []string{"PROMETHEUS_PORT"},
						Value:   8080,
					},
				},
				Action: Serve(ctx),
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		logrus.Fatal(err)
	}
}

func RemoveTables(ctx context.Context) cli.ActionFunc {
	return func(c *cli.Context) error {
		args := c.Args().Slice()
		if len(args) == 0 {
			return cli.Exit("Please provide a target table", 1)
		}

		cfg := config.Get(c)

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
}

func AddTables(ctx context.Context) cli.ActionFunc {
	return func(c *cli.Context) error {
		args := c.Args().Slice()
		if len(args) != 1 {
			return cli.Exit("Please provide a source and target table", 1)
		}

		cfg := config.Get(c)

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
}

func ListTables(ctx context.Context) cli.ActionFunc {
	return func(c *cli.Context) error {
		cfg := config.Get(c)

		db, err := dao.New(ctx, cfg.DbURI)
		if err != nil {
			return cli.Exit(fmt.Errorf("failed to connect to database: %w", err), 1)
		}

		streams, err := db.GetActiveStreams()
		if err != nil {
			return cli.Exit(fmt.Errorf("failed to get active streams: %w", err), 1)
		}

		table := tablewriter.NewWriter(os.Stdout)
		table.SetAutoWrapText(false)
		table.SetHeader([]string{"Source", "Target"})
		for target, source := range streams {
			table.Append([]string{source.String(), target.String()})
		}

		table.Render()

		return nil
	}
}

func ListSnapshots() cli.ActionFunc {
	return func(c *cli.Context) error {
		args := c.Args().Slice()
		if len(args) != 1 {
			return cli.Exit("Please provide a source", 1)
		}
		cfg := config.Get(c)
		source, err := config.ParseSource(args[0])
		if err != nil {
			logrus.Errorf("failed to parse source table: %v", err)
			return cli.Exit("Failed to parse source table", 1)
		}

		client := creek.NewClient(cfg.NatsURI, cfg.NatsNamespace)

		conn, err := client.Connect()
		if err != nil {
			panic(err)
		}

		snaps, err := conn.ListSnapshots(context.Background(), source.DB(), source.Name())
		if err != nil {
			panic(err)
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
}

func ApplySnapshot(ctx context.Context, cancel context.CancelFunc) cli.ActionFunc {
	return func(c *cli.Context) error {
		dbCtx, dbCancel := context.WithCancel(context.Background())
		defer dbCancel()

		snapMode := config.SnapMode(c.String("mode"))

		args := c.Args().Slice()
		if len(args) != 1 {
			return cli.Exit("Please provide a source and target table", 1)
		}

		cfg := config.Get(c)

		mappings := make(map[config.Source]config.Target)
		source, target, err := config.ParseTable(args[0])
		if err != nil {
			return cli.Exit("Failed to parse source and target table", 1)
		}
		mappings[source] = target

		db, err := dao.New(dbCtx, cfg.DbURI)
		if err != nil {
			logrus.Panicln("failed to initialize database: ", err)
		}
		logrus.Info("successfully connected to database")

		creekStream, err := stream.NewStream(ctx, cfg, db)
		if err != nil {
			return err
		}

		_, err = creekStream.ApplySnapshot(snapMode, source, target, c.String("name"))
		if err != nil {
			logrus.Errorf("failed to take snapshot: %v", err)
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
}

func TakeSnapshot(ctx context.Context, cancel context.CancelFunc) cli.ActionFunc {
	return func(c *cli.Context) error {
		dbCtx, dbCancel := context.WithCancel(context.Background())
		defer dbCancel()

		snapMode := config.SnapMode(c.String("mode"))

		args := c.Args().Slice()
		if len(args) == 0 {
			return cli.Exit("Please supply parameters", 1)
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

		cfg := config.Get(c)

		db, err := dao.New(dbCtx, cfg.DbURI)
		if err != nil {
			logrus.Panicln("failed to initialize database: ", err)
		}
		logrus.Info("successfully connected to database")

		creekStream, err := stream.NewStream(ctx, cfg, db)
		if err != nil {
			return err
		}

		for target, source := range mappings {
			_, err = creekStream.NewSnapshot(snapMode, source, target)
			if err != nil {
				logrus.Errorf("failed to take snapshot: %v", err)
			}
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
}

func CreateSchemas(ctx context.Context) cli.ActionFunc {
	return func(c *cli.Context) error {
		args := c.Args().Slice()
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

		cfg := config.Get(c)

		db, err := dao.New(ctx, cfg.DbURI)
		if err != nil {
			logrus.Panicln("failed to initialize database: ", err)
		}
		logrus.Info("successfully connected to database")

		creekStream, err := stream.NewStream(ctx, cfg, db)
		if err != nil {
			return err
		}

		for source, target := range mappings {
			err = creekStream.CreateSchema(ctx, source, target)
			if err != nil {
				logrus.Errorf("failed to create schema %s: %v", target.Name(), err)
				continue
			}
			logrus.Infof("Created table %s", target)
		}

		return nil
	}
}

func Serve(ctx context.Context) func(*cli.Context) error {
	return func(c *cli.Context) error {
		cfg := config.Get(c)
		dbCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		db, err := dao.New(dbCtx, cfg.DbURI)
		if err != nil {
			logrus.Panicln("failed to initialize database: ", err)
		}
		logrus.Info("successfully connected to database")

		go metrics.Start(ctx, c.Int("prometheus-port"))

		creekStream, err := stream.NewStream(ctx, cfg, db)
		if err != nil {
			return err
		}

		creekStream.StartListenAPI()

		streams, err := db.GetActiveStreams()
		if err != nil {
			return fmt.Errorf("failed to get active streams: %w", err)
		}

		for target, source := range streams {
			err := creekStream.AddWALTable(ctx, source, target)
			if err != nil {
				logrus.Errorf("failed to start streaming wal for table %s", target)
			}
		}

		allDone := chanz.EveryDone(
			ctx.Done(),
			creekStream.Done(),
		)

		for {
			select {
			case <-allDone:
				cancel()
				os.Exit(0)
			}
		}
	}
}
