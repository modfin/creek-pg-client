package config

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/modfin/creek-pg-client/internal/metrics"
	"github.com/modfin/creek-pg-client/internal/utils"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

type SnapMode string

const Upsert SnapMode = "upsert"
const Clean SnapMode = "clean"

func ValidSnapOptions() []SnapMode {
	return []SnapMode{Upsert, Clean}
}

func DefaultSnapOption() SnapMode {
	return Upsert
}

type Source struct {
	db        string
	namespace string
	table     string
}

func (s Source) String() string {
	return fmt.Sprintf("%s.%s.%s", s.db, s.namespace, s.table)
}

func (s Source) DB() string {
	return s.db
}

func (s Source) Name() string {
	return fmt.Sprintf("%s.%s", s.namespace, s.table)
}

type Target struct {
	namespace string
	table     string
}

func (t Target) String() string {
	return fmt.Sprintf("%s.%s", t.namespace, t.table)
}

func (t Target) Name() string {
	return fmt.Sprintf("%s.%s", t.namespace, t.table)
}

func (t Target) Table() string {
	return t.table
}

func (t Target) NS() string {
	return t.namespace
}

type Config struct {
	LogLevel string

	NatsURI       string
	NatsNamespace string
	DbURI         string
	DbNamespace   string
	SnapMode      string
}

var cfg Config
var set sync.Once

func Get(c *cli.Context) Config {
	set.Do(func() {
		cfg.NatsURI = c.String("nats-uri")
		cfg.NatsNamespace = c.String("nats-namespace")
		cfg.DbURI = c.String("db-uri")
		cfg.DbNamespace = c.String("db-namespace")

		logRate := float32(c.Float64("log-rate"))
		logBurst := c.Int("log-burst")

		ll, err := logrus.ParseLevel(c.String("log-level"))
		if err != nil {
			ll = logrus.InfoLevel
		}
		limitedWriter := utils.NewRateLimitedWriter(logRate, logBurst, ll)
		promHook := metrics.MustNewPrometheusHook()
		logrus.SetLevel(ll)
		logrus.AddHook(promHook)
		logrus.AddHook(limitedWriter)
		logrus.SetOutput(io.Discard)
	})

	return cfg
}

func ParseSource(tables string) (s Source, err error) {
	source := strings.SplitN(tables, ".", 3)

	if len(source) != 3 {
		err = errors.New("source must contain 3 fields separated by '.'")
		return
	}

	return Source{
		db:        source[0],
		namespace: source[1],
		table:     source[2],
	}, nil
}

func ParseTarget(tables string) (t Target, err error) {
	target := strings.SplitN(tables, ".", 2)

	if len(target) != 2 {
		err = errors.New("target must contain 3 fields separated by '.'")
		return
	}

	return Target{
		namespace: target[0],
		table:     target[1],
	}, nil
}

func ParseTable(tables string) (s Source, t Target, err error) {
	split := strings.SplitN(tables, ":", 2)
	if len(split) != 2 {
		err = errors.New("must contain a source and target separated by ':'")
		return
	}
	source := strings.SplitN(split[0], ".", 3)
	target := strings.SplitN(split[1], ".", 2)

	if len(source) != 3 {
		err = errors.New("source must contain 3 fields separated by '.'")
		return
	}

	if len(target) != 2 {
		err = errors.New("target must contain 2 fields separated by '.'")
		return
	}

	return Source{
			db:        source[0],
			namespace: source[1],
			table:     source[2],
		}, Target{
			namespace: target[0],
			table:     target[1],
		}, nil
}
