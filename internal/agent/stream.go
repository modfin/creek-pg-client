package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/hamba/avro/v2"
	"github.com/jackc/pglogrepl"
	"github.com/modfin/creek"
	"github.com/modfin/creek-pg-client/internal/config"
	"github.com/modfin/creek-pg-client/internal/dao"
	"github.com/modfin/henry/chanz"
	"github.com/modfin/henry/mapz"
	"github.com/modfin/henry/slicez"
	"github.com/schollz/progressbar/v3"
	"github.com/sirupsen/logrus"
)

// TODO: rename and create a map that holds one stream per source db and can append when the API adds a new source db
type Agent struct {
	ctx context.Context
	db  *dao.DB

	client *creek.Client

	bar *progressbar.ProgressBar

	lock sync.RWMutex

	// one nats connection per target

	clients map[string]*creek.Client

	walStreams map[SubscriptionKey]*walStream

	snapWg sync.WaitGroup

	isClosed  chan struct{}
	closeOnce sync.Once
}

type SubscriptionKey struct {
	Source config.Source
	Target config.Target
}

type logrusLogger struct {
	logger *logrus.Logger
}

func newLogger(level string) *logrusLogger {
	ll, err := logrus.ParseLevel(level)
	if err != nil {
		ll = logrus.InfoLevel
	}
	l := logrus.New()
	l.SetLevel(ll)
	return &logrusLogger{
		logger: l,
	}
}

func (l *logrusLogger) Info(format string, args ...interface{}) {
	l.logger.Infof(format, args...)
}

func (l *logrusLogger) Debug(format string, args ...interface{}) {
	l.logger.Debugf(format, args...)
}

func (l *logrusLogger) Error(format string, args ...interface{}) {
	l.logger.Errorf(format, args...)
}

func NewStream(ctx context.Context, cfg config.Config, targetDb string, db *dao.DB) *Agent {
	log := newLogger(cfg.LogLevel)

	client := creek.NewClient(cfg.NatsURI, cfg.NatsNamespace, targetDb)
	client.WithLogger(log)

	logrus.Info("successfully connected to creek")

	s := Agent{
		ctx:        ctx,
		db:         db,
		lock:       sync.RWMutex{},
		client:     client,
		walStreams: make(map[config.Target]*walStream),
		isClosed:   make(chan struct{}),
	}

	go func() {
		select {
		case <-ctx.Done():
			s.tryClose()
		}
	}()

	return &s
}

func (s *Agent) StartListenAPI() {
	apiCalls := s.db.StartAPI()
	go s.apiHandler(apiCalls)
}

func (s *Agent) NewSnapshot(mode config.SnapMode, source config.Source, target config.Target) (heder creek.SnapshotHeader, err error) {

	data, close, err := s.client.Snapshot(s.ctx, source.DB(), source.Name())
	if err != nil {
		return
	}
	defer close()

	logrus.Infof("taking new snapshot of table %s at %s", source, data.Header().At)

	err = s.saveSnapshot(mode, data, source, target)

	return data.Header(), err
}

func (s *Agent) ApplySnapshot(mode config.SnapMode, source config.Source, target config.Target, snapTopic string) (header creek.SnapshotHeader, err error) {

	logrus.Infof("applying snapshot %s of table %s", snapTopic, source)
	data, close, err := s.client.GetSnapshot(s.ctx, snapTopic)
	if err != nil {
		return
	}
	defer close()

	err = s.saveSnapshot(mode, data, source, target)

	return data.Header(), err
}

func (s *Agent) AddWALTable(ctx context.Context, source config.Source, target config.Target) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	// TODO set walstreams at the end
	if _, ok := s.walStreams[target]; ok {
		return nil
	}

	ws, err := s.streamTable(ctx, source, target)
	if err != nil {
		return fmt.Errorf("failed to start streaming table %s: %w", source, err)
	}

	err = s.db.SetActive(target, true)
	if err != nil {
		return fmt.Errorf("failed to set wal stream for %s to active: %w", target, err)
	}

	s.walStreams[target] = ws
	return nil
}

func (s *Agent) PauseWAL(target config.Target) {
	s.lock.Lock()
	defer s.lock.Unlock()
	ws, ok := s.walStreams[target]
	if !ok {
		return
	}
	ws.Pause()
}

func (s *Agent) ResumeWAL(target config.Target, skipToLSN pglogrepl.LSN) {
	s.lock.Lock()
	defer s.lock.Unlock()
	ws, ok := s.walStreams[target]
	if !ok {
		return
	}
	ws.SetSkipTo(skipToLSN)
	ws.Resume()
}

func (s *Agent) RemoveWALTable(target config.Target) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	ws, ok := s.walStreams[target]
	if !ok {
		return errors.New("target table is not being streamed")
	}
	ws.Close()
	delete(s.walStreams, target)

	return nil
}

// SnapsDone returns a channel that sends a message when all snapshots are finished
func (s *Agent) SnapsDone() <-chan struct{} {
	snapChan := make(chan struct{})
	go func() {
		s.snapWg.Wait()
		snapChan <- struct{}{}
	}()

	return snapChan
}

func (s *Agent) tryClose() {
	s.closeOnce.Do(func() {
		s.snapWg.Wait()
		logrus.Info("snaps done")
		// TODO: close all streams
		for _, ws := range s.walStreams {
			ws.Close()
		}
		logrus.Info("stream done")
		s.isClosed <- struct{}{}
	})
}

func (s *Agent) CreateSchema(ctx context.Context, source config.Source, target config.Target) error {
	schema, err := s.client.GetLastSchema(ctx, source.DB(), source.Name())
	if err != nil {
		return err
	}

	avroSchema, err := avro.Parse(schema.Schema)
	if err != nil {
		return fmt.Errorf("failed to parse schema: %w", err)
	}

	fields, err := avroSchemaToFields(avroSchema)
	if err != nil {
		return fmt.Errorf("failed to extract db fields from schema: %w", err)
	}

	err = s.db.CreateTable(target, fields)

	return err
}

func (s *Agent) Done() <-chan struct{} {
	walDones := slicez.Map(mapz.Values(s.walStreams), func(a *walStream) <-chan struct{} {
		return a.Done()
	})

	return chanz.EveryDone(append(walDones, s.isClosed)...)
}

func avroSchemaToFields(schema avro.Schema) ([]dao.Field, error) {
	var fields []dao.Field

	r := schema.(*avro.RecordSchema)
	rf := r.Fields()
	after, ok := slicez.Find(rf, func(f *avro.Field) bool {
		return f.Name() == "after"
	})
	if !ok {
		return fields, errors.New("no after field")
	}
	union, ok := after.Type().(*avro.UnionSchema)
	if !ok {
		return fields, errors.New("no after union")
	}
	r, ok = union.Types()[1].(*avro.RecordSchema)
	if !ok {
		return fields, errors.New("no record in after union")
	}
	for _, rf := range r.Fields() {
		fields = append(fields, dao.Field{
			Name:     rf.Name(),
			DataType: rf.Prop("pgType").(string),
			Pk:       rf.Prop("pgKey").(bool),
		})
	}

	return fields, nil
}
