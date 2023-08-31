package stream

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/modfin/creek"
	"github.com/modfin/creek-pg-client/internal/config"
	"github.com/modfin/creek-pg-client/internal/metrics"
	"github.com/schollz/progressbar/v3"
	"github.com/sirupsen/logrus"
)

func (s *Stream) getLastSnapReader(ctx context.Context, source config.Source) (*creek.SnapshotReader, error) {
	var snapData *creek.SnapshotReader
	var err error

	snaps, err := s.conn.ListSnapshots(ctx, source.DB(), source.Name())
	if err != nil {
		return nil, err
	}
	if len(snaps) == 0 {
		return nil, errors.New("no snapshots exist")
	}

	snapData, err = s.conn.GetSnapshot(ctx, snaps[len(snaps)-1].Name)

	return snapData, err
}

func (s *Stream) saveSnapshot(mode config.SnapMode, data *creek.SnapshotReader, source config.Source, target config.Target) (err error) {
	s.snapWg.Add(1)
	defer s.snapWg.Done()

	s.bar = progressbar.Default(int64(data.Header().ApproxRows), fmt.Sprintf("snapshot of %s", source))

	var persistTable string
	if mode == config.Upsert {
		persistTable = target.Name()
	}

	if mode == config.Clean {
		persistTable = fmt.Sprintf("%s_tmp_%d", target.Name(), data.Header().At.Unix())
		err := s.db.CopyTable(target.Name(), persistTable)
		if err != nil {
			return fmt.Errorf("failed to create temporary snapshot table: %w", err)
		}
	}

	persist := func(rows []creek.SnapRow) (err error) {
		err = s.db.PersistSnapshotData(persistTable, rows, data.Keys())
		if err != nil {
			return
		}
		s.bar.Add(len(rows))
		metrics.AddInserts(creek.SnapStream, source.Name(), target.Name(), len(rows))
		return
	}

	var rows []creek.SnapRow

LOOP:
	for {
		select {
		case <-s.ctx.Done():
			err := persist(rows)
			if err != nil {
				logrus.Error(err)
			}

			return errors.New("context closed before snapshot was finished")

		case row, ok := <-data.Chan():
			if !ok {
				err = persist(rows)
				if err != nil {
					logrus.Error(err)
				}
				break LOOP
			}
			rows = append(rows, row)
			if len(rows) == 50 {
				err = persist(rows)
				if err != nil {
					logrus.Error(err)
				}
				rows = nil
			}
		case <-time.After(time.Millisecond * 100):
			err = persist(rows)
			if err != nil {
				logrus.Error(err)
			}
			rows = nil
		}
	}

	if mode == config.Clean {
		err := s.db.SwapAndDropTable(target.NS(), target.Table(), persistTable)
		if err != nil {
			return fmt.Errorf("failed to clean and apply snapshot table: %w", err)
		}
	}

	// Finished
	logrus.Infof("finished snapshotting %s into table %s", source, target)
	err = s.db.UpdateLocation(source, target, data.Header().LSN, data.Header().At)
	if err != nil {
		return fmt.Errorf("failed to persist snapshot location: %w", err)
	}

	return nil
}
