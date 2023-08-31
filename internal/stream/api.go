package stream

import (
	"github.com/jackc/pglogrepl"
	"github.com/modfin/creek-pg-client/internal/config"
	"github.com/modfin/henry/slicez"
	"github.com/sirupsen/logrus"
	"strings"
)

func (s *Stream) apiHandler(calls <-chan string) {
	for call := range calls {
		spit := strings.SplitN(call, " ", 2)
		if len(spit) != 2 {
			logrus.Errorf("invalid command: %s", spit)
			continue
		}
		cmd := spit[0]
		value := spit[1]

		// ADD db.namespace.table:namespace.table
		if cmd == "ADD" {
			source, target, err := config.ParseTable(value)
			if err != nil {
				logrus.Errorf("failed to parse table: %v", err)
				continue
			}
			err = s.AddWALTable(s.ctx, source, target)
			s.db.LogNotification(call, err)
			if err != nil {
				logrus.Errorf("failed to add WAL table: %v", err)
				continue
			}
			logrus.Infof("Added and started streaming WAL for table %s", target)
		}

		// REMOVE db.namespace.table:namespace.table
		if cmd == "REMOVE" {
			target, err := config.ParseTarget(value)
			if err != nil {
				logrus.Errorf("failed to parse table: %v", err)
				continue
			}
			err = s.RemoveWALTable(target)
			s.db.LogNotification(call, err)
			if err != nil {
				logrus.Errorf("failed to remove WAL table: %v", err)
				continue
			}
			logrus.Infof("Stopped streaming WAL for table %s", target)
		}

		// CREATE db.namespace.table:namespace.table
		if cmd == "CREATE" {
			source, target, err := config.ParseTable(value)
			if err != nil {
				logrus.Errorf("failed to parse table: %v", err)
				continue
			}
			err = s.CreateSchema(s.ctx, source, target)
			s.db.LogNotification(call, err)
			if err != nil {
				logrus.Errorf("failed to create schema %s: %v", target.Name(), err)
				continue
			}
			logrus.Infof("Created table %s", target)
		}

		// SNAP clean db.namespace.table:namespace.table
		if cmd == "SNAP" {
			spit = strings.SplitN(value, " ", 2)
			if len(spit) != 2 {
				logrus.Errorf("invalid command: %s", spit)
				continue
			}
			mode := spit[0]
			tables := spit[1]

			if !slicez.Contains(config.ValidSnapOptions(), config.SnapMode(mode)) {
				logrus.Errorf("invalid snap mode")
				continue
			}

			source, target, err := config.ParseTable(tables)
			if err != nil {
				logrus.Errorf("failed to parse table: %v", err)
				continue
			}

			go func() {
				s.PauseWAL(target)

				header, err := s.NewSnapshot(config.SnapMode(mode), source, target)
				if err != nil {
					logrus.Errorf("failed to snapshot %s, err: %v", target, err)
				}

				lsn, err := pglogrepl.ParseLSN(header.LSN)
				// TODO: what to do if parsing fails?
				s.ResumeWAL(target, lsn)
			}()
		}

		// APPLY_SNAP clean CREEK.db.snap.namespace.table.YYYYMMDDHHMMSS_ms_id db.namespace.table:namespace.table
		if cmd == "APPLY_SNAP" {
			spit = strings.SplitN(value, " ", 3)
			if len(spit) != 3 {
				logrus.Errorf("invalid command: %s", spit)
				continue
			}
			mode := spit[0]
			topic := spit[1]
			tables := spit[2]

			if !slicez.Contains(config.ValidSnapOptions(), config.SnapMode(mode)) {
				logrus.Errorf("invalid snap mode")
				continue
			}

			source, target, err := config.ParseTable(tables)
			if err != nil {
				logrus.Errorf("failed to parse table: %v", err)
				continue
			}

			go func() {
				s.PauseWAL(target)
				header, err := s.ApplySnapshot(config.SnapMode(mode), source, target, topic)
				if err != nil {
					logrus.Errorf("failed to snapshot %s, err: %v", target, err)
				}

				lsn, err := pglogrepl.ParseLSN(header.LSN)
				// TODO: what to do if parsing fails?
				s.ResumeWAL(target, lsn)
			}()
		}
	}
}
