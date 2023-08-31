package stream

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/modfin/creek"
	"github.com/modfin/creek-pg-client/internal/config"
	"github.com/modfin/creek-pg-client/internal/dao"
	"github.com/modfin/creek-pg-client/internal/metrics"
	"github.com/sirupsen/logrus"
)

type op string

const (
	pause  op = "pause"
	jump   op = "jump"
	resume op = "resume"
)

type command struct {
	op  op
	lsn string
	ts  time.Time
}

type walStream struct {
	ctx    context.Context
	cancel context.CancelFunc

	source config.Source
	target config.Target
	lsn    string
	ts     time.Time

	skipTo pglogrepl.LSN

	ctrlChan chan command

	wal  *creek.WALStream
	conn *creek.Conn
	db   *dao.DB

	doneChan chan struct{}
}

func (s *Stream) streamTable(ctx context.Context, source config.Source, target config.Target) (ws *walStream, err error) {
	ctx, cancel := context.WithCancel(ctx)

	ws = &walStream{
		ctx:      ctx,
		cancel:   cancel,
		source:   source,
		target:   target,
		lsn:      "0/0",
		ctrlChan: make(chan command, 1),
		wal:      nil,
		db:       s.db,
		conn:     s.conn,
		ts:       time.Time{},
		skipTo:   pglogrepl.LSN(0),
		doneChan: make(chan struct{}),
	}

	err = ws.init()
	if err != nil {
		return nil, err
	}

	go ws.stream()

	return ws, nil
}

func (w *walStream) SetSkipTo(lsn pglogrepl.LSN) {
	w.skipTo = lsn
}

func (w *walStream) Resume() {
	w.ctrlChan <- command{op: resume}
}

func (w *walStream) Pause() {
	w.ctrlChan <- command{op: pause}
}

func (w *walStream) init() error {

	var streamErr error

	lsn, ts, err := w.db.GetStreamLocation(w.source, w.target)
	if err != nil {
		return fmt.Errorf("failed to find stream location for table %s: %v", w.source, err)

	}

	// Start streaming a little bit before to be on the safe side
	beforeTime := time.Second * 10

	w.lsn = lsn
	w.ts = ts

	w.wal, streamErr = w.conn.SteamWALFrom(w.ctx, w.source.DB(), w.source.Name(), ts.Add(-beforeTime), lsn)

	if streamErr != nil {
		return fmt.Errorf("failed to stream wal for table %s: %v", w.source, err)
	}

	if streamErr != nil {
		return fmt.Errorf("failed to set stream active for %s: %v", w.source, err)
	}

	return nil
}

func (w *walStream) stream() {

	persistLoc := func() {
		err := w.db.UpdateLocation(w.source, w.target, w.lsn, w.ts)
		if err != nil {
			logrus.Errorf("failed to persist stream location: %v", err)
		}
	}

	waitContinue := func() {
		for {
			cmd := <-w.ctrlChan
			if cmd.op == resume {
				break
			}
		}
	}

	timeoutDuration := time.Second * 10

	timeout := time.Now().Add(timeoutDuration)

	for {

		select {
		case <-w.ctx.Done():
			w.wal.Close()
			persistLoc()
			logrus.Info("wal done")
			w.doneChan <- struct{}{}

			return
		case cmd := <-w.ctrlChan:
			logrus.Infof("ctrlchan")
			if cmd.op == pause {
				persistLoc()
				waitContinue()
			}

			if cmd.op == jump {
				w.wal.Close()
				err := w.db.UpdateLocation(w.source, w.target, cmd.lsn, cmd.ts)
				if err != nil {
					logrus.Errorf("failed to jump stream location: %v", err)
				}
				err = w.init()
			}
		default:
		}

		withDeadline, cancel := context.WithDeadline(w.ctx, timeout)

		message, err := w.wal.Next(withDeadline)
		cancel()
		if errors.Is(err, context.Canceled) {
			persistLoc()
			timeout = time.Now().Add(timeoutDuration)
			continue
		}

		if err != nil {
			logrus.Errorf("failed to get next wal message for table %s: %v. exiting", w.source, err)
			return
		}

		w.lsn = message.Source.LSN
		w.ts = message.Source.TxAt

		lsn, _ := pglogrepl.ParseLSN(w.lsn)
		// TODO: what to do if parsing fails?
		if lsn < w.skipTo {
			continue
		}

		err = w.db.PersistMessage(message, w.target.Name())

		if err != nil {
			logrus.Errorf("failed to persist wal message for table %s: %v", w.source, err)
		}
		metrics.IncInserts(creek.WalStream, w.source.Name(), w.target.Name())
	}
}

func (w *walStream) Done() <-chan struct{} {
	return w.doneChan
}

func (w *walStream) Close() {
	w.cancel()
}
