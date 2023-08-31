package dao

import (
	"context"
	"errors"
	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/modfin/creek-pg-client/internal/utils"
	"time"

	"github.com/sirupsen/logrus"
)

func (db *DB) StartAPI() <-chan string {
	msgs := make(chan string)

	go db.startApi(msgs)

	return msgs
}

func (db *DB) startApi(notifications chan string) {

	connect := func() (*pgxpool.Conn, error) {
		conn, err := db.pool.Acquire(db.ctx)
		if err != nil {
			return nil, err
		}
		_, err = conn.Exec(db.ctx, "listen creek")
		if err != nil {
			conn.Release()
		}
		return conn, err
	}

	notify := func(err error, timeout time.Duration) {
		logrus.Errorf("[listen api] failed to connect to database, retrying in %ds", int(timeout.Seconds()))
	}

	tryConnect := func() (conn *pgxpool.Conn, err error) {
		conn, err = backoff.RetryNotifyWithData(connect, utils.DefaultBackoff(), notify)
		return
	}

	conn, err := tryConnect()
	if err != nil {
		logrus.Errorf("[listen api] failed to connect: %v", err)
		return
	}
	defer conn.Release()

	_, err = conn.Exec(db.ctx, "listen creek_consumer")
	if err != nil {
		logrus.Errorf("failed to listen to creek_consumer: %v", err)
	}
	logrus.Debug("listening for notifications on _creek_consumer")

	for {
		notification, err := conn.Conn().WaitForNotification(db.ctx)
		if errors.Is(err, context.Canceled) {
			return
		}
		if err != nil {
			logrus.Errorf("failed while waiting to get notification for creek_consumer: %v", err)
			if err.Error() == "conn closed" {
				conn, err = tryConnect() // Blocks until new connection
				if err != nil {
					logrus.Errorf("[listen api] failed to reconnect: %v", err)
					return
				}
			}
			continue
		}

		notifications <- notification.Payload
	}
}
