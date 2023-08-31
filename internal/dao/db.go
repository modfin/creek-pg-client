package dao

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/modfin/creek-pg-client/internal/config"
	pgtypeavro "github.com/modfin/creek/pgtype-avro"
	"github.com/modfin/henry/slicez"
	"github.com/sirupsen/logrus"
)

type query struct {
	query string
	args  []any
}

type DB struct {
	ctx context.Context

	pool *pgxpool.Pool

	persistBus chan []query
}

func New(ctx context.Context, uri string) (*DB, error) {
	pool, err := pgxpool.New(ctx, uri)
	if err != nil {
		return nil, err
	}

	db := &DB{
		ctx:        ctx,
		pool:       pool,
		persistBus: make(chan []query),
	}

	go db.startBus()

	return db, db.ensureSchemas()
}

func (db *DB) SetContext(ctx context.Context) {
	db.ctx = ctx
}

func (db *DB) ensureSchemas() error {
	// Create schema with consumer metadata
	_, err := db.pool.Exec(db.ctx, `
CREATE SCHEMA IF NOT EXISTS _creek_consumer;
CREATE TABLE IF NOT EXISTS _creek_consumer.subscriptions (
	topic TEXT,
	target TEXT,
	lsn PG_LSN,
	timestamp TIMESTAMPTZ,
	active BOOL,
	CONSTRAINT pk_creek_subscriptions PRIMARY KEY (target)
);
CREATE TABLE IF NOT EXISTS _creek_consumer._notification_log (
	timestamp TIMESTAMPTZ DEFAULT NOW(),
	command TEXT,
	status TEXT,
	CONSTRAINT pk_creek_notification_log PRIMARY KEY (timestamp)
);
create or replace function _creek_consumer.add_table("table" text)
returns void language plpgsql as $$
    begin
        execute pg_notify('creek_consumer', 'ADD ' || "table");
    end;
    $$;

create or replace function _creek_consumer.remove_table("table" text)
returns void language plpgsql as $$
    begin
        execute pg_notify('creek_consumer', 'REMOVE ' || "table");
    end;
    $$;

create or replace function _creek_consumer.create_schema("table" text)
returns void language plpgsql as $$
    begin
        execute pg_notify('creek_consumer', 'CREATE ' || "table");
    end;
    $$;

create or replace function _creek_consumer.snapshot("table" text, "mode" text)
returns void language plpgsql as $$
    begin
        execute pg_notify('creek_consumer', 'SNAP ' || "mode" || ' ' || "table");
    end;
    $$;

create or replace function _creek_consumer.apply_snapshot("topic" text, "table" text, "mode" text)
returns void language plpgsql as $$
    begin
        execute pg_notify('creek_consumer', 'APPLY_SNAP ' || "mode" || ' ' || "topic" || ' ' || "table");
    end;
    $$;
`)

	return err
}

// Way of batching queries and running them inside one transaction. Has
// debouncing logic. Improves wal read performance by a factor of ~4x on my computer.
func (db *DB) startBus() {

	var batch pgx.Batch

	timer := time.After(time.Millisecond * 100)

	resetTimer := func() {
		timer = time.After(time.Millisecond * 100)
	}

	persist := func() error {
		defer resetTimer()
		return db.pool.SendBatch(db.ctx, &batch).Close()
	}

	for {
		select {
		case qs := <-db.persistBus:
			for _, q := range qs {
				logrus.Tracef("[db queue] %s %v", q.query, q.args)
				batch.Queue(q.query, q.args...)
			}

			if batch.Len() >= 50 { // Magic numer
				err := persist()
				if err != nil {
					logrus.Errorf("failed to persist data: %v", err)
				}
				batch = pgx.Batch{}
			}
		case <-timer: // Magic debounce time
			resetTimer()
			if batch.Len() > 0 {
				err := persist()
				if err != nil {
					logrus.Errorf("failed to persist data: %v", err)
				}
				batch = pgx.Batch{}
			}
		}

	}
}

type Field struct {
	Name     string
	DataType string
	Pk       bool
}

func (db *DB) CreateTable(target config.Target, fields []Field) error {
	var columns strings.Builder
	var pkColumns []string
	for _, field := range fields {
		columns.WriteString(fmt.Sprintf("%s %s, ", field.Name, field.DataType))
		if field.Pk {
			pkColumns = append(pkColumns, field.Name)
		}
	}

	q := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	%s
	CONSTRAINT pk_%s PRIMARY KEY (%s)
)`, target.Name(), columns.String(), target.Table(), strings.Join(pkColumns, ", "))

	logrus.Debugf("%s", q)

	_, err := db.pool.Exec(db.ctx, q)

	return err
}

func (db *DB) UpdateLocation(source config.Source, target config.Target, lsn string, ts time.Time) error {
	return db.UpdateLocationWithContext(db.ctx, source, target, lsn, ts)
}

func (db *DB) UpdateLocationWithContext(ctx context.Context, source config.Source, target config.Target, lsn string, ts time.Time) error {
	if len(lsn) == 0 {
		lsn = "0/0"
	}

	_, err := db.pool.Exec(ctx, `
INSERT INTO _creek_consumer.subscriptions (topic, target, lsn, timestamp) 
VALUES ($1, $2, $3, $4) ON CONFLICT (target) DO UPDATE SET 
target=excluded.target, lsn=excluded.lsn, timestamp=excluded.timestamp
`, source, target, lsn, ts)

	return err
}

func (db *DB) GetActiveStreams() (map[config.Target]config.Source, error) {

	rows, err := db.pool.Query(db.ctx, `SELECT topic, target FROM _creek_consumer.subscriptions WHERE active=true`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	streams := make(map[config.Target]config.Source)

	for rows.Next() {
		var srcStr string
		var targetStr string

		err = rows.Scan(&srcStr, &targetStr)
		if err != nil {
			return nil, err
		}

		source, err := config.ParseSource(srcStr)
		if err != nil {
			return nil, err
		}

		target, err := config.ParseTarget(targetStr)
		if err != nil {
			return nil, err
		}

		streams[target] = source

	}

	return streams, nil
}

func (db *DB) GetStreamLocation(source config.Source, target config.Target) (lsn string, ts time.Time, err error) {
	lsn = "0/0"

	err = db.pool.QueryRow(db.ctx,
		`SELECT lsn, timestamp FROM _creek_consumer.subscriptions WHERE target=$1`,
		target).Scan(&lsn, &ts)

	if errors.Is(err, pgx.ErrNoRows) {
		_, innerErr := db.pool.Exec(db.ctx, `
INSERT INTO _creek_consumer.subscriptions (topic, target, lsn, timestamp) 
VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING 
`, source, target, "0/0", time.Time{})
		if innerErr != nil {
			return lsn, ts, innerErr
		}

		return lsn, ts, nil
	}

	return
}

func (db *DB) LogNotification(call string, err error) {
	if err == nil {
		err = errors.New("ok")
	}
	_, err = db.pool.Exec(db.ctx, `
INSERT INTO _creek_consumer._notification_log (command, status) 
VALUES ($1, $2)
`, call, err.Error())
	if err != nil {
		logrus.Errorf("failed to log notification: %v", err)
	}
}

func (db *DB) SetActive(target config.Target, active bool) error {

	_, err := db.pool.Exec(db.ctx,
		`UPDATE _creek_consumer.subscriptions SET active = $1 WHERE target = $2`,
		active, target)

	return err
}

func (db *DB) CopyTable(old string, new string) error {
	_, err := db.pool.Exec(db.ctx, fmt.Sprintf(`CREATE TABLE %s (
   LIKE %s 
     INCLUDING defaults
     INCLUDING constraints
     INCLUDING indexes
);`, new, old))

	return err
}

func (db *DB) SwapAndDropTable(namespace string, old string, new string) error {
	tx, err := db.pool.Begin(db.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(db.ctx)

	_, err = tx.Exec(db.ctx, fmt.Sprintf(`DROP TABLE %s.%s`, namespace, old))
	if err != nil {
		return err
	}
	_, err = tx.Exec(db.ctx, fmt.Sprintf(`ALTER TABLE %s RENAME TO %s`, new, old))
	if err != nil {
		return err
	}

	err = tx.Commit(db.ctx)
	if err != nil {
		return err
	}

	return nil
}

func toPgType(v any) any {
	switch val := v.(type) {
	case time.Duration:
		return pgtype.Time{Microseconds: val.Microseconds(), Valid: true}
	case map[string]interface{}:
		// Array type, we will only ever have arrays with the consistent types ??
		if array, ok := val["array"]; ok {
			return toPgType(array)
		}

		if array, ok := val["long.timestamp-micros"]; ok {
			return toPgType(array)
		}

		if array, ok := val["long.time-micros"]; ok {
			return toPgType(array)
		}

		if array, ok := val["int.date"]; ok {
			return toPgType(array)
		}

		return v
	case []interface{}:
		return slicez.Map(val, toPgType)
	case *big.Rat:
		flt := new(big.Float).SetRat(val)
		var num pgtype.Numeric
		err := num.Scan(flt.Text('f', -1))
		if err != nil {
			logrus.Errorf("failed to scan numeric value, big problem: %v", err)
			return val
		}

		return num
	case string:
		if val == pgtypeavro.NegativeInfinity {
			return "-infinity"
		}
		return val
	default:
		return v
	}
}
