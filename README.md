## Creek postgres consumer

Consumer for [Creek](https://github.com/modfin/creek) that applies changes from
Creek to a postgres database. It has the following features:

* Listen for WAL events on creek and apply changes in "realtime" to a target
  PostgreSQL database.
* Create database tables based on the schema of a table in creek.
* List available taken snapshots of a table in creek.
* Apply an already existing snapshot to a corresponding table in the target
  database.
* Request a new snapshot from creek, and apply it to a table in the target 
  database.
* An API that allows interacting with and changing the configuration of the
  consumer while it is running.

Used in conjunction with a [Creek producer](https://github.com/modfin/creek),
this system enables keeping Postgres tables in sync across databases, including
across different versions of Postgres.

## How to run

Build using

```
go build cmd/clientd.go
```

## Configuring

There are two main ways of interacting with and configuring the program.
Firstly, it is possible to configure the client using a CLI. To view all
commands and options, you can run `./clientd --help`.

Please note that configuring using CLI the **while the client is running** (ie.
`./clientd serve`) is **not recommended** and may lead to unintended
consequences. It is mainly intended to be used when first setting up a client
and for ergonomic reasons when testing/debugging.

If you want to interact with the client while it is running, please use the
PostgreSQL API. This API has functionality for starting and stopping processing
of WAL events for specific tables, as well as applying snapshots.

### Configuring using CLI

Create a table in the target database.

```
./clientd --nats-uri uri --db-uri uri create-schemas \
    db.namespace.table:target_namespace.target_table
```

Request and apply a new snapshot of the table.

```
./clientd --nats-uri uri --db-uri uri snapshot \
     db.namespace.table:target_namespace.target_table
```

Add the table to start listening for WAL events.

```
./clientd --nats-uri uri --db-uri uri add-tables \
    db.namespace.table:target_namespace.target_table
```

List active streams.

```
./clientd --nats-uri uri --db-uri uri list-tables
```

List existing snapshots for a table.

```
./clientd --nats-uri uri --db-uri uri list-snapshots db.namespace.table
```

Apply an existing snapshot for a table to a target table.

```
./clientd --nats-uri uri --db-uri uri apply-snapshot \
     --name CREEK.db.snap.namespace.table.YYYYMMDDHHMMSS_ms_id \
     db.namespace.table:target_namespace.target_table
```

Stop listening for WAL events for a table.

```
./clientd --nats-uri uri --db-uri uri remove-table target_namespace.target_table
```

### Configuring using the PostgreSQL API

Start the service

```
./clientd --nats-uri uri --db-uri uri serve
```

In a PostgreSQL shell connected to the same database and namespace:

```SQL
-- Create a copy of the db.namespace.table table in the database
SELECT _creek_consumer.create_schema('db.namespace.table:target_namespace.target_table');

-- Request and apply a new snapshot of the table. Valid modes are 'upsert' and 'clean'
SELECT _creek_consumer.snapshot('db.namespace.table:target_namespace.target_table', 'clean');

-- Start listening to WAL events and applying them to table
SELECT _creek_consumer.add_table('db.namespace.table:target_namespace.target_table');

-- View current status
SELECT * FROM _creek_consumer.subscriptions;

-- Stop listening and applying changes for table. Valid modes are 'upsert' and 'clean'
SELECT _creek_consumer.remove_table('target_namespace.target_table', 'upsert');

-- Apply a specific snapshot
SELECT _creek_consumer.apply_snapshot(
    'CREEK.db.snap.namespace.table.YYYYMMDDHHMMSS_ms_id', 
    'db.namespace.table:target_namespace.target_table', 
    'upsert');

-- View status or errors from commands that have been run
SELECT * FROM _creek_consumer._notification_log;

-- View active subscriptions, ie tables that are being listened to
SELECT * FROM _creek_consumer.subscriptions WHERE active = true;
```

#### Snapshot modes

There are currently two different snapshot modes: `'upsert'` and `'clean'`.
Upsert simply performs `INSERT ... ON CONFLICT UPDATE` statements. Clean creates
a new temporary table, applies the snapshot to the new table, and then replaces
the old table with the new table once the snapshot is complete.

#### Notes

If applying a snapshot to a table that is currently subscribing to WAL events,
processing of WAL events for this table will be paused until the snapshot is
complete. Processing of events will continue from the time the snapshot was
taken after it is complete.

It is safe to restart the service as it will persist its last location in the 
database that it is connected to, and continue streaming WAL events from this
location on startup.