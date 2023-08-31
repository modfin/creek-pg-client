package dao

import (
	"fmt"
	"strings"

	"github.com/modfin/creek"
	"github.com/modfin/henry/mapz"
	"github.com/modfin/henry/slicez"
	"github.com/sirupsen/logrus"
)

func (db *DB) PersistSnapshotData(target string, data []creek.SnapRow, pk []string) error {
	return db.snapshot(target, data, pk)
}

func (db *DB) snapshot(target string, rows []creek.SnapRow, pk []string) error {

	if len(rows) == 0 {
		return nil
	}

	var insertKeys []string
	var vals []any
	var updateKeys []string

	keys := slicez.Sort(mapz.Keys(rows[0]))

	for _, k := range keys {
		insertKeys = append(insertKeys, k)
		updateKeys = append(updateKeys, fmt.Sprintf("%s = excluded.%s", k, k))
	}

	var acc []string
	i := 1
	for _, row := range rows {
		var str []string
		for _, k := range keys {
			val := toPgType(row[k])
			vals = append(vals, val)
			str = append(str, fmt.Sprintf("$%d", i))
			i += 1
		}
		acc = append(acc, fmt.Sprintf("(%s)", strings.Join(str, ", ")))
	}

	insertKeysStr := strings.Join(insertKeys, ", ")
	keysStr := strings.Join(pk, ", ")
	updateStr := strings.Join(updateKeys, ",\n")

	query := fmt.Sprintf(`
INSERT INTO %s (%s) 
VALUES 
%s 
ON CONFLICT (%s) 
DO UPDATE SET %s`, target, insertKeysStr, strings.Join(acc, ",\n"), keysStr, updateStr)

	logrus.Trace(query, " ", vals)

	// Already has batching logic, we probably don't need more.
	_, err := db.pool.Exec(db.ctx, query, vals...)
	if err != nil {
		return err
	}

	return nil
}
