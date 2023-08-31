package dao

import (
	"fmt"
	"strings"

	"github.com/modfin/creek"
	"github.com/modfin/henry/mapz"
	"github.com/modfin/henry/slicez"
)

func (db *DB) PersistMessage(message creek.WAL, target string) error {
	// TODO: add truncate

	switch message.Op {
	case creek.OpInsert:
		return db.insert(target, message)
	case creek.OpUpdate:
		return db.update(target, message)
	case creek.OpUpdatePk:
		return db.updatePk(target, message)
	case creek.OpDelete:
		return db.delete(target, message)
	default:
		return fmt.Errorf("unknown operation: %s", message.Op)
	}
}

func (db *DB) insert(target string, message creek.WAL) error {
	var keys []string
	var vals []any
	placeHolders := make([]string, len(keys))
	i := 1
	for k, v := range *message.After {
		keys = append(keys, k)
		vals = append(vals, toPgType(v))
		placeHolders = append(placeHolders, fmt.Sprintf("$%d", i))
		i++
	}

	keysStr := strings.Join(keys, ", ")
	placeHoldersStr := strings.Join(placeHolders, ", ")

	q := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING", target, keysStr, placeHoldersStr)

	db.persistBus <- []query{{query: q, args: vals}}

	return nil
}

func (db *DB) update(target string, message creek.WAL) error {

	beforeKeys := slicez.Sort(mapz.Keys(*message.Before))
	afterKeys := slicez.Sort(mapz.Keys(*message.After))

	var setExcluded []string
	var insertPlaceholders []string
	var afterVals []any
	i := 1
	for _, k := range afterKeys {
		insertPlaceholders = append(insertPlaceholders, fmt.Sprintf("$%d", i))
		setExcluded = append(setExcluded, fmt.Sprintf("%s = excluded.%s", k, k))
		afterVals = append(afterVals, toPgType((*message.After)[k]))
		i += 1
	}

	var conflictKeys []string
	for _, k := range beforeKeys {
		conflictKeys = append(conflictKeys, k)
	}

	insertKeysStr := strings.Join(afterKeys, ", ")
	insertPlaceholdersStr := strings.Join(insertPlaceholders, ", ")
	conflictKeysStr := strings.Join(conflictKeys, ", ")
	excludedStr := strings.Join(setExcluded, ", ")

	insertQuery := fmt.Sprintf(`
INSERT INTO %s (%s) 
VALUES (%s)
ON CONFLICT (%s)
DO UPDATE SET %s`, target, insertKeysStr, insertPlaceholdersStr, conflictKeysStr, excludedStr)

	db.persistBus <- []query{{query: insertQuery, args: afterVals}}

	return nil
}

func (db *DB) updatePk(target string, message creek.WAL) error {

	beforeKeys := slicez.Sort(mapz.Keys(*message.Before))
	afterKeys := slicez.Sort(mapz.Keys(*message.After))

	var insertPlaceholders []string
	var afterVals []any
	i := 1
	for _, k := range afterKeys {
		insertPlaceholders = append(insertPlaceholders, fmt.Sprintf("$%d", i))
		afterVals = append(afterVals, toPgType((*message.After)[k]))
		i += 1
	}

	var whereKeys []string
	var beforeVals []any
	i = 1
	for _, k := range beforeKeys {
		whereKeys = append(whereKeys, fmt.Sprintf("%s = $%d", k, i))
		beforeVals = append(beforeVals, toPgType((*message.Before)[k]))
		i += 1
	}

	whereStr := strings.Join(whereKeys, " AND ")
	insertKeysStr := strings.Join(afterKeys, ", ")
	insertPlaceholdersStr := strings.Join(insertPlaceholders, ", ")

	updateQuery := fmt.Sprintf(`DELETE FROM %s WHERE %s`, target, whereStr)
	insertQuery := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING`, target, insertKeysStr, insertPlaceholdersStr)

	db.persistBus <- []query{
		{query: updateQuery, args: beforeVals},
		{query: insertQuery, args: afterVals},
	}

	return nil
}

func (db *DB) delete(target string, message creek.WAL) error {
	i := 1
	var whereKeys []string
	var whereVals []any
	for k, v := range *message.Before {
		whereKeys = append(whereKeys, fmt.Sprintf("%s = $%d", k, i))
		whereVals = append(whereVals, toPgType(v))
		i++
	}

	whereStr := strings.Join(whereKeys, " AND ")

	q := fmt.Sprintf("DELETE FROM %s WHERE %s", target, whereStr)

	db.persistBus <- []query{{query: q, args: whereVals}}

	return nil
}
