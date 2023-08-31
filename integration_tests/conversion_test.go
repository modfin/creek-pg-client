package integration_tests

import (
	"context"
	"testing"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/modfin/creek"
	"github.com/modfin/creek-pg-client/internal/dao"
	"github.com/stretchr/testify/assert"
)

func TestTypes(t *testing.T) {
	url := GetDBURL()

	db, err := dao.New(context.Background(), url)
	assert.NoError(t, err)

	schema := Schema("types")
	data := Data("types")

	var message creek.WAL
	err = avro.Unmarshal(schema, data, &message)
	assert.NoError(t, err)

	err = db.PersistMessage(message, "public.types")
	assert.NoError(t, err)

	pool := GetDBConn()
	time.Sleep(time.Millisecond * 100)

	var count int
	err = pool.QueryRow(context.Background(), `SELECT count(*) FROM public.types`).Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

}
