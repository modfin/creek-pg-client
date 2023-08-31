package integration_tests

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/modfin/creek-pg-client/integration_tests/dbc"
	"github.com/testcontainers/testcontainers-go"

	"github.com/sirupsen/logrus"
)

var (
	postgres      *dbc.Container
	natsContainer testcontainers.Container
	testsStarted  time.Time
)

const DBname = "test_db"

func startTestContainers(ctx context.Context, dockerNetworkName string) error {
	var err error

	postgres, err = dbc.New("./containers/", "Dockerfile.postgres")
	if err != nil {
		logrus.Errorf("Failed to create DB container spec: %v", err)
		return err
	}

	var startError bool
	var wg sync.WaitGroup

	wg.Add(1)
	go startPostgres(ctx, dockerNetworkName, &wg, &startError)

	wg.Wait()
	if startError {
		log.Fatal("There were errors while starting containers, check log output for more information. Test aborted!")
	}

	log.Print("**********************************************************************")
	log.Printf("*                                                                    *")
	log.Printf("*   DB URL: %-50s    *", postgres.DBURL())
	log.Printf("*                                                                    *")
	log.Print("**********************************************************************")

	testsStarted = time.Now()
	return nil
}

func GetDBURL() string {
	return postgres.DBURL()
}

func GetDBConn() *pgxpool.Pool {
	return postgres.DBConn()
}

func GetPgContainer() *dbc.Container {
	return postgres
}

func ResetContainer() error {
	return postgres.RestartInCurrentCtx()
}

func LoadSql(files ...string) {
	for _, f := range files {
		err := postgres.LoadSQL(postgres.DBConn(), "testdata", f, 1, false)
		if err != nil {
			panic(err.Error())
		}
	}
}

func startPostgres(ctx context.Context, dockerNetworkName string, wg *sync.WaitGroup, startError *bool) {
	defer wg.Done()
	err := postgres.Start(ctx, dockerNetworkName, DBname)
	if err != nil {
		logrus.Errorf("Failed to start postgres container: %v", err)
		*startError = true
		return
	}
}

func shutdownTestContainers(ctx context.Context) {
	// Insert small delay here so that we have time to flush container logs before we shut down the containers
	time.Sleep(time.Second * 2)

	// time.Sleep(time.Hour) // Enable to keep containers alive while debugging

	log.Print("Shutting down containers..")
	var wg sync.WaitGroup
	terminate := func(container interface{ Terminate(ctx context.Context) }) {
		defer wg.Done()
		container.Terminate(ctx)
	}
	wg.Add(1)
	go terminate(postgres)
	wg.Wait()
}
