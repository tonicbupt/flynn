package main

import (
	"os"
	"time"

	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/que-go"
	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/jackc/pgx"
	"github.com/flynn/flynn/Godeps/_workspace/src/gopkg.in/inconshreveable/log15.v2"
	"github.com/flynn/flynn/controller/client"
	"github.com/flynn/flynn/controller/worker/deployment"
	"github.com/flynn/flynn/pkg/postgres"
	"github.com/flynn/flynn/pkg/shutdown"
)

const workerCount = 10

var logger = log15.New("app", "worker")

func main() {
	log := logger.New("fn", "main")

	log.Info("creating controller client")
	client, err := controller.NewClient("", os.Getenv("AUTH_KEY"))
	if err != nil {
		log.Error("error creating controller client", "err", err)
		shutdown.Fatal()
	}

	log.Info("connecting to postgres")
	db := postgres.Wait("", "")

	log.Info("creating postgres connection pool")
	pgxpool, err := pgx.NewConnPool(pgx.ConnPoolConfig{
		ConnConfig: pgx.ConnConfig{
			Host:     os.Getenv("PGHOST"),
			User:     os.Getenv("PGUSER"),
			Password: os.Getenv("PGPASSWORD"),
			Database: os.Getenv("PGDATABASE"),
		},
		AfterConnect:   que.PrepareStatements,
		MaxConnections: workerCount,
	})
	if err != nil {
		log.Error("error creating postgres connection pool", "err", err)
		shutdown.Fatal()
	}
	shutdown.BeforeExit(func() { pgxpool.Close() })

	workers := que.NewWorkerPool(
		que.NewClient(pgxpool),
		que.WorkMap{
			"deployment": deployment.JobHandler(db, client, logger),
		},
		workerCount,
	)
	workers.Interval = 5 * time.Second

	log.Info("starting workers", "count", workerCount, "interval", workers.Interval)
	workers.Start()
	shutdown.BeforeExit(func() { workers.Shutdown() })

	select {} // block and keep running
}
