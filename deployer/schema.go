package main

import (
	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/go-sql"
	"github.com/flynn/flynn/pkg/migrate"
)

func migrateDB(db *sql.DB) error {
	m := migrate.NewMigrations()
	m.Add(1,
		`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`,

		`CREATE TABLE jobs (
    job_id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    steps text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now())`,
	)
	return m.Migrate(db)
}
