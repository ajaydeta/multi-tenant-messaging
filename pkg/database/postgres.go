package database

import (
	"log"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

func NewPostgresDB(url string) (*sqlx.DB, error) {
	db, err := sqlx.Connect("postgres", url)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	log.Println("Successfully connected to the database")
	return db, nil
}
