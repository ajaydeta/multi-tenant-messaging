package helper

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

type DBExecutor interface {
	NamedExec(query string, arg interface{}) (sql.Result, error)
	NamedQuery(query string, arg interface{}) (*sqlx.Rows, error)
	Get(dest interface{}, query string, args ...interface{}) error
	Select(dest interface{}, query string, args ...interface{}) error
	Exec(query string, args ...interface{}) (sql.Result, error)
	Queryx(query string, args ...interface{}) (*sqlx.Rows, error)
	QueryRowx(query string, args ...interface{}) *sqlx.Row
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error)
	QueryRowxContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error)
}

func GetDbFromContext(ctx context.Context, db DBExecutor) DBExecutor {
	if dbCtx := ctx.Value("db"); dbCtx != nil {
		if dbSqlx, ok := dbCtx.(*sqlx.Tx); ok {
			db = dbSqlx
		}
	}
	return db
}
