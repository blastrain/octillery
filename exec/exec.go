package exec

import (
	"context"
	"database/sql"

	"github.com/pkg/errors"
	"go.knocknote.io/octillery/connection"
	"go.knocknote.io/octillery/sqlparser"
)

type mergedResult struct {
	affectedRows   int64
	lastInsertedID int64
	err            error
}

func (r *mergedResult) LastInsertId() (int64, error) {
	return r.lastInsertedID, r.err
}

func (r *mergedResult) RowsAffected() (int64, error) {
	return r.affectedRows, r.err
}

// QueryExecutor the interface for executing query to shards
type QueryExecutor interface {
	Query() ([]*sql.Rows, error)
	QueryRow() (*sql.Row, error)
	Prepare() (*sql.Stmt, error)
	Stmt() (*sql.Stmt, error)
	Exec() (sql.Result, error)
}

// QueryExecutorBase a implementation of QueryExecutor interface.
type QueryExecutorBase struct {
	ctx   context.Context
	tx    *connection.TxConnection
	conn  *connection.DBConnection
	query sqlparser.Query
}

// Prepare executes prepare for shards.
// Currently, this is not supported.
func (e *QueryExecutorBase) Prepare() (*sql.Stmt, error) {
	return nil, errors.New("currently not supported Prepare() for sharding table")
}

// Stmt executes stmt for shards.
// Currently, this is not supported.
func (e *QueryExecutorBase) Stmt() (*sql.Stmt, error) {
	return nil, errors.New("currently not supported Stmt() for sharding table")
}

func (e *QueryExecutorBase) exec(conn connection.Connection, query string, args ...interface{}) (sql.Result, error) {
	if e.tx != nil {
		result, err := e.tx.Exec(e.ctx, conn, query, args...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return result, nil
	}

	if e.ctx == nil {
		return conn.Conn().Exec(query, args...)
	}
	return conn.Conn().ExecContext(e.ctx, query, args...)
}

func (e *QueryExecutorBase) execQuery(conn connection.Connection, query string, args ...interface{}) (*sql.Rows, error) {
	if e.tx != nil {
		return e.tx.Query(e.ctx, conn, query, args...)
	}

	if e.ctx == nil {
		return conn.Conn().Query(query, args...)
	}
	return conn.Conn().QueryContext(e.ctx, query, args...)
}

func (e *QueryExecutorBase) execQueryRow(conn connection.Connection, query string, args ...interface{}) (*sql.Row, error) {
	if e.tx != nil {
		row, err := e.tx.QueryRow(e.ctx, conn, query, args...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return row, nil
	}

	if e.ctx == nil {
		return conn.Conn().QueryRow(query, args...), nil
	}
	return conn.Conn().QueryRowContext(e.ctx, query, args...), nil
}

// NewQueryExecutor creates instance of QueryExecutor interface.
// If specify unknown query type, returns nil
func NewQueryExecutor(ctx context.Context, conn *connection.DBConnection, tx *connection.TxConnection, query sqlparser.Query) QueryExecutor {
	base := &QueryExecutorBase{
		ctx:   ctx,
		tx:    tx,
		query: query,
		conn:  conn,
	}
	switch query.QueryType() {
	case sqlparser.CreateTable:
		return NewCreateTableQueryExecutor(base)
	case sqlparser.TruncateTable:
		return NewTruncateQueryExecutor(base)
	case sqlparser.Select:
		return NewSelectQueryExecutor(base)
	case sqlparser.Insert:
		return NewInsertQueryExecutor(base)
	case sqlparser.Update:
		return NewUpdateQueryExecutor(base)
	case sqlparser.Delete:
		return NewDeleteQueryExecutor(base)
	case sqlparser.Drop:
		return NewDropQueryExecutor(base)
	case sqlparser.Show:
		return NewShowQueryExecutor(base)
	default:
	}
	return nil
}
