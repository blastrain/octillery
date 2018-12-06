package sql

import (
	"context"
	core "database/sql"
	"fmt"

	vtparser "github.com/knocknote/vitess-sqlparser/sqlparser"
	"github.com/pkg/errors"
	"go.knocknote.io/octillery/connection"
	"go.knocknote.io/octillery/debug"
	"go.knocknote.io/octillery/exec"
	"go.knocknote.io/octillery/sqlparser"
)

// Tx the compatible type of Tx in 'database/sql' package.
type Tx struct {
	tx      *connection.TxConnection
	connMgr *connection.DBConnectionManager
	ctx     context.Context
	opts    *core.TxOptions
}

// WriteQueries informations of executed INSERT/UPDATE/DELETE query
func (proxy *Tx) WriteQueries() []*connection.QueryLog {
	return proxy.tx.WriteQueries
}

// ReadQueries informations of executed SELECT query
func (proxy *Tx) ReadQueries() []*connection.QueryLog {
	return proxy.tx.ReadQueries
}

func (proxy *Tx) connectionAndQuery(queryText string, args ...interface{}) (*connection.DBConnection, sqlparser.Query, error) {
	parser, err := sqlparser.New()
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	query, err := parser.Parse(queryText, args...)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	conn, err := proxy.connMgr.ConnectionByTableName(query.Table())
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return conn, query, nil
}

func (proxy *Tx) execProxy(ctx context.Context, queryText string, args ...interface{}) (Result, error) {
	conn, query, err := proxy.connectionAndQuery(queryText, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if proxy.tx == nil {
		proxy.tx = conn.Begin(proxy.ctx, proxy.opts)
	}
	if conn.IsShard {
		result, err := exec.NewQueryExecutor(ctx, conn, proxy.tx, query).Exec()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return result, nil
	}
	result, err := proxy.tx.Exec(ctx, conn, queryText, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return result, nil
}

func (proxy *Tx) prepareProxy(ctx context.Context, queryText string) (*core.Stmt, connection.Connection, error) {
	conn, query, err := proxy.connectionAndQuery(queryText)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	if proxy.tx == nil {
		proxy.tx = conn.Begin(proxy.ctx, proxy.opts)
	}
	if conn.IsShard {
		stmt, err := exec.NewQueryExecutor(ctx, conn, proxy.tx, query).Prepare()
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}
		return stmt, conn, nil
	}
	stmt, err := proxy.tx.Prepare(ctx, conn, queryText)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return stmt, conn, nil
}

func (proxy *Tx) stmtProxy(ctx context.Context, stmt *Stmt) (*core.Stmt, connection.Connection, error) {
	if stmt == nil {
		return nil, nil, errors.New("invalid stmt")
	}
	conn, query, err := proxy.connectionAndQuery(stmt.query)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	if proxy.tx == nil {
		proxy.tx = conn.Begin(proxy.ctx, proxy.opts)
	}
	if conn.IsShard {
		stmt, err := exec.NewQueryExecutor(ctx, conn, proxy.tx, query).Stmt()
		if err != nil {
			return nil, nil, errors.WithStack(err)
		}
		return stmt, conn, nil
	}
	result, err := proxy.tx.Stmt(ctx, conn, stmt.core)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return result, conn, nil
}

func (proxy *Tx) queryProxy(ctx context.Context, queryText string, args ...interface{}) (*Rows, error) {
	conn, query, err := proxy.connectionAndQuery(queryText, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if proxy.tx == nil {
		proxy.tx = conn.Begin(proxy.ctx, proxy.opts)
	}
	if conn.IsShard {
		rows, err := exec.NewQueryExecutor(ctx, conn, proxy.tx, query).Query()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return &Rows{cores: rows}, nil
	}

	rows, err := proxy.tx.Query(ctx, conn, queryText, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &Rows{cores: []*core.Rows{rows}}, nil
}

func (proxy *Tx) queryRowProxy(ctx context.Context, queryText string, args ...interface{}) *Row {
	conn, query, err := proxy.connectionAndQuery(queryText, args...)
	if err != nil {
		return &Row{err: err}
	}
	if proxy.tx == nil {
		proxy.tx = conn.Begin(proxy.ctx, proxy.opts)
	}
	if conn.IsShard {
		row, err := exec.NewQueryExecutor(ctx, conn, proxy.tx, query).QueryRow()
		if err != nil {
			return &Row{err: err}
		}
		return &Row{core: row}
	}
	row, err := proxy.tx.QueryRow(ctx, conn, queryText, args...)
	if err != nil {
		return &Row{err: err}
	}
	return &Row{core: row}
}

// Commit the compatible method of Commit in 'database/sql' package.
func (proxy *Tx) Commit() error {
	debug.Printf("Tx.Commit()")
	if err := proxy.tx.Commit(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// Rollback the compatible method of Rollback in 'database/sql' package.
func (proxy *Tx) Rollback() error {
	debug.Printf("Tx.Rollback()")
	if err := proxy.tx.Rollback(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// PrepareContext the compatible method of PrepareContext in 'database/sql' package.
func (proxy *Tx) PrepareContext(ctx context.Context, query string) (*Stmt, error) {
	debug.Printf("Tx.PrepareContext: %s", query)
	stmt, conn, err := proxy.prepareProxy(ctx, query)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &Stmt{
		core:  stmt,
		query: query,
		tx:    proxy.tx,
		conn:  conn,
	}, nil
}

// Prepare the compatible method of Prepare in 'database/sql' package.
func (proxy *Tx) Prepare(query string) (*Stmt, error) {
	debug.Printf("Tx.Prepare: %s", query)
	stmt, conn, err := proxy.prepareProxy(nil, query)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &Stmt{
		core:  stmt,
		query: query,
		tx:    proxy.tx,
		conn:  conn,
	}, nil
}

// StmtContext the compatible method of StmtContext in 'database/sql' package.
func (proxy *Tx) StmtContext(ctx context.Context, stmt *Stmt) *Stmt {
	debug.Printf("Tx.StmtContext")
	result, conn, err := proxy.stmtProxy(ctx, stmt)
	if err != nil {
		return &Stmt{err: err}
	}
	return &Stmt{
		core:  result,
		query: stmt.query,
		tx:    proxy.tx,
		conn:  conn,
	}
}

// Stmt the compatible method of Stmt in 'database/sql' package.
func (proxy *Tx) Stmt(stmt *Stmt) *Stmt {
	debug.Printf("Tx.Stmt")
	result, conn, err := proxy.stmtProxy(nil, stmt)
	if err != nil {
		return &Stmt{err: err}
	}
	return &Stmt{
		core:  result,
		query: stmt.query,
		tx:    proxy.tx,
		conn:  conn,
	}
}

// ExecContext the compatible method of ExecContext in 'database/sql' package.
func (proxy *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (Result, error) {
	debug.Printf("Tx.ExecContext: %s", query)
	result, err := proxy.execProxy(ctx, query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return result, nil
}

// Exec the compatible method of Exec in 'database/sql' package.
func (proxy *Tx) Exec(query string, args ...interface{}) (Result, error) {
	debug.Printf("Tx.Exec: %s", query)
	result, err := proxy.execProxy(nil, query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return result, nil
}

// QueryContext the compatible method of QueryContext in 'database/sql' package.
func (proxy *Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (*Rows, error) {
	debug.Printf("Tx.QueryContext: %s", query)
	rows, err := proxy.queryProxy(ctx, query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return rows, nil
}

// Query the compatible method of Query in 'database/sql' package.
func (proxy *Tx) Query(query string, args ...interface{}) (*Rows, error) {
	debug.Printf("Tx.Query: %s", query)
	rows, err := proxy.queryProxy(nil, query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return rows, nil
}

// QueryRowContext the compatible method of QueryRowContext in 'database/sql' package.
func (proxy *Tx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *Row {
	debug.Printf("Tx.QueryRowContext: %s", query)
	return proxy.queryRowProxy(ctx, query, args...)
}

// QueryRow the compatible method of QueryRow in 'database/sql' package.
func (proxy *Tx) QueryRow(query string, args ...interface{}) *Row {
	debug.Printf("Tx.QueryRow: %s", query)
	return proxy.queryRowProxy(nil, query, args...)
}

func (proxy *Tx) replaceInsertQueryByQueryLog(log *connection.QueryLog, query *sqlparser.InsertQuery) error {
	if log.LastInsertID == 0 {
		return nil
	}
	stmt := query.Stmt
	foundIDColumnIndex := -1
	for idx, column := range stmt.Columns {
		if column.String() == "id" {
			foundIDColumnIndex = idx
			break
		}
	}
	if foundIDColumnIndex >= 0 {
		val := vtparser.NewIntVal([]byte(fmt.Sprint(log.LastInsertID)))
		stmt.Rows.(vtparser.Values)[0][foundIDColumnIndex] = val
	} else {
		columns := vtparser.Columns{}
		columns = append(columns, vtparser.NewColIdent("id"))
		for _, column := range stmt.Columns {
			columns = append(columns, column)
		}
		stmt.Columns = columns
		values := vtparser.Values{vtparser.ValTuple{}}
		val := vtparser.NewIntVal([]byte(fmt.Sprint(log.LastInsertID)))
		values[0] = append(values[0], val)
		for _, val := range stmt.Rows.(vtparser.Values)[0] {
			values[0] = append(values[0], val)
		}
		stmt.Rows = values
	}
	return nil
}

// ExecWithQueryLog exec query by *connection.QueryLog.
// This is able to use for recovery from distributed transaction error.
func (proxy *Tx) ExecWithQueryLog(log *connection.QueryLog) (Result, error) {
	parser, err := sqlparser.New()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	query, err := parser.Parse(log.Query, log.Args)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	switch query.QueryType() {
	case sqlparser.Insert:
		proxy.replaceInsertQueryByQueryLog(log, query.(*sqlparser.InsertQuery))
		fallthrough
	case sqlparser.Update, sqlparser.Delete:
		conn, err := proxy.connMgr.ConnectionByTableName(query.Table())
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if proxy.tx == nil {
			proxy.tx = conn.Begin(proxy.ctx, proxy.opts)
		}
		if conn.IsShard {
			result, err := exec.NewQueryExecutor(proxy.ctx, conn, proxy.tx, query).Exec()
			if err != nil {
				return nil, errors.WithStack(err)
			}
			return result, nil
		}
		result, err := proxy.tx.Exec(proxy.ctx, conn, log.Query, log.Args...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return result, nil
	}
	return nil, errors.Errorf("cannot exec query type %d", query.QueryType())
}
