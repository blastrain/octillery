package sql

import (
	"context"
	core "database/sql"
	coredriver "database/sql/driver"
	"time"

	"github.com/pkg/errors"
	"go.knocknote.io/octillery/connection"
	"go.knocknote.io/octillery/debug"
	"go.knocknote.io/octillery/exec"
	"go.knocknote.io/octillery/sqlparser"
)

// QueryLog type for storing information of executed query
type QueryLog struct {
	Query        string        `json:"query"`
	Args         []interface{} `json:"args"`
	LastInsertID int64         `json:"lastInsertId"`
}

// DB the compatible structure of DB in 'database/sql' package.
type DB struct {
	connMgr *connection.DBConnectionManager
}

// Open the compatible method of Open in 'database/sql' package.
func Open(driverName, dataSourceName string) (*DB, error) {
	mgr, err := connection.NewConnectionManager()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if err := mgr.SetQueryString(dataSourceName); err != nil {
		return nil, errors.WithStack(err)
	}
	return &DB{connMgr: mgr}, nil
}

// ConnectionManager returns instance that manage all database connections.
func (db *DB) ConnectionManager() *connection.DBConnectionManager {
	return db.connMgr
}

// PingContext the compatible method of PingContext in 'database/sql' package.
// Currently, PingContext is ignored.
func (db *DB) PingContext(ctx context.Context) error {
	// ignore pingContext
	return nil
}

// Ping the compatible method of Ping in 'database/sql' package.
// Currently, Ping is ignored.
func (db *DB) Ping() error {
	// ignore Ping
	return nil
}

// Close the compatible method of Close in 'database/sql' package.
func (db *DB) Close() error {
	return db.connMgr.Close()
}

// SetMaxIdleConns the compatible method of SetMaxIdleConns in 'database/sql' package,
// call SetMaxIdleConns for all opened connections.
func (db *DB) SetMaxIdleConns(n int) {
	db.connMgr.SetMaxIdleConns(n)
}

// SetMaxOpenConns the compatible method of SetMaxOpenConns in 'database/sql' package,
// call SetMaxOpenConns for all opened connections.
func (db *DB) SetMaxOpenConns(n int) {
	db.connMgr.SetMaxOpenConns(n)
}

// SetConnMaxLifetime the compatible method of SetConnMaxLifetime in 'database/sql' package,
// call SetConnMaxLifetime for all opened connections.
func (db *DB) SetConnMaxLifetime(d time.Duration) {
	db.connMgr.SetConnMaxLifetime(d)
}

// Stats the compatible method of Stats in 'database/sql' package.
func (db *DB) Stats() DBStats {
	return DBStats{}
}

// PrepareContext the compatible method of PrepareContext in 'database/sql' package.
func (db *DB) PrepareContext(ctx context.Context, query string) (*Stmt, error) {
	debug.Printf("DB.PrepareContext: %s", query)
	stmt, err := db.prepareProxy(ctx, query)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &Stmt{core: stmt, query: query}, nil
}

// Prepare the compatible method of Prepare in 'database/sql' package.
func (db *DB) Prepare(query string) (*Stmt, error) {
	debug.Printf("DB.Prepare: %s", query)
	stmt, err := db.prepareProxy(nil, query)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &Stmt{core: stmt, query: query}, nil
}

// ExecContext the compatible method of ExecContext in 'database/sql' package.
func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (Result, error) {
	debug.Printf("DB.ExecContext: %s", query)
	result, err := db.execProxy(ctx, query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return result, nil
}

// Exec the compatible method of Exec in 'database/sql' package.
func (db *DB) Exec(query string, args ...interface{}) (Result, error) {
	debug.Printf("DB.Exec: %s", query)
	result, err := db.execProxy(nil, query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return result, nil
}

// QueryContext the compatible method of QueryContext in 'database/sql' package.
func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*Rows, error) {
	debug.Printf("DB.QueryContext: %s", query)
	rows, err := db.queryProxy(ctx, query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return rows, nil
}

// Query the compatible method of Query in 'database/sql' package.
func (db *DB) Query(query string, args ...interface{}) (*Rows, error) {
	debug.Printf("DB.Query: %s", query)
	rows, err := db.queryProxy(nil, query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return rows, nil
}

// QueryRowContext the compatible method of QueryRowContext in 'database/sql' package.
func (db *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *Row {
	debug.Printf("DB.QueryRowContext: %s", query)
	return db.queryRowProxy(ctx, query, args...)
}

// QueryRow the compatible method of QueryRow in 'database/sql' package.
func (db *DB) QueryRow(query string, args ...interface{}) *Row {
	debug.Printf("DB.QueryRow: %s", query)
	return db.queryRowProxy(nil, query, args...)
}

// BeginTx the compatible method of BeginTx in 'database/sql' package.
func (db *DB) BeginTx(ctx context.Context, opts *TxOptions) (*Tx, error) {
	debug.Printf("DB.BeginTx")
	if db.connMgr == nil {
		return nil, errors.New("cannot get connection manager from sql.(*DB)")
	}
	var coreopts *core.TxOptions
	if opts != nil {
		coreopts = &core.TxOptions{
			Isolation: core.IsolationLevel(opts.Isolation),
			ReadOnly:  opts.ReadOnly,
		}
	}
	return &Tx{
		tx:           nil,
		ctx:          ctx,
		opts:         coreopts,
		connMgr:      db.connMgr,
		WriteQueries: []*QueryLog{},
		ReadQueries:  []*QueryLog{},
	}, nil
}

// Begin the compatible method of Begin in 'database/sql' package.
func (db *DB) Begin() (*Tx, error) {
	debug.Printf("DB.Begin()")
	if db.connMgr == nil {
		return nil, errors.New("cannot get connection manager from sql.(*DB)")
	}
	return &Tx{
		tx:           nil,
		ctx:          nil,
		opts:         nil,
		connMgr:      db.connMgr,
		WriteQueries: []*QueryLog{},
		ReadQueries:  []*QueryLog{},
	}, nil
}

// Driver the compatible method of Driver in 'database/sql' package.
func (db *DB) Driver() coredriver.Driver {
	debug.Printf("DB.Driver()")
	return nil
}

func (db *DB) connectionAndQuery(queryText string, args ...interface{}) (*connection.DBConnection, sqlparser.Query, error) {
	parser, err := sqlparser.New()
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	query, err := parser.Parse(queryText, args...)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	conn, err := db.connMgr.ConnectionByTableName(query.Table())
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return conn, query, nil
}

func (db *DB) execProxy(ctx context.Context, queryText string, args ...interface{}) (Result, error) {
	conn, query, err := db.connectionAndQuery(queryText, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if conn.IsShard {
		result, err := exec.NewQueryExecutor(ctx, conn, nil, query).Exec()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return result, nil
	}
	result, err := conn.Exec(ctx, queryText, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return result, nil
}

func (db *DB) prepareProxy(ctx context.Context, queryText string) (*core.Stmt, error) {
	conn, query, err := db.connectionAndQuery(queryText)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if conn.IsShard {
		stmt, err := exec.NewQueryExecutor(ctx, conn, nil, query).Prepare()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return stmt, nil
	}
	stmt, err := conn.Prepare(ctx, queryText)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return stmt, nil
}

func (db *DB) queryProxy(ctx context.Context, queryText string, args ...interface{}) (*Rows, error) {
	conn, query, err := db.connectionAndQuery(queryText, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if conn.IsShard {
		rows, err := exec.NewQueryExecutor(ctx, conn, nil, query).Query()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return &Rows{cores: rows}, nil
	}
	rows, err := conn.Query(ctx, queryText, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &Rows{cores: []*core.Rows{rows}}, nil
}

func (db *DB) queryRowProxy(ctx context.Context, queryText string, args ...interface{}) *Row {
	conn, query, err := db.connectionAndQuery(queryText, args...)
	if err != nil {
		return &Row{err: err}
	}
	if conn.IsShard {
		row, err := exec.NewQueryExecutor(ctx, conn, nil, query).QueryRow()
		if err != nil {
			return &Row{err: err}
		}
		return &Row{core: row}
	}
	return &Row{core: conn.QueryRow(ctx, queryText, args...)}
}
