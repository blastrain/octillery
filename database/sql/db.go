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

type QueryLog struct {
	Query        string        `json:"query"`
	Args         []interface{} `json:"args"`
	LastInsertID int64         `json:"lastInsertId"`
}

type DB struct {
	connMgr *connection.DBConnectionManager
}

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

func (db *DB) ConnectionManager() *connection.DBConnectionManager {
	return db.connMgr
}

func (db *DB) PingContext(ctx context.Context) error {
	// ignore pingContext
	return nil
}

func (db *DB) Ping() error {
	// ignore Ping
	return nil
}

func (db *DB) Close() error {
	return db.connMgr.Close()
}

func (db *DB) SetMaxIdleConns(n int) {
	db.connMgr.SetMaxIdleConns(n)
}

func (db *DB) SetMaxOpenConns(n int) {
	db.connMgr.SetMaxOpenConns(n)
}

func (db *DB) SetConnMaxLifetime(d time.Duration) {
	db.connMgr.SetConnMaxLifetime(d)
}

func (db *DB) Stats() DBStats {
	return DBStats{}
}

func (db *DB) PrepareContext(ctx context.Context, query string) (*Stmt, error) {
	debug.Printf("DB.PrepareContext: %s", query)
	stmt, err := db.prepareProxy(ctx, query)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &Stmt{core: stmt, query: query}, nil
}

func (db *DB) Prepare(query string) (*Stmt, error) {
	debug.Printf("DB.Prepare: %s", query)
	stmt, err := db.prepareProxy(nil, query)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &Stmt{core: stmt, query: query}, nil
}

func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (Result, error) {
	debug.Printf("DB.ExecContext: %s", query)
	result, err := db.execProxy(ctx, query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return result, nil
}

func (db *DB) Exec(query string, args ...interface{}) (Result, error) {
	debug.Printf("DB.Exec: %s", query)
	result, err := db.execProxy(nil, query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return result, nil
}

func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*Rows, error) {
	debug.Printf("DB.QueryContext: %s", query)
	rows, err := db.queryProxy(ctx, query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return rows, nil
}

func (db *DB) Query(query string, args ...interface{}) (*Rows, error) {
	debug.Printf("DB.Query: %s", query)
	rows, err := db.queryProxy(nil, query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return rows, nil
}

func (db *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *Row {
	debug.Printf("DB.QueryRowContext: %s", query)
	return db.queryRowProxy(ctx, query, args...)
}

func (db *DB) QueryRow(query string, args ...interface{}) *Row {
	debug.Printf("DB.QueryRow: %s", query)
	return db.queryRowProxy(nil, query, args...)
}

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
