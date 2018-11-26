package driver

import (
	"context"
	"errors"
	"reflect"
)

type Value interface{}

type NamedValue struct {
	Name    string
	Ordinal int
	Value   Value
}

type Driver interface {
	Open(name string) (Conn, error)
}

var ErrSkip = errors.New("driver: skip fast-path; continue as if unimplemented")
var ErrBadConn = errors.New("driver: bad connection")

type Pinger interface {
	Ping(ctx context.Context) error
}

type Execer interface {
	Exec(query string, args []Value) (Result, error)
}

type ExecerContext interface {
	ExecContext(ctx context.Context, query string, args []NamedValue) (Result, error)
}

type Queryer interface {
	Query(query string, args []Value) (Rows, error)
}

type QueryerContext interface {
	QueryContext(ctx context.Context, query string, args []NamedValue) (Rows, error)
}

type Conn interface {
	Prepare(query string) (Stmt, error)
	Close() error
	Begin() (Tx, error)
}

type ConnPrepareContext interface {
	PrepareContext(ctx context.Context, query string) (Stmt, error)
}

type IsolationLevel int

type TxOptions struct {
	Isolation IsolationLevel
	ReadOnly  bool
}

type ConnBeginTx interface {
	BeginTx(ctx context.Context, opts TxOptions) (Tx, error)
}

type Result interface {
	LastInsertId() (int64, error)
	RowsAffected() (int64, error)
}

type Stmt interface {
	Close() error
	NumInput() int
	Exec(args []Value) (Result, error)
	Query(args []Value) (Rows, error)
}

type StmtExecContext interface {
	ExecContext(ctx context.Context, args []NamedValue) (Result, error)
}

type StmtQueryContext interface {
	QueryContext(ctx context.Context, args []NamedValue) (Rows, error)
}

type ColumnConverter interface {
	ColumnConverter(idx int) ValueConverter
}

type Rows interface {
	Columns() []string
	Close() error
	Next(dest []Value) error
}

type RowsNextResultSet interface {
	Rows
	HasNextResultSet() bool
	NextResultSet() error
}

type RowsColumnTypeScanType interface {
	Rows
	ColumnTypeScanType(index int) reflect.Type
}

type RowsColumnTypeDatabaseTypeName interface {
	Rows
	ColumnTypeDatabaseTypeName(index int) string
}

type RowsColumnTypeLength interface {
	Rows
	ColumnTypeLength(index int) (length int64, ok bool)
}

type RowsColumnTypeNullable interface {
	Rows
	ColumnTypeNullable(index int) (nullable, ok bool)
}

type RowsColumnTypePrecisionScale interface {
	Rows
	ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool)
}

type Tx interface {
	Commit() error
	Rollback() error
}

type RowsAffected int64

var _ Result = RowsAffected(0)

func (RowsAffected) LastInsertId() (int64, error) {
	return 0, errors.New("no LastInsertId available")
}

func (v RowsAffected) RowsAffected() (int64, error) {
	return int64(v), nil
}
