package driver

import (
	"context"
	"errors"
	"reflect"
)

// Value the compatible interface of Value in 'database/sql/driver' package.
type Value interface{}

// NamedValue the compatible structure of NamedValue in 'database/sql/driver' package.
type NamedValue struct {
	Name    string
	Ordinal int
	Value   Value
}

// Driver the compatible interface of Driver in 'database/sql/driver' package.
type Driver interface {
	Open(name string) (Conn, error)
}

// ErrSkip the compatible value of ErrSkip in 'database/sql/driver' package.
var ErrSkip = errors.New("driver: skip fast-path; continue as if unimplemented")

// ErrBadConn the compatible value of ErrBadConn in 'database/sql/driver' package.
var ErrBadConn = errors.New("driver: bad connection")

// Pinger the compatible interface of Pinger in 'database/sql/driver' package.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Execer the compatible interface of Execer in 'database/sql/driver' package.
type Execer interface {
	Exec(query string, args []Value) (Result, error)
}

// ExecerContext the compatible interface of ExecerContext in 'database/sql/driver' package.
type ExecerContext interface {
	ExecContext(ctx context.Context, query string, args []NamedValue) (Result, error)
}

// Queryer the compatible interface of Queryer in 'database/sql/driver' package.
type Queryer interface {
	Query(query string, args []Value) (Rows, error)
}

// QueryerContext the compatible interface of QueryerContext in 'database/sql/driver' package.
type QueryerContext interface {
	QueryContext(ctx context.Context, query string, args []NamedValue) (Rows, error)
}

// Conn the compatible interface of Conn in 'database/sql/driver' package.
type Conn interface {
	Prepare(query string) (Stmt, error)
	Close() error
	Begin() (Tx, error)
}

// ConnPrepareContext the compatible interface of ConnPrepareContext in 'database/sql/driver' package.
type ConnPrepareContext interface {
	PrepareContext(ctx context.Context, query string) (Stmt, error)
}

// IsolationLevel the compatible type of IsolationLevel in 'database/sql/driver' package.
type IsolationLevel int

// TxOptions the compatible structure of TxOptions in 'database/sql/driver' package.
type TxOptions struct {
	Isolation IsolationLevel
	ReadOnly  bool
}

// ConnBeginTx the compatible interface of ConnBeginTx in 'database/sql/driver' package.
type ConnBeginTx interface {
	BeginTx(ctx context.Context, opts TxOptions) (Tx, error)
}

// Result the compatible interface of Result in 'database/sql/driver' package.
type Result interface {
	LastInsertId() (int64, error)
	RowsAffected() (int64, error)
}

// Stmt the compatible interface of Stmt in 'database/sql/driver' package.
type Stmt interface {
	Close() error
	NumInput() int
	Exec(args []Value) (Result, error)
	Query(args []Value) (Rows, error)
}

// StmtExecContext the compatible interface of StmtExecContext in 'database/sql/driver' package.
type StmtExecContext interface {
	ExecContext(ctx context.Context, args []NamedValue) (Result, error)
}

// StmtQueryContext the compatible interface of StmtQueryContext in 'database/sql/driver' package.
type StmtQueryContext interface {
	QueryContext(ctx context.Context, args []NamedValue) (Rows, error)
}

// ColumnConverter the compatible interface of ColumnConverter in 'database/sql/driver' package.
type ColumnConverter interface {
	ColumnConverter(idx int) ValueConverter
}

// Rows the compatible interface of Rows in 'database/sql/driver' package.
type Rows interface {
	Columns() []string
	Close() error
	Next(dest []Value) error
}

// RowsNextResultSet the compatible interface of RowsNextResultSet in 'database/sql/driver' package.
type RowsNextResultSet interface {
	Rows
	HasNextResultSet() bool
	NextResultSet() error
}

// RowsColumnTypeScanType the compatible interface of RowsColumnTypeScanType in 'database/sql/driver' package.
type RowsColumnTypeScanType interface {
	Rows
	ColumnTypeScanType(index int) reflect.Type
}

// RowsColumnTypeDatabaseTypeName the compatible interface of RowsColumnTypeDatabaseTypeName in 'database/sql/driver' package.
type RowsColumnTypeDatabaseTypeName interface {
	Rows
	ColumnTypeDatabaseTypeName(index int) string
}

// RowsColumnTypeLength the compatible interface of RowsColumnTypeLength in 'database/sql/driver' package.
type RowsColumnTypeLength interface {
	Rows
	ColumnTypeLength(index int) (length int64, ok bool)
}

// RowsColumnTypeNullable the compatible interface of RowsColumnTypeNullable in 'database/sql/driver' package.
type RowsColumnTypeNullable interface {
	Rows
	ColumnTypeNullable(index int) (nullable, ok bool)
}

// RowsColumnTypePrecisionScale the compatible interface of RowsColumnTypePrecisionScale in 'database/sql/driver' package.
type RowsColumnTypePrecisionScale interface {
	Rows
	ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool)
}

// Tx the compatible interface of Tx in 'database/sql/driver' package.
type Tx interface {
	Commit() error
	Rollback() error
}

// RowsAffected the compatible type of RowsAffected in 'database/sql/driver' package.
type RowsAffected int64

var _ Result = RowsAffected(0)

// LastInsertId the compatible method of LastInsertId in 'database/sql/driver' package.
func (RowsAffected) LastInsertId() (int64, error) {
	return 0, errors.New("no LastInsertId available")
}

// RowsAffected the compatible method of RowsAffected in 'database/sql/driver' package.
func (v RowsAffected) RowsAffected() (int64, error) {
	return int64(v), nil
}
