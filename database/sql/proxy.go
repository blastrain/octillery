package sql

import (
	"context"
	core "database/sql"
	coredriver "database/sql/driver"
	"reflect"
	"strings"

	"github.com/pkg/errors"
	"go.knocknote.io/octillery/database/sql/driver"
)

type NamedArg struct {
	Name  string
	Value interface{}
}

type TxOptions struct {
	Isolation IsolationLevel
	ReadOnly  bool
}

type NullString struct {
	core   core.NullString
	String string
	Valid  bool
}

type NullInt64 struct {
	core  core.NullInt64
	Int64 int64
	Valid bool
}

type NullFloat64 struct {
	core    core.NullFloat64
	Float64 float64
	Valid   bool
}

type NullBool struct {
	core  core.NullBool
	Bool  bool
	Valid bool
}

type DBStats struct {
	core core.DBStats
}

type Stmt struct {
	core  *core.Stmt
	err   error
	query string
}

type Rows struct {
	cores            []*core.Rows
	currentRowsIndex int
}

type ColumnType struct {
	core *core.ColumnType
}

type Row struct {
	core *core.Row
	err  error
}

type Result interface {
	LastInsertId() (int64, error)
	RowsAffected() (int64, error)
}

type RawBytes []byte

var ErrTxDone = errors.New("sql: Transaction has already been committed or rolled back")
var ErrNoRows = errors.New("sql: no rows in result set")

type driverProxy struct {
	driver driver.Driver
}

type connProxy struct {
	conn driver.Conn
}

type stmtProxy struct {
	stmt driver.Stmt
}

type resultProxy struct {
	result driver.Result
}

type rowsProxy struct {
	rows driver.Rows
}

type txProxy struct {
	tx driver.Tx
}

func (t *txProxy) Commit() error {
	return t.tx.Commit()
}

func (t *txProxy) Rollback() error {
	return t.tx.Rollback()
}

func (r *rowsProxy) Columns() []string {
	return r.rows.Columns()
}
func (r *rowsProxy) Close() error {
	return r.rows.Close()
}

func (r *rowsProxy) Next(dest []coredriver.Value) error {
	newDest := make([]driver.Value, len(dest))
	for idx, v := range dest {
		newDest[idx] = driver.Value(v)
	}
	err := r.rows.Next(newDest)
	for idx, _ := range dest {
		dest[idx] = newDest[idx]
	}
	return err
}

func (r *resultProxy) LastInsertId() (int64, error) {
	return r.result.LastInsertId()
}

func (r *resultProxy) RowsAffected() (int64, error) {
	return r.result.RowsAffected()
}

func (s *stmtProxy) Close() error {
	return s.stmt.Close()
}

func (s *stmtProxy) NumInput() int {
	return s.stmt.NumInput()
}

func (s *stmtProxy) Exec(args []coredriver.Value) (coredriver.Result, error) {
	newArgs := make([]driver.Value, len(args))
	for idx, v := range args {
		newArgs[idx] = driver.Value(v)
	}
	result, err := s.stmt.Exec(newArgs)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &resultProxy{result: result}, nil
}

func (s *stmtProxy) Query(args []coredriver.Value) (coredriver.Rows, error) {
	newArgs := make([]driver.Value, len(args))
	for idx, v := range args {
		newArgs[idx] = driver.Value(v)
	}
	rows, err := s.stmt.Query(newArgs)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &rowsProxy{rows: rows}, nil
}

func (c *connProxy) Prepare(query string) (coredriver.Stmt, error) {
	stmt, err := c.conn.Prepare(query)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &stmtProxy{stmt: stmt}, nil
}

func (c *connProxy) Close() error {
	return errors.WithStack(c.conn.Close())
}

func (c *connProxy) Begin() (coredriver.Tx, error) {
	tx, err := c.conn.Begin()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &txProxy{tx: tx}, nil
}

func (d *driverProxy) Open(dsn string) (coredriver.Conn, error) {
	conn, err := d.driver.Open(dsn)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &connProxy{conn: conn}, nil
}

func Register(name string, driver driver.Driver) {
	// ignore register from application
}

func RegisterByOctillery(name string, driver driver.Driver) {
	driverProxy := &driverProxy{}
	driverProxy.driver = driver
	core.Register(name, driverProxy)
}

func RegisterByOctilleryAsOriginalDriverType(name string, driver coredriver.Driver) {
	core.Register(name, driver)
}

func Drivers() []string {
	return core.Drivers()
}

func Named(name string, value interface{}) NamedArg {
	return NamedArg{Name: name, Value: value}
}

func (ns *NullString) Scan(value interface{}) error {
	ns.core.String = ns.String
	ns.core.Valid = ns.Valid
	if err := ns.core.Scan(value); err != nil {
		return errors.WithStack(err)
	}
	ns.String = ns.core.String
	ns.Valid = ns.core.Valid
	return nil
}

func (ns NullString) Value() (driver.Value, error) {
	if !ns.Valid {
		return nil, nil
	}
	return ns.String, nil
}

func (n *NullInt64) Scan(value interface{}) error {
	n.core.Int64 = n.Int64
	n.core.Valid = n.Valid
	if err := n.core.Scan(value); err != nil {
		return errors.WithStack(err)
	}
	n.Int64 = n.core.Int64
	n.Valid = n.core.Valid
	return nil
}

func (n NullInt64) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Int64, nil
}

func (n *NullFloat64) Scan(value interface{}) error {
	n.core.Float64 = n.Float64
	n.core.Valid = n.Valid
	if err := n.core.Scan(value); err != nil {
		return errors.WithStack(err)
	}
	n.Float64 = n.core.Float64
	n.Valid = n.core.Valid
	return nil
}

func (n NullFloat64) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Float64, nil
}

func (n *NullBool) Scan(value interface{}) error {
	n.core.Bool = n.Bool
	n.core.Valid = n.Valid
	if err := n.core.Scan(value); err != nil {
		return errors.WithStack(err)
	}
	n.Bool = n.core.Bool
	n.Valid = n.core.Valid
	return nil
}

func (n NullBool) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Bool, nil
}

func (s *Stmt) ExecContext(ctx context.Context, args ...interface{}) (core.Result, error) {
	if s.err != nil {
		return nil, errors.WithStack(s.err)
	}
	result, err := s.core.ExecContext(ctx, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return result, nil
}

func (s *Stmt) Exec(args ...interface{}) (core.Result, error) {
	if s.err != nil {
		return nil, errors.WithStack(s.err)
	}
	result, err := s.core.Exec(args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return result, nil
}

func (s *Stmt) QueryContext(ctx context.Context, args ...interface{}) (*Rows, error) {
	if s.err != nil {
		return nil, errors.WithStack(s.err)
	}
	rows, err := s.core.QueryContext(ctx, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &Rows{cores: []*core.Rows{rows}}, nil
}

func (s *Stmt) Query(args ...interface{}) (*Rows, error) {
	if s.err != nil {
		return nil, errors.WithStack(s.err)
	}
	rows, err := s.core.Query(args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &Rows{cores: []*core.Rows{rows}}, nil
}

func (s *Stmt) QueryRowContext(ctx context.Context, args ...interface{}) *Row {
	if s.err != nil {
		return &Row{err: s.err}
	}
	return &Row{core: s.core.QueryRowContext(ctx, args...)}
}

func (s *Stmt) QueryRow(args ...interface{}) *Row {
	if s.err != nil {
		return &Row{err: s.err}
	}
	return &Row{core: s.core.QueryRow(args...)}
}

func (s *Stmt) Close() error {
	return errors.WithStack(s.core.Close())
}

func (rs *Rows) index() int {
	idx := rs.currentRowsIndex
	if len(rs.cores) == rs.currentRowsIndex {
		idx = rs.currentRowsIndex - 1
	}
	return idx
}

func (rs *Rows) Next() bool {
	if len(rs.cores) == rs.currentRowsIndex {
		return false
	}
	existsNextRow := rs.cores[rs.currentRowsIndex].Next()
	if !existsNextRow {
		rs.currentRowsIndex += 1
		return rs.Next()
	}
	return true
}

func (rs *Rows) NextResultSet() bool {
	if len(rs.cores) == rs.currentRowsIndex {
		return false
	}
	existsNextResultSet := rs.cores[rs.currentRowsIndex].NextResultSet()
	if !existsNextResultSet {
		rs.currentRowsIndex += 1
		return rs.NextResultSet()
	}
	return true
}

func (rs *Rows) Err() error {
	return errors.WithStack(rs.cores[rs.index()].Err())
}

func (rs *Rows) Columns() ([]string, error) {
	columns, err := rs.cores[rs.index()].Columns()
	if err != nil {
		return []string{}, errors.WithStack(err)
	}
	return columns, nil
}

func (rs *Rows) ColumnTypes() ([]*ColumnType, error) {
	types, err := rs.cores[rs.index()].ColumnTypes()
	if err != nil {
		return []*ColumnType{}, errors.WithStack(err)
	}
	if types != nil {
		newTypes := make([]*ColumnType, len(types))
		for idx, columnType := range types {
			newTypes[idx] = &ColumnType{core: columnType}
		}
		return newTypes, nil
	}
	return nil, nil
}

func (rs *Rows) Scan(dest ...interface{}) error {
	return errors.WithStack(rs.cores[rs.index()].Scan(dest...))
}

func (rs *Rows) Close() error {
	errs := []string{}
	for _, core := range rs.cores {
		if err := core.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, ":"))
	}
	return nil
}

func (ci *ColumnType) Name() string {
	return ci.core.Name()
}

func (ci *ColumnType) Length() (length int64, ok bool) {
	return ci.core.Length()
}

func (ci *ColumnType) DecimalSize() (precision, scale int64, ok bool) {
	return ci.core.DecimalSize()
}

func (ci *ColumnType) ScanType() reflect.Type {
	return ci.core.ScanType()
}

func (ci *ColumnType) Nullable() (nullable, ok bool) {
	return ci.core.Nullable()
}

func (ci *ColumnType) DatabaseTypeName() string {
	return ci.core.DatabaseTypeName()
}

func (r *Row) Scan(dest ...interface{}) error {
	if r.err != nil {
		return errors.WithStack(r.err)
	}
	if r.core == nil {
		return errors.New("sql.Row pointer is nil")
	}
	return errors.WithStack(r.core.Scan(dest...))
}

type IsolationLevel int

const (
	LevelDefault IsolationLevel = iota
	LevelReadUncommitted
	LevelReadCommitted
	LevelWriteCommitted
	LevelRepeatableRead
	LevelSnapshot
	LevelSerializable
	LevelLinearizable
)

type Scanner interface {
	Scan(src interface{}) error
}
