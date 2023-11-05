package sql

import (
	"context"
	core "database/sql"
	coredriver "database/sql/driver"
	"reflect"
	"strings"

	"github.com/pkg/errors"
	"github.com/aokabi/octillery/connection"
	"github.com/aokabi/octillery/database/sql/driver"
)

// NamedArg the compatible structure of NamedArg in 'database/sql' package.
type NamedArg struct {
	Name  string
	Value interface{}
}

// TxOptions the compatible structure of TxOptions in 'database/sql' package.
type TxOptions struct {
	Isolation IsolationLevel
	ReadOnly  bool
}

// NullString the compatible structure of NullString in 'database/sql' package.
type NullString struct {
	core   core.NullString
	String string
	Valid  bool
}

// NullInt64 the compatible structure of NullInt64 in 'database/sql' package.
type NullInt64 struct {
	core  core.NullInt64
	Int64 int64
	Valid bool
}

// NullFloat64 the compatible structure of NullFloat64 in 'database/sql' package.
type NullFloat64 struct {
	core    core.NullFloat64
	Float64 float64
	Valid   bool
}

// NullBool is the compatible structure of NullBool in 'database/sql' package.
type NullBool struct {
	core  core.NullBool
	Bool  bool
	Valid bool
}

// DBStats the compatible structure of DBStats in 'database/sql' package.
type DBStats struct {
	core core.DBStats
}

// Stmt the compatible structure of Stmt in 'database/sql' package.
type Stmt struct {
	core  *core.Stmt
	err   error
	query string
	tx    *connection.TxConnection
	conn  connection.Connection
}

// Rows the compatible structure of Rows in 'database/sql' package.
type Rows struct {
	cores            []*core.Rows
	currentRowsIndex int
}

// ColumnType the compatible structure of ColumnType in 'database/sql' package.
type ColumnType struct {
	core *core.ColumnType
}

// Row the compatible structure of Row in 'database/sql' package.
type Row struct {
	core *core.Row
	err  error
}

// Result the compatible interface of Result in 'database/sql' package.
type Result interface {
	LastInsertId() (int64, error)
	RowsAffected() (int64, error)
}

// RawBytes the compatible type of RawBytes in 'database/sql' package.
type RawBytes []byte

// ErrTxDone the compatible value of ErrTxDone in 'database/sql' package.
var ErrTxDone = errors.New("sql: Transaction has already been committed or rolled back")

// ErrNoRows the compatible value of ErrNoRows in 'database/sql' package.
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
	for idx := range dest {
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

// Register the compatible method of Register in 'database/sql' package.
// If this called by application, ignore register driver.
// Octillery register driver by RegisterByOctillery instead.
func Register(name string, driver driver.Driver) {
	// ignore register from application
}

// RegisterByOctillery register driver by Octillery
func RegisterByOctillery(name string, driver driver.Driver) {
	driverProxy := &driverProxy{}
	driverProxy.driver = driver
	core.Register(name, driverProxy)
}

// Drivers the compatible method of Drivers in 'database/sql' package.
func Drivers() []string {
	return core.Drivers()
}

// Named the compatible method of Named in 'database/sql' package.
func Named(name string, value interface{}) NamedArg {
	return NamedArg{Name: name, Value: value}
}

// Scan the compatible method of Scan in 'database/sql' package.
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

// Value the compatible method of Value in 'database/sql' package.
func (ns NullString) Value() (driver.Value, error) {
	if !ns.Valid {
		return nil, nil
	}
	return ns.String, nil
}

// Scan the compatible method of Scan in 'database/sql' package.
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

// Value the compatible method of Value in 'database/sql' package.
func (n NullInt64) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Int64, nil
}

// Scan the compatible method of Scan in 'database/sql' package.
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

// Value the compatible method of Value in 'database/sql' package.
func (n NullFloat64) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Float64, nil
}

// Scan the compatible method of Scan in 'database/sql' package.
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

// Value the compatible method of Value in 'database/sql' package.
func (n NullBool) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Bool, nil
}

// ExecContext the compatible method of ExecContext in 'database/sql' package.
func (s *Stmt) ExecContext(ctx context.Context, args ...interface{}) (core.Result, error) {
	if s.err != nil {
		return nil, errors.WithStack(s.err)
	}
	result, err := s.core.ExecContext(ctx, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if s.tx == nil {
		return result, nil
	}
	if err := s.tx.AddWriteQuery(s.conn, result, s.query, args...); err != nil {
		return nil, errors.WithStack(err)
	}
	return result, nil
}

// Exec the compatible method of Exec in 'database/sql' package.
func (s *Stmt) Exec(args ...interface{}) (core.Result, error) {
	if s.err != nil {
		return nil, errors.WithStack(s.err)
	}
	result, err := s.core.Exec(args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if s.tx == nil {
		return result, nil
	}
	if err := s.tx.AddWriteQuery(s.conn, result, s.query, args...); err != nil {
		return nil, errors.WithStack(err)
	}
	return result, nil
}

// QueryContext the compatible method of QueryContext in 'database/sql' package.
func (s *Stmt) QueryContext(ctx context.Context, args ...interface{}) (*Rows, error) {
	if s.err != nil {
		return nil, errors.WithStack(s.err)
	}
	rows, err := s.core.QueryContext(ctx, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if s.tx != nil {
		s.tx.AddReadQuery(s.query, args...)
	}
	return &Rows{cores: []*core.Rows{rows}}, nil
}

// Query the compatible method of Query in 'database/sql' package.
func (s *Stmt) Query(args ...interface{}) (*Rows, error) {
	if s.err != nil {
		return nil, errors.WithStack(s.err)
	}
	rows, err := s.core.Query(args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if s.tx != nil {
		s.tx.AddReadQuery(s.query, args...)
	}
	return &Rows{cores: []*core.Rows{rows}}, nil
}

// QueryRowContext the compatible method of QueryRowContext in 'database/sql' package.
func (s *Stmt) QueryRowContext(ctx context.Context, args ...interface{}) *Row {
	if s.err != nil {
		return &Row{err: s.err}
	}
	if s.tx != nil {
		s.tx.AddReadQuery(s.query, args...)
	}
	return &Row{core: s.core.QueryRowContext(ctx, args...)}
}

// QueryRow the compatible method of QueryRow in 'database/sql' package.
func (s *Stmt) QueryRow(args ...interface{}) *Row {
	if s.err != nil {
		return &Row{err: s.err}
	}
	if s.tx != nil {
		s.tx.AddReadQuery(s.query, args...)
	}
	return &Row{core: s.core.QueryRow(args...)}
}

// Close the compatible method of Close in 'database/sql' package.
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

// Next the compatible method of Next in 'database/sql' package.
func (rs *Rows) Next() bool {
	if len(rs.cores) == rs.currentRowsIndex {
		return false
	}
	existsNextRow := rs.cores[rs.currentRowsIndex].Next()
	if !existsNextRow {
		rs.currentRowsIndex++
		return rs.Next()
	}
	return true
}

// NextResultSet the compatible method of NextResultSet in 'database/sql' package.
func (rs *Rows) NextResultSet() bool {
	if len(rs.cores) == rs.currentRowsIndex {
		return false
	}
	existsNextResultSet := rs.cores[rs.currentRowsIndex].NextResultSet()
	if !existsNextResultSet {
		rs.currentRowsIndex++
		return rs.NextResultSet()
	}
	return true
}

// Err the compatible method of Err in 'database/sql' package.
func (rs *Rows) Err() error {
	return errors.WithStack(rs.cores[rs.index()].Err())
}

// Columns the compatible method of Columns in 'database/sql' package.
func (rs *Rows) Columns() ([]string, error) {
	columns, err := rs.cores[rs.index()].Columns()
	if err != nil {
		return []string{}, errors.WithStack(err)
	}
	return columns, nil
}

// ColumnTypes the compatible method of ColumnTypes in 'database/sql' package.
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

// Scan the compatible method of Scan in 'database/sql' package.
func (rs *Rows) Scan(dest ...interface{}) error {
	return errors.WithStack(rs.cores[rs.index()].Scan(dest...))
}

// Close the compatible method of Close in 'database/sql' package.
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

// Name the compatible method of Name in 'database/sql' package.
func (ci *ColumnType) Name() string {
	return ci.core.Name()
}

// Length the compatible method of Length in 'database/sql' package.
func (ci *ColumnType) Length() (length int64, ok bool) {
	return ci.core.Length()
}

// DecimalSize the compatible method of DecimalSize in 'database/sql' package.
func (ci *ColumnType) DecimalSize() (precision, scale int64, ok bool) {
	return ci.core.DecimalSize()
}

// ScanType the compatible method of ScanType in 'database/sql' package.
func (ci *ColumnType) ScanType() reflect.Type {
	return ci.core.ScanType()
}

// Nullable the compatible method of Nullable in 'database/sql' package.
func (ci *ColumnType) Nullable() (nullable, ok bool) {
	return ci.core.Nullable()
}

// DatabaseTypeName the compatible method of DatabaseTypeName in 'database/sql' package.
func (ci *ColumnType) DatabaseTypeName() string {
	return ci.core.DatabaseTypeName()
}

// Scan the compatible method of Scan in 'database/sql' package.
func (r *Row) Scan(dest ...interface{}) error {
	if r.err != nil {
		return errors.WithStack(r.err)
	}
	if r.core == nil {
		return errors.New("sql.Row pointer is nil")
	}
	return errors.WithStack(r.core.Scan(dest...))
}

// IsolationLevel the compatible type of IsolationLevel in 'database/sql' package.
type IsolationLevel int

const (
	// LevelDefault the compatible of LevelDefault in 'database/sql' package.
	LevelDefault IsolationLevel = iota
	// LevelReadUncommitted the compatible of LevelReadUncommitted in 'database/sql' package.
	LevelReadUncommitted
	// LevelReadCommitted the compatible of LevelReadCommitted in 'database/sql' package.
	LevelReadCommitted
	// LevelWriteCommitted the compatible of LevelWriteCommitted in 'database/sql' package.
	LevelWriteCommitted
	// LevelRepeatableRead the compatible of LevelRepeatableRead in 'database/sql' package.
	LevelRepeatableRead
	// LevelSnapshot the compatible of LevelSnapshot in 'database/sql' package.
	LevelSnapshot
	// LevelSerializable the compatible of LevelSerializable in 'database/sql' package.
	LevelSerializable
	// LevelLinearizable the compatible of LevelLinearizable in 'database/sql' package.
	LevelLinearizable
)

// Scanner the compatible interface of Scanner in 'database/sql' package.
type Scanner interface {
	Scan(src interface{}) error
}
