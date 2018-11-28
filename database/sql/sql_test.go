package sql

import (
	"context"
	core "database/sql"
	"io"
	"path/filepath"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/pkg/errors"
	"go.knocknote.io/octillery/config"
	"go.knocknote.io/octillery/connection"
	"go.knocknote.io/octillery/connection/adapter"
	"go.knocknote.io/octillery/database/sql/driver"
	"go.knocknote.io/octillery/path"
)

type TestAdapter struct {
	adapterName                        string
	currentSequenceIDErr               error
	nextSequenceIDErr                  error
	execDDLErr                         error
	createSequencerTableIfNotExistsErr error
	insertRowToSequencerIfNotExistsErr error
}

func (t *TestAdapter) CurrentSequenceID(conn *core.DB, tableName string) (int64, error) {
	return 1, t.currentSequenceIDErr
}

func (t *TestAdapter) NextSequenceID(conn *core.DB, tableName string) (int64, error) {
	return 2, t.nextSequenceIDErr
}

func (t *TestAdapter) ExecDDL(config *config.DatabaseConfig) error {
	return t.execDDLErr
}

func (t *TestAdapter) OpenConnection(config *config.DatabaseConfig, queryValues string) (*core.DB, error) {
	return core.Open(t.adapterName, "")
}

func (t *TestAdapter) CreateSequencerTableIfNotExists(conn *core.DB, tableName string) error {
	return t.createSequencerTableIfNotExistsErr
}

func (t *TestAdapter) InsertRowToSequencerIfNotExists(conn *core.DB, tableName string) error {
	return t.insertRowToSequencerIfNotExistsErr
}

type TestDriver struct {
	openErr error
}

func (t *TestDriver) Open(name string) (driver.Conn, error) {
	return &TestConn{}, t.openErr
}

type TestConn struct {
	prepareErr error
	beginErr   error
	closeErr   error
	queryErr   error
}

func (t *TestConn) Prepare(query string) (driver.Stmt, error) {
	inputNum := len(regexp.MustCompile(`\?`).Split(query, -1)) - 1
	return &TestStmt{inputNum: inputNum}, t.prepareErr
}

func (t *TestConn) Begin() (driver.Tx, error) {
	return &TestTx{}, t.beginErr
}

func (t *TestConn) Close() error {
	return t.closeErr
}

func (t *TestConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return &TestRows{firstTime: true}, t.queryErr
}

type TestStmt struct {
	inputNum int
	closeErr error
	execErr  error
	queryErr error
}

func (t *TestStmt) Close() error {
	return t.closeErr
}

func (t *TestStmt) NumInput() int {
	return t.inputNum
}

func (t *TestStmt) Exec(args []driver.Value) (driver.Result, error) {
	return &TestResult{}, t.execErr
}

func (t *TestStmt) Query(args []driver.Value) (driver.Rows, error) {
	return &TestRows{firstTime: true}, t.queryErr
}

type TestResult struct {
	lastInsertIDErr error
	rowsAffectedErr error
}

func (t *TestResult) LastInsertId() (int64, error) {
	return 0, t.lastInsertIDErr
}

func (t *TestResult) RowsAffected() (int64, error) {
	return 0, t.rowsAffectedErr
}

type TestRows struct {
	firstTime bool
	closeErr  error
	nextErr   error
}

func (t *TestRows) Columns() []string {
	if t.firstTime {
		return []string{"name", "age", "is_god", "point"}
	}
	return []string{}
}

func (t *TestRows) Close() error {
	return t.closeErr
}

func (t *TestRows) Next(dest []driver.Value) error {
	if t.firstTime {
		dest[0] = "alice"
		dest[1] = 10
		dest[2] = true
		dest[3] = 3.14
		t.firstTime = false
	} else {
		return io.EOF
	}
	return t.nextErr
}

type TestTx struct {
	commitErr   error
	rollbackErr error
}

func (t *TestTx) Commit() error {
	return t.commitErr
}

func (t *TestTx) Rollback() error {
	return t.rollbackErr
}

func checkErr(t *testing.T, err error) {
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
}

func init() {
	if _, err := Open("sqlite3", "?parseTime=true&loc=Asia%2FTokyo"); err == nil {
		panic(errors.New("cannot handle error"))
	}
	adapter.Register("sqlite3", &TestAdapter{adapterName: "sqlite3"})
	RegisterByOctillery("sqlite3", &TestDriver{})
	confPath := filepath.Join(path.ThisDirPath(), "..", "..", "test_databases.yml")
	cfg, err := config.Load(confPath)
	if err != nil {
		panic(err)
	}
	if err := connection.SetConfig(cfg); err != nil {
		panic(err)
	}
}

func TestNamedValue(t *testing.T) {
	if Named("name", "alice").Name != "name" {
		t.Fatal("not work Named")
	}
}

func TestDrivers(t *testing.T) {
	drivers := Drivers()
	if len(drivers) != 1 {
		t.Fatal("not work Drivers")
	}
	if drivers[0] != "sqlite3" {
		t.Fatal("not work Drivers")
	}
}

func TestRegister(t *testing.T) {
	Register("sqlite3", &TestDriver{})
}

func testColumnType(t *testing.T, rows *Rows) {
	t.Run("validate column type", func(t *testing.T) {
		types, err := rows.ColumnTypes()
		checkErr(t, err)
		if len(types) != 4 {
			t.Fatal("cannot get columnTypes")
		}
		columnType := types[0]
		if columnType.Name() != "name" {
			t.Fatal("cannot work ColumnType.Name")
		}
		if _, ok := columnType.Length(); ok {
			t.Fatal("cannot work ColumnType.Length")
		}
		if _, _, ok := columnType.DecimalSize(); ok {
			t.Fatal("cannot work ColumnType.DecimalSize")
		}
		if columnType.ScanType().Kind() != reflect.Interface {
			t.Fatal("cannot work ColumnType.ScanType")
		}
		if _, ok := columnType.Nullable(); ok {
			t.Fatal("cannot work ColumnType.Nullable")
		}
		if name := columnType.DatabaseTypeName(); name != "" {
			t.Fatal("cannot work ColumnType.DatabaseTypeName")
		}
	})
}

func testRows(t *testing.T, rows *Rows) {
	for {
		for rows.Next() {
			var (
				name  string
				age   int
				isGod bool
				point float32
			)
			checkErr(t, rows.Scan(&name, &age, &isGod, &point))
			if name != "alice" {
				t.Fatal("cannot scan")
			}
			if age != 10 {
				t.Fatal("cannot scan")
			}
			if !isGod {
				t.Fatal("cannot scan")
			}
			if int(point) != 3 {
				t.Fatal("cannot scan")
			}
		}
		if !rows.NextResultSet() {
			break
		}
	}
}

func testPrepareWithNotShardingTable(ctx context.Context, t *testing.T, db *DB) {
	stmt, err := db.PrepareContext(ctx, "select name from user_stages where id = ?")
	checkErr(t, err)
	defer stmt.Close()
	t.Run("query", func(t *testing.T) {
		rows, err := stmt.Query(1)
		checkErr(t, err)
		defer rows.Close()
		t.Run("validate columns", func(t *testing.T) {
			columns, err := rows.Columns()
			checkErr(t, err)
			if len(columns) != 4 {
				t.Fatal("cannot get columns")
			}
			testColumnType(t, rows)
		})
		checkErr(t, rows.Err())
		testRows(t, rows)
	})
	t.Run("query context", func(t *testing.T) {
		rows, err := stmt.QueryContext(ctx, 1)
		checkErr(t, err)
		defer rows.Close()
		for rows.Next() {
			var (
				name  string
				age   int
				isGod bool
				point float32
			)
			checkErr(t, rows.Scan(&name, &age, &isGod, &point))
			if name != "alice" {
				t.Fatal("cannot scan")
			}
		}
		checkErr(t, rows.Err())
	})
}

func testPrepareContextWithNotShardingTable(ctx context.Context, t *testing.T, db *DB) {
	t.Run("query", func(t *testing.T) {
		stmt, err := db.Prepare("select * from user_stages where id = ?")
		checkErr(t, err)
		defer stmt.Close()
		t.Run("query row without context", func(t *testing.T) {
			var (
				name  string
				age   int
				isGod bool
				point float32
			)
			stmt.QueryRow(1).Scan(&name, &age, &isGod, &point)
			if name != "alice" {
				t.Fatal("cannot scan")
			}
		})
		t.Run("query row with context", func(t *testing.T) {
			var (
				name  string
				age   int
				isGod bool
				point float32
			)
			stmt.QueryRowContext(ctx, 1).Scan(&name, &age, &isGod, &point)
			if name != "alice" {
				t.Fatal("cannot scan")
			}
		})
	})
	t.Run("exec", func(t *testing.T) {
		stmt, err := db.Prepare("update user_stages set name = 'bob' where id = ?")
		checkErr(t, err)
		defer stmt.Close()
		t.Run("exec without context", func(t *testing.T) {
			result, err := stmt.Exec(1)
			checkErr(t, err)
			if _, err := result.LastInsertId(); err != nil {
				t.Fatalf("%+v\n", err)
			}
			if _, err := result.RowsAffected(); err != nil {
				t.Fatalf("%+v\n", err)
			}
		})
		t.Run("exec with context", func(t *testing.T) {
			result, err := stmt.ExecContext(ctx, 1)
			checkErr(t, err)
			if _, err := result.LastInsertId(); err != nil {
				t.Fatalf("%+v\n", err)
			}
			if _, err := result.RowsAffected(); err != nil {
				t.Fatalf("%+v\n", err)
			}
		})
	})
}

func TestDB(t *testing.T) {
	db, err := Open("sqlite3", "?parseTime=true&loc=Asia%2FTokyo")
	checkErr(t, err)
	defer db.Close()
	mgr := db.ConnectionManager()
	if mgr == nil {
		t.Fatal("cannot get connection manager")
	}
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(10)
	db.SetConnMaxLifetime(10 * time.Second)
	db.Stats()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	checkErr(t, db.PingContext(ctx))
	checkErr(t, db.Ping())
	t.Run("prepare context", func(t *testing.T) {
		t.Run("not sharding table", func(t *testing.T) {
			testPrepareWithNotShardingTable(ctx, t, db)
		})
	})
	t.Run("prepare", func(t *testing.T) {
		t.Run("not sharding table", func(t *testing.T) {
			testPrepareContextWithNotShardingTable(ctx, t, db)
		})
	})
	if _, err := db.ExecContext(ctx, "update users set name = 'alice' where id = 1"); err != nil {
		t.Fatalf("%+v\n", err)
	}
	if _, err := db.Exec("update users set name = 'alice' where id = 1"); err != nil {
		t.Fatalf("%+v\n", err)
	}
	if _, err := db.QueryContext(ctx, "select * from users"); err != nil {
		t.Fatalf("%+v\n", err)
	}
	if _, err := db.Query("select * from users"); err != nil {
		t.Fatalf("%+v\n", err)
	}
	if row := db.QueryRowContext(ctx, "select * from users"); row == nil {
		t.Fatal("invalid row instance")
	}
	if row := db.QueryRow("select * from users"); row == nil {
		t.Fatal("invalid row instance")
	}
	if _, err := db.BeginTx(ctx, &TxOptions{}); err != nil {
		t.Fatalf("%+v\n", err)
	}
	if _, err := db.Begin(); err != nil {
		t.Fatalf("%+v\n", err)
	}
}

func testTransactionStmtError(t *testing.T, tx *Tx, stmt *Stmt) {
	t.Run("error", func(t *testing.T) {
		if stmt := tx.Stmt(nil); stmt == nil {
			t.Fatal("cannot handle error")
		}
		invalidStmt := tx.StmtContext(nil, nil)
		if _, err := invalidStmt.ExecContext(nil, ""); err == nil {
			t.Fatal("cannot handle error")
		}
		if _, err := invalidStmt.Exec(""); err == nil {
			t.Fatal("cannot handle error")
		}
		if _, err := invalidStmt.QueryContext(nil, ""); err == nil {
			t.Fatal("cannot handle error")
		}
		if _, err := invalidStmt.Query(""); err == nil {
			t.Fatal("cannot handle error")
		}
	})
}

func testTransactionQueryRowWithoutContext(t *testing.T, stmt *Stmt) {
	t.Run("query row without context", func(t *testing.T) {
		var (
			name  NullString
			age   NullInt64
			isGod NullBool
			point NullFloat64
		)
		checkErr(t, stmt.QueryRow(1).Scan(&name, &age, &isGod, &point))
		nameValue, err := name.Value()
		checkErr(t, err)
		if nameValue.(string) != "alice" {
			t.Fatal("cannot scan")
		}
		ageValue, err := age.Value()
		checkErr(t, err)
		if ageValue.(int64) != 10 {
			t.Fatal("cannot scan")
		}
		isGodValue, err := isGod.Value()
		checkErr(t, err)
		if !isGodValue.(bool) {
			t.Fatal("cannot scan")
		}
		pointValue, err := point.Value()
		checkErr(t, err)
		if int(pointValue.(float64)) != 3 {
			t.Fatal("cannot scan")
		}
	})
}

func testTransactionQueryRowWithContext(ctx context.Context, t *testing.T, stmt *Stmt) {
	t.Run("query row with context", func(t *testing.T) {
		var (
			name  NullString
			age   NullInt64
			isGod NullBool
			point NullFloat64
		)
		stmt.QueryRowContext(ctx, 1).Scan(&name, &age, &isGod, &point)
		nameValue, err := name.Value()
		checkErr(t, err)
		if nameValue.(string) != "alice" {
			t.Fatal("cannot scan")
		}
	})
}

func testTransactionWithNotShardingTable(ctx context.Context, t *testing.T, tx *Tx) {
	t.Run("query", func(t *testing.T) {
		stmt, err := tx.PrepareContext(ctx, "select * from user_stages where id = ?")
		checkErr(t, err)
		defer stmt.Close()
		if stmt := tx.StmtContext(ctx, stmt); stmt == nil {
			t.Fatalf("invalid stmt instance")
		}
		if stmt := tx.Stmt(stmt); stmt == nil {
			t.Fatalf("invalid stmt instance")
		} else {
			if _, err := stmt.Exec("update user_stages set name = 'alice'"); err != nil {
				t.Fatalf("%+v\n", err)
			}
		}
		testTransactionStmtError(t, tx, stmt)
		testTransactionQueryRowWithoutContext(t, stmt)
		testTransactionQueryRowWithContext(ctx, t, stmt)
	})
	t.Run("exec", func(t *testing.T) {
		stmt, err := tx.Prepare("update user_stages set name = 'bob' where id = ?")
		checkErr(t, err)
		defer stmt.Close()
		t.Run("exec without context", func(t *testing.T) {
			result, err := stmt.Exec(1)
			checkErr(t, err)
			if _, err := result.LastInsertId(); err != nil {
				t.Fatalf("%+v\n", err)
			}
			if _, err := result.RowsAffected(); err != nil {
				t.Fatalf("%+v\n", err)
			}
		})
		t.Run("exec with context", func(t *testing.T) {
			result, err := stmt.ExecContext(ctx, 1)
			checkErr(t, err)
			if _, err := result.LastInsertId(); err != nil {
				t.Fatalf("%+v\n", err)
			}
			if _, err := result.RowsAffected(); err != nil {
				t.Fatalf("%+v\n", err)
			}
		})
	})
}

func TestTransaction(t *testing.T) {
	db, err := Open("sqlite3", "?parseTime=true&loc=Asia%2FTokyo")
	checkErr(t, err)
	defer db.Close()
	tx, err := db.Begin()
	checkErr(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Run("prepare context", func(t *testing.T) {
		t.Run("not sharding table", func(t *testing.T) {
			testTransactionWithNotShardingTable(ctx, t, tx)
		})
	})

	// transaction error. cannot access other database by same Tx instance
	if _, err := tx.ExecContext(ctx, "update users set name = 'alice' where id = 1"); err == nil {
		t.Fatal("cannot handle error")
	}
	if _, err := tx.ExecContext(ctx, "update user_stages set name = 'alice' where id = 1"); err != nil {
		t.Fatalf("%+v\n", err)
	}
	if _, err := tx.Exec("update user_stages set name = 'alice' where id = 1"); err != nil {
		t.Fatalf("%+v\n", err)
	}
	if _, err := tx.QueryContext(ctx, "select * from user_stages"); err != nil {
		t.Fatalf("%+v\n", err)
	}
	if _, err := tx.Query("select * from user_stages"); err != nil {
		t.Fatalf("%+v\n", err)
	}
	if row := tx.QueryRowContext(ctx, "select * from user_stages"); row == nil {
		t.Fatal("invalid row instance")
	}
	if row := tx.QueryRow("select * from user_stages"); row == nil {
		t.Fatal("invalid row instance")
	}

	checkErr(t, tx.Commit())

	t.Run("rollback", func(t *testing.T) {
		tx, err := db.Begin()
		checkErr(t, err)
		if _, err := tx.ExecContext(ctx, "update users set name = 'alice' where id = 1"); err != nil {
			t.Fatalf("%+v\n", err)
		}
		if _, err := tx.Exec("update users set name = 'alice' where id = 1"); err != nil {
			t.Fatalf("%+v\n", err)
		}
		if _, err := tx.QueryContext(ctx, "select * from users"); err != nil {
			t.Fatalf("%+v\n", err)
		}
		if _, err := tx.Query("select * from users"); err != nil {
			t.Fatalf("%+v\n", err)
		}
		if row := tx.QueryRowContext(ctx, "select * from users"); row == nil {
			t.Fatal("invalid row instance")
		}
		if row := tx.QueryRow("select * from users"); row == nil {
			t.Fatal("invalid row instance")
		}
		checkErr(t, tx.Rollback())
	})
}

var errOpen = errors.New("open error")

func testPrepareError(t *testing.T, db *DB) {
	t.Run("error prepare", func(t *testing.T) {
		stmt, err := db.Prepare("select name from user_errors where id = ?")
		if errors.Cause(err) != errOpen {
			t.Fatalf("%+v\n", err)
		}
		if stmt != nil {
			t.Fatal("cannot handle error")
		}
	})
}

func testPrepareContextError(t *testing.T, db *DB) {
	t.Run("error prepare context", func(t *testing.T) {
		stmt, err := db.PrepareContext(nil, "select name from user_errors where id = ?")
		if errors.Cause(err) != errOpen {
			t.Fatalf("%+v\n", err)
		}
		if stmt != nil {
			t.Fatal("cannot handle error")
		}
	})
}

func testExecError(t *testing.T, db *DB) {
	t.Run("error exec", func(t *testing.T) {
		result, err := db.Exec("update user_errors set name = 'alice' where id = ?", 1)
		if errors.Cause(err) != errOpen {
			t.Fatalf("%+v\n", err)
		}
		if result != nil {
			t.Fatal("cannot handle error")
		}
	})
}

func testExecContextError(t *testing.T, db *DB) {
	t.Run("error exec context", func(t *testing.T) {
		result, err := db.ExecContext(nil, "update user_errors set name = 'alice' where id = ?", 1)
		if errors.Cause(err) != errOpen {
			t.Fatalf("%+v\n", err)
		}
		if result != nil {
			t.Fatal("cannot handle error")
		}
	})
}

func testQueryError(t *testing.T, db *DB) {
	t.Run("error query", func(t *testing.T) {
		rows, err := db.Query("select * from user_errors")
		if errors.Cause(err) != errOpen {
			t.Fatalf("%+v\n", err)
		}
		if rows != nil {
			t.Fatal("cannot handle error")
		}
	})
}

func testQueryContextError(t *testing.T, db *DB) {
	t.Run("error query context", func(t *testing.T) {
		rows, err := db.QueryContext(nil, "select * from user_errors")
		if errors.Cause(err) != errOpen {
			t.Fatalf("%+v\n", err)
		}
		if rows != nil {
			t.Fatal("cannot handle error")
		}
	})
}

func testQueryRowError(t *testing.T, db *DB) {
	t.Run("error query row", func(t *testing.T) {
		row := db.QueryRow("select * from user_errors where id = 1")
		var name string
		if err := row.Scan(&name); errors.Cause(err) != errOpen {
			t.Fatalf("%+v\n", err)
		}
	})
}

func testQueryRowContextError(t *testing.T, db *DB) {
	t.Run("error query row context", func(t *testing.T) {
		row := db.QueryRowContext(nil, "select * from user_errors where id = 1")
		var name string
		if err := row.Scan(&name); errors.Cause(err) != errOpen {
			t.Fatalf("%+v\n", err)
		}
	})
}

func testPrepareTransactionError(t *testing.T, tx *Tx) {
	t.Run("error prepare", func(t *testing.T) {
		stmt, err := tx.Prepare("select name from user_errors where id = ?")
		if errors.Cause(err) != errOpen {
			t.Fatalf("%+v\n", err)
		}
		if stmt != nil {
			t.Fatal("cannot handle error")
		}
	})
}

func testPrepareContextTransactionError(t *testing.T, tx *Tx) {
	t.Run("error prepare context", func(t *testing.T) {
		stmt, err := tx.PrepareContext(nil, "select name from user_errors where id = ?")
		if errors.Cause(err) != errOpen {
			t.Fatalf("%+v\n", err)
		}
		if stmt != nil {
			t.Fatal("cannot handle error")
		}
	})
}

func testExecTransactionError(t *testing.T, tx *Tx) {
	t.Run("error exec", func(t *testing.T) {
		result, err := tx.Exec("update user_errors set name = 'alice' where id = ?", 1)
		if errors.Cause(err) != errOpen {
			t.Fatalf("%+v\n", err)
		}
		if result != nil {
			t.Fatal("cannot handle error")
		}
	})
}

func testExecContextTransactionError(t *testing.T, tx *Tx) {
	t.Run("error exec context", func(t *testing.T) {
		result, err := tx.ExecContext(nil, "update user_errors set name = 'alice' where id = ?", 1)
		if errors.Cause(err) != errOpen {
			t.Fatalf("%+v\n", err)
		}
		if result != nil {
			t.Fatal("cannot handle error")
		}
	})
}

func testQueryTransactionError(t *testing.T, tx *Tx) {
	t.Run("error query", func(t *testing.T) {
		rows, err := tx.Query("select * from user_errors")
		if errors.Cause(err) != errOpen {
			t.Fatalf("%+v\n", err)
		}
		if rows != nil {
			t.Fatal("cannot handle error")
		}
	})
}

func testQueryContextTransactionError(t *testing.T, tx *Tx) {
	t.Run("error query context", func(t *testing.T) {
		rows, err := tx.QueryContext(nil, "select * from user_errors")
		if errors.Cause(err) != errOpen {
			t.Fatalf("%+v\n", err)
		}
		if rows != nil {
			t.Fatal("cannot handle error")
		}
	})
}

func testQueryRowTransactionError(t *testing.T, tx *Tx) {
	t.Run("error query row", func(t *testing.T) {
		row := tx.QueryRow("select * from user_errors where id = 1")
		var name string
		if err := row.Scan(&name); errors.Cause(err) != errOpen {
			t.Fatalf("%+v\n", err)
		}
	})
}

func testQueryRowContextTransactionError(t *testing.T, tx *Tx) {
	t.Run("error query row context", func(t *testing.T) {
		row := tx.QueryRowContext(nil, "select * from user_errors where id = 1")
		var name string
		if err := row.Scan(&name); errors.Cause(err) != errOpen {
			t.Fatalf("%+v\n", err)
		}
	})
}

func TestError(t *testing.T) {
	adapter.Register("test", &TestAdapter{adapterName: "test"})
	confPath := filepath.Join(path.ThisDirPath(), "error_config.yml")
	cfg, err := config.Load(confPath)
	checkErr(t, err)
	checkErr(t, connection.SetConfig(cfg))

	RegisterByOctillery("test", &TestDriver{openErr: errOpen})
	t.Run("invalid query string", func(t *testing.T) {
		if _, err := Open("", "?#%"); err == nil {
			t.Fatal("cannot handle error")
		}
	})
	db, err := Open("", "")
	checkErr(t, err)
	testPrepareError(t, db)
	testPrepareContextError(t, db)
	testExecError(t, db)
	testExecContextError(t, db)
	testQueryError(t, db)
	testQueryContextError(t, db)
	testQueryRowError(t, db)
	testQueryRowContextError(t, db)

	tx, err := db.Begin()
	checkErr(t, err)
	testPrepareTransactionError(t, tx)
	testPrepareContextTransactionError(t, tx)
	testExecTransactionError(t, tx)
	testExecContextTransactionError(t, tx)
	testQueryTransactionError(t, tx)
	testQueryContextTransactionError(t, tx)
	testQueryRowTransactionError(t, tx)
	testQueryRowContextTransactionError(t, tx)
	t.Run("error commit", func(t *testing.T) {
		if err := tx.Commit(); err == nil {
			t.Fatal("cannot handle error")
		}
	})
}
