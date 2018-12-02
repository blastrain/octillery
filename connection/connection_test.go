package connection

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"log"
	"path/filepath"
	"testing"
	"time"

	"go.knocknote.io/octillery/config"
	"go.knocknote.io/octillery/connection/adapter"
	"go.knocknote.io/octillery/path"
)

type TestAdapter struct {
}

func (t *TestAdapter) CurrentSequenceID(conn *sql.DB, tableName string) (int64, error) {
	return 1, nil
}

func (t *TestAdapter) NextSequenceID(conn *sql.DB, tableName string) (int64, error) {
	return 2, nil
}

func (t *TestAdapter) ExecDDL(config *config.DatabaseConfig) error {
	return nil
}

func (t *TestAdapter) OpenConnection(config *config.DatabaseConfig, queryValues string) (*sql.DB, error) {
	return sql.Open("sqlite3", "")
}

func (t *TestAdapter) CreateSequencerTableIfNotExists(conn *sql.DB, tableName string) error {
	return nil
}

func (t *TestAdapter) InsertRowToSequencerIfNotExists(conn *sql.DB, tableName string) error {
	return nil
}

type TestDriver struct {
}

func (t *TestDriver) Open(name string) (driver.Conn, error) {
	return &TestConn{}, nil
}

type TestConn struct {
}

func (t *TestConn) Prepare(query string) (driver.Stmt, error) {
	return &TestStmt{}, nil
}

func (t *TestConn) Begin() (driver.Tx, error) {
	return &TestTx{}, nil
}

func (t *TestConn) Close() error {
	return nil
}

func (t *TestConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return &TestRows{}, nil
}

type TestStmt struct {
}

func (t *TestStmt) Close() error {
	return nil
}

func (t *TestStmt) NumInput() int {
	return 0
}

func (t *TestStmt) Exec(args []driver.Value) (driver.Result, error) {
	return &TestResult{}, nil
}

func (t *TestStmt) Query(args []driver.Value) (driver.Rows, error) {
	return &TestRows{}, nil
}

type TestResult struct {
}

func (t *TestResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (t *TestResult) RowsAffected() (int64, error) {
	return 0, nil
}

type TestRows struct {
}

func (t *TestRows) Columns() []string {
	return []string{}
}

func (t *TestRows) Close() error {
	return nil
}

func (t *TestRows) Next(dest []driver.Value) error {
	return nil
}

type TestTx struct {
}

func (t *TestTx) Commit() error {
	return nil
}

func (t *TestTx) Rollback() error {
	return nil
}

func checkErr(t *testing.T, err error) {
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
}

func init() {
	SetBeforeCommitCallback(func(tx *TxConnection, writeQueries []*QueryLog) error {
		log.Println("BeforeCommit", writeQueries)
		return nil
	})
	SetAfterCommitCallback(func(*TxConnection) {
		log.Println("AfterCommit")
	}, func(tx *TxConnection, isCriticalError bool, failureQueries []*QueryLog) {
		log.Println("AfterCommit", failureQueries)
	})
	adapter.Register("sqlite3", &TestAdapter{})
	sql.Register("sqlite3", &TestDriver{})
	confPath := filepath.Join(path.ThisDirPath(), "..", "test_databases.yml")
	cfg, err := config.Load(confPath)
	if err != nil {
		panic(err)
	}
	if err := SetConfig(cfg); err != nil {
		panic(err)
	}
}

func TestSetQueryString(t *testing.T) {
	mgr, err := NewConnectionManager()
	checkErr(t, err)
	defer mgr.Close()
	if err := mgr.SetQueryString("?#%"); err == nil {
		t.Fatal("cannot handle error")
	}
	checkErr(t, mgr.SetQueryString(""))
	checkErr(t, mgr.SetQueryString("?parseTime=true&loc=Asia%2FTokyo"))
}

func TestGetConnection(t *testing.T) {
	mgr, err := NewConnectionManager()
	checkErr(t, err)
	defer mgr.Close()
	t.Run("connection by users table", func(t *testing.T) {
		conn, err := mgr.ConnectionByTableName("users")
		checkErr(t, err)
		if c, _ := mgr.ConnectionByTableName("users"); c != conn {
			t.Fatal("cannot fetch same instance")
		}
		if conn.ShardConnections.ShardNum() != 2 {
			t.Fatal("cannot get shard num")
		}
		if len(conn.ShardConnections.AllShard()) != 2 {
			t.Fatal("cannot get all shard")
		}
		shardConn := conn.ShardConnections.ShardConnectionByName("user_shard_1")
		if shardConn == nil {
			t.Fatal("cannot get shard connection by name")
		}
		shardConnByIndex := conn.ShardConnections.ShardConnectionByIndex(0)
		if shardConnByIndex == nil {
			t.Fatal("cannot get shard connection by index")
		}
		if _, err := mgr.SequencerConnectionByTableName("users"); err != nil {
			t.Fatalf("%+v\n", err)
		}
	})
	t.Run("connection by user_stages table", func(t *testing.T) {
		if _, err := mgr.ConnectionByTableName("user_stages"); err != nil {
			t.Fatalf("%+v\n", err)
		}
	})
}

func TestSetSettings(t *testing.T) {
	mgr, err := NewConnectionManager()
	checkErr(t, err)
	defer mgr.Close()
	mgr.SetMaxIdleConns(10)
	mgr.SetMaxOpenConns(10)
	mgr.SetConnMaxLifetime(10 * time.Second)
}

func TestCurrentSequenceID(t *testing.T) {
	mgr, err := NewConnectionManager()
	checkErr(t, err)
	defer mgr.Close()
	id, err := mgr.CurrentSequenceID("users")
	checkErr(t, err)
	if id != 1 {
		t.Fatal("cannot get current sequence id")
	}
}

func TestNextSequenceID(t *testing.T) {
	mgr, err := NewConnectionManager()
	checkErr(t, err)
	defer mgr.Close()
	id, err := mgr.NextSequenceID("users")
	checkErr(t, err)
	if id != 2 {
		t.Fatal("cannot get next sequence id")
	}
	{
		conn, err := mgr.ConnectionByTableName("users")
		checkErr(t, err)
		id, err := conn.NextSequenceID("users")
		checkErr(t, err)
		if id != 2 {
			t.Fatal("cannot get next sequence id")
		}
	}
}

func TestIsShardTable(t *testing.T) {
	mgr, err := NewConnectionManager()
	checkErr(t, err)
	defer mgr.Close()
	if !mgr.IsShardTable("users") {
		t.Fatal("cannot set is_shard configuration")
	}
	if mgr.IsShardTable("user_stages") {
		t.Fatal("cannot set is_shard configuration")
	}
}

func TestEqualDSN(t *testing.T) {
	mgr, err := NewConnectionManager()
	checkErr(t, err)
	defer mgr.Close()
	conn, err := mgr.ConnectionByTableName("users")
	checkErr(t, err)
	t.Run("same instance", func(t *testing.T) {
		if !conn.EqualDSN(conn) {
			t.Fatal("cannot work equal dsn")
		}
	})
	t.Run("another instance", func(t *testing.T) {
		mgr, err := NewConnectionManager()
		checkErr(t, err)
		defer mgr.Close()
		anotherConn, err := mgr.ConnectionByTableName("users")
		checkErr(t, err)
		if !conn.EqualDSN(anotherConn) {
			t.Fatal("cannot work equal dsn")
		}
	})
}

func TestIsEqualShardColumnToShardKeyColumn(t *testing.T) {
	mgr, err := NewConnectionManager()
	checkErr(t, err)
	defer mgr.Close()
	if !mgr.IsEqualShardColumnToShardKeyColumn("users") {
		t.Fatal("cannot set shard_column and shard_key")
	}
	if mgr.IsEqualShardColumnToShardKeyColumn("user_items") {
		t.Fatal("cannot set shard_column and shard_key")
	}
	if mgr.IsEqualShardColumnToShardKeyColumn("user_decks") {
		t.Fatal("cannot set shard_column and shard_key")
	}
	if !mgr.IsEqualShardColumnToShardKeyColumn("user_stages") {
		t.Fatal("cannot set shard_column and shard_key")
	}
	conn, err := mgr.ConnectionByTableName("users")
	checkErr(t, err)
	if !conn.IsEqualShardColumnToShardKeyColumn() {
		t.Fatal("cannot set shard_column and shard_key")
	}
}

func TestShardConnectionByID(t *testing.T) {
	mgr, err := NewConnectionManager()
	checkErr(t, err)
	defer mgr.Close()
	conn, err := mgr.ConnectionByTableName("users")
	checkErr(t, err)
	{
		shardConn, err := conn.ShardConnectionByID(1)
		checkErr(t, err)
		if shardConn.ShardName != "user_shard_2" {
			t.Fatal("invalid shard connection by id")
		}
	}
	{
		shardConn, err := conn.ShardConnectionByID(2)
		checkErr(t, err)
		if shardConn.ShardName != "user_shard_1" {
			t.Fatal("invalid shard connection by id")
		}
	}
}

func TestShardColumnName(t *testing.T) {
	mgr, err := NewConnectionManager()
	checkErr(t, err)
	defer mgr.Close()
	if mgr.ShardColumnName("users") != "id" {
		t.Fatal("cannot get shard_column name")
	}
	if mgr.ShardColumnName("user_items") != "" {
		t.Fatal("cannot get shard_column name")
	}
	if mgr.ShardColumnName("user_decks") != "id" {
		t.Fatal("cannot get shard_column name")
	}
	if mgr.ShardColumnName("user_stages") != "" {
		t.Fatal("cannot get shard_column name")
	}
}

func TestShardKeyColumnName(t *testing.T) {
	mgr, err := NewConnectionManager()
	checkErr(t, err)
	defer mgr.Close()
	if mgr.ShardKeyColumnName("users") != "id" {
		t.Fatal("cannot get shard_key name")
	}
	if mgr.ShardKeyColumnName("user_items") != "user_id" {
		t.Fatal("cannot get shard_key name")
	}
	if mgr.ShardKeyColumnName("user_decks") != "user_id" {
		t.Fatal("cannot get shard_key name")
	}
	if mgr.ShardKeyColumnName("user_stages") != "" {
		t.Fatal("cannot get shard_key name")
	}
}

func TestQuery(t *testing.T) {
	mgr, err := NewConnectionManager()
	checkErr(t, err)
	defer mgr.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Run("not sharding table", func(t *testing.T) {
		conn, err := mgr.ConnectionByTableName("user_stages")
		checkErr(t, err)
		t.Run("without context", func(t *testing.T) {
			rows, err := conn.Query(nil, "select * from user_stages")
			checkErr(t, err)
			if columns, _ := rows.Columns(); len(columns) != 0 {
				t.Fatal("unknown rows")
			}
		})
		t.Run("with context", func(t *testing.T) {
			rows, err := conn.Query(ctx, "select * from user_stages")
			checkErr(t, err)
			if columns, _ := rows.Columns(); len(columns) != 0 {
				t.Fatal("unknown rows")
			}
		})
	})
}

func TestQueryRow(t *testing.T) {
	mgr, err := NewConnectionManager()
	checkErr(t, err)
	defer mgr.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Run("not sharding table", func(t *testing.T) {
		conn, err := mgr.ConnectionByTableName("user_stages")
		checkErr(t, err)
		t.Run("without context", func(t *testing.T) {
			conn.QueryRow(nil, "select * from user_stages where user_id = 1")
		})
		t.Run("with context", func(t *testing.T) {
			conn.QueryRow(ctx, "select * from user_stages where user_id = 1")
		})
	})
}

func TestPrepare(t *testing.T) {
	mgr, err := NewConnectionManager()
	checkErr(t, err)
	defer mgr.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Run("not sharding table", func(t *testing.T) {
		conn, err := mgr.ConnectionByTableName("user_stages")
		checkErr(t, err)
		t.Run("without context", func(t *testing.T) {
			stmt, err := conn.Prepare(nil, "select * from user_stages where user_id = ?")
			checkErr(t, err)
			checkErr(t, stmt.Close())
		})
		t.Run("with context", func(t *testing.T) {
			stmt, err := conn.Prepare(ctx, "select * from user_stages where user_id = ?")
			checkErr(t, err)
			checkErr(t, stmt.Close())
		})
	})
}

func TestExec(t *testing.T) {
	mgr, err := NewConnectionManager()
	checkErr(t, err)
	defer mgr.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Run("not sharding table", func(t *testing.T) {
		conn, err := mgr.ConnectionByTableName("user_stages")
		checkErr(t, err)
		t.Run("without context", func(t *testing.T) {
			result, err := conn.Exec(nil, "update user_stages set name = 'alice' where user_id = ?")
			checkErr(t, err)
			affected, err := result.RowsAffected()
			checkErr(t, err)
			if affected != 0 {
				t.Fatal("cannot get rows affected")
			}
		})
		t.Run("with context", func(t *testing.T) {
			result, err := conn.Exec(ctx, "update user_stages set name = 'alice' where user_id = ?")
			checkErr(t, err)
			affected, err := result.RowsAffected()
			checkErr(t, err)
			if affected != 0 {
				t.Fatal("cannot get rows affected")
			}
		})
	})
}

func TestTransaction(t *testing.T) {
	mgr, err := NewConnectionManager()
	checkErr(t, err)
	defer mgr.Close()
	conn, err := mgr.ConnectionByTableName("user_stages")
	checkErr(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Run("without context", func(t *testing.T) {
		tx := conn.Begin(nil, nil)
		stmt, err := tx.Prepare(nil, conn, "select * from user_stages where id = ?")
		checkErr(t, err)
		if _, err := tx.Stmt(nil, conn, stmt); err != nil {
			t.Fatalf("%+v\n", err)
		}
		if _, err := tx.QueryRow(nil, conn, "select * from user_stages where id = 1"); err != nil {
			t.Fatalf("%+v\n", err)
		}
		if _, err := tx.Query(nil, conn, "select * from user_stages"); err != nil {
			t.Fatalf("%+v\n", err)
		}
		if _, err := tx.Exec(nil, conn, "delete from user_stages where id = 1"); err != nil {
			t.Fatalf("%+v\n", err)
		}
		checkErr(t, tx.Commit())
	})
	t.Run("with context", func(t *testing.T) {
		tx := conn.Begin(ctx, nil)
		stmt, err := tx.Prepare(ctx, conn, "select * from user_stages where id = ?")
		checkErr(t, err)
		if _, err := tx.Stmt(ctx, conn, stmt); err != nil {
			t.Fatalf("%+v\n", err)
		}
		if _, err := tx.QueryRow(ctx, conn, "select * from user_stages where id = 1"); err != nil {
			t.Fatalf("%+v\n", err)
		}
		if _, err := tx.Query(ctx, conn, "select * from user_stages"); err != nil {
			t.Fatalf("%+v\n", err)
		}
		if _, err := tx.Exec(ctx, conn, "delete from user_stages where id = 1"); err != nil {
			t.Fatalf("%+v\n", err)
		}
		checkErr(t, tx.Rollback())
	})
}
