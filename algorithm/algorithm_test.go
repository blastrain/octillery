package algorithm

import (
	"database/sql"
	"database/sql/driver"
	"testing"
)

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

func init() {
	sql.Register("sqlite3", &TestDriver{})
}

func TestModulo(t *testing.T) {
	conn, err := sql.Open("sqlite3", "")
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	t.Run("load as default algorithm", func(t *testing.T) {
		modulo, err := LoadShardingAlgorithm("")
		if err != nil {
			t.Fatalf("%+v\n", err)
		}
		t.Run("init", func(t *testing.T) {
			if !modulo.Init([]*sql.DB{conn}) {
				t.Fatal("cannot initialize modulo algorithm")
			}
		})
		t.Run("shard", func(t *testing.T) {
			shardConn, err := modulo.Shard([]*sql.DB{conn}, 1)
			if err != nil {
				t.Fatalf("%+v\n", err)
			}
			if conn != shardConn {
				t.Fatal("cannot get shard connection")
			}
		})
	})
	t.Run("load by name", func(t *testing.T) {
		modulo, err := LoadShardingAlgorithm("modulo")
		if err != nil {
			t.Fatalf("%+v\n", err)
		}
		t.Run("init", func(t *testing.T) {
			if !modulo.Init([]*sql.DB{conn}) {
				t.Fatal("cannot initialize algorithm")
			}
		})
		t.Run("shard", func(t *testing.T) {
			shardConn, err := modulo.Shard([]*sql.DB{conn}, 1)
			if err != nil {
				t.Fatalf("%+v\n", err)
			}
			if conn != shardConn {
				t.Fatal("cannot get shard connection")
			}
		})
	})

}

func TestHashMap(t *testing.T) {
	conn, err := sql.Open("sqlite3", "")
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	t.Run("load by name", func(t *testing.T) {
		hashmap, err := LoadShardingAlgorithm("hashmap")
		if err != nil {
			t.Fatalf("%+v\n", err)
		}
		conns := []*sql.DB{conn, conn}
		t.Run("init", func(t *testing.T) {
			if !hashmap.Init(conns) {
				t.Fatal("cannot initialize algorithm")
			}
		})
		t.Run("shard", func(t *testing.T) {
			shardConn, err := hashmap.Shard(conns, 1)
			if err != nil {
				t.Fatalf("%+v\n", err)
			}
			if conn != shardConn {
				t.Fatal("cannot get shard connection")
			}
		})
	})
}
