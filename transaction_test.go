package octillery

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/pkg/errors"
	"go.knocknote.io/octillery/database/sql"
	"go.knocknote.io/octillery/path"
)

func init() {
}

func insertToUsers(tx *sql.Tx, t *testing.T) int64 {
	result, err := tx.Exec("INSERT INTO users(id, name, age) VALUES (null, 'alice', 5)")
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	return id
}

func insertToUserItems(tx *sql.Tx, t *testing.T) int64 {
	result, err := tx.Exec("INSERT INTO user_items(id, user_id) VALUES (null, 10)")
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	return id
}

func insertToUserDecks(tx *sql.Tx, t *testing.T) int64 {
	result, err := tx.Exec("INSERT INTO user_decks(id, user_id) values (null, 10)")
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	return id
}

func insertToUserStages(tx *sql.Tx, t *testing.T) int64 {
	result, err := tx.Exec("INSERT INTO user_stages(user_id, name, age) values (10, 'bob', 10)")
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	return id
}

func initializeTables(t *testing.T) {
	if err := LoadConfig(filepath.Join(path.ThisDirPath(), "test_databases.yml")); err != nil {
		t.Fatalf("%+v\n", err)
	}
	db, err := sql.Open("", "")
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	for _, tableName := range []string{"users", "user_items", "user_decks", "user_stages"} {
		if _, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)); err != nil {
			t.Fatalf("%+v\n", err)
		}
	}
	if _, err := db.Exec(`
CREATE TABLE IF NOT EXISTS users(
    id integer NOT NULL PRIMARY KEY,
    name varchar(255) NOT NULL,
    age integer NOT NULL
)`); err != nil {
		t.Fatalf("%+v\n", err)
	}
	if _, err := db.Exec(`
CREATE TABLE IF NOT EXISTS user_items(
    id integer NOT NULL PRIMARY KEY autoincrement,
    user_id integer NOT NULL
)`); err != nil {
		t.Fatalf("%+v\n", err)
	}
	if _, err := db.Exec(`
CREATE TABLE IF NOT EXISTS user_decks(
    id integer NOT NULL PRIMARY KEY autoincrement,
    user_id integer NOT NULL
)`); err != nil {
		t.Fatalf("%+v\n", err)
	}
	if _, err := db.Exec(`
CREATE TABLE IF NOT EXISTS user_stages(
    id integer NOT NULL PRIMARY KEY autoincrement,
    user_id integer NOT NULL,
    name varchar(255) NOT NULL,
    age integer NOT NULL
)`); err != nil {
		t.Fatalf("%+v\n", err)
	}
}

func insertRecords(tx *sql.Tx, t *testing.T) {
	insertToUsers(tx, t)
	insertToUserItems(tx, t)
	insertToUserDecks(tx, t)
	insertToUserStages(tx, t)
}

func TestDistributedTransaction(t *testing.T) {
	initializeTables(t)
	db, err := sql.Open("", "")
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	insertRecords(tx, t)
	BeforeCommitCallback(func(tx *sql.Tx, writeQueries []*sql.QueryLog) error {
		if len(writeQueries) != 4 {
			t.Fatal("cannot capture write queries")
		}
		return nil
	})
	AfterCommitCallback(func(*sql.Tx) error {
		return nil
	}, func(tx *sql.Tx, isCriticalError bool, failureQueries []*sql.QueryLog) error {
		t.Fatal("cannot commit")
		return nil
	})
	if err := tx.Commit(); err != nil {
		t.Fatalf("%+v\n", err)
	}
}

func TestDistributedTransactionNormalError(t *testing.T) {
	initializeTables(t)
	db, err := sql.Open("", "")
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	id := insertToUsers(tx, t)
	insertToUserItems(tx, t)
	insertToUserDecks(tx, t)
	insertToUserStages(tx, t)
	BeforeCommitCallback(func(tx *sql.Tx, writeQueries []*sql.QueryLog) error {
		if len(writeQueries) != 4 {
			t.Fatal("cannot capture write queries")
		}
		return nil
	})
	AfterCommitCallback(func(*sql.Tx) error {
		t.Fatal("cannot handle error")
		return nil
	}, func(tx *sql.Tx, isCriticalError bool, failureQueries []*sql.QueryLog) error {
		if isCriticalError {
			t.Fatal("cannot handle critical error")
		}
		if len(failureQueries) != 1 {
			t.Fatal("cannot capture failure query")
		}
		if failureQueries[0].Query != fmt.Sprintf("insert into users(id, name, age) values (%d, 'alice', 5)", id) {
			t.Fatal("cannot capture failure query")
		}
		if failureQueries[0].LastInsertID != id {
			t.Fatal("cannot capture failure query")
		}
		return nil
	})
	os.Remove("/tmp/user_shard_1.bin-journal")
	os.Remove("/tmp/user_shard_2.bin-journal")
	// Fail first commit to users table, in this case critical error will not occur.
	if err := tx.Commit(); err == nil {
		t.Fatal("cannot handle error")
	} else {
		tx.Rollback()
		log.Println(err)
	}
}

func TestDistributedTransactionCriticalError(t *testing.T) {
	initializeTables(t)
	db, err := sql.Open("", "")
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	insertRecords(tx, t)
	BeforeCommitCallback(func(tx *sql.Tx, writeQueries []*sql.QueryLog) error {
		if len(writeQueries) != 4 {
			t.Fatal("cannot capture write queries")
		}
		return nil
	})
	AfterCommitCallback(func(*sql.Tx) error {
		t.Fatal("cannot handle error")
		return nil
	}, func(tx *sql.Tx, isCriticalError bool, failureQueries []*sql.QueryLog) error {
		if !isCriticalError {
			t.Fatal("cannot handle critical error")
		}
		if len(failureQueries) != 1 {
			t.Fatal("cannot capture failure query")
		}
		if failureQueries[0].Query != "INSERT INTO user_stages(user_id, name, age) values (10, 'bob', 10)" {
			t.Fatal("cannot capture failure query")
		}
		if failureQueries[0].LastInsertID != 1 {
			t.Fatal("cannot capture failure query")
		}
		failureQuery := failureQueries[0]
		initializeTables(t)
		// recovery from critical error
		newTx, err := db.Begin()
		if err != nil {
			t.Fatalf("%+v\n", err)
		}
		result, err := newTx.ExecWithQueryLog(failureQuery)
		if err != nil {
			t.Fatalf("%+v\n", err)
		}
		lastInsertID, err := result.LastInsertId()
		if err != nil {
			t.Fatalf("%+v\n", err)
		}
		if lastInsertID != 1 {
			t.Fatal("cannot recovery query")
		}
		if err := newTx.Rollback(); err != nil {
			t.Fatalf("%+v\n", err)
		}
		return nil
	})
	if err := os.Remove("/tmp/user_stage.bin"); err != nil {
		t.Fatalf("%+v\n", err)
	}
	if err := os.Remove("/tmp/user_stage.bin-journal"); err != nil {
		t.Fatalf("%+v\n", err)
	}
	if err := tx.Commit(); err == nil {
		t.Fatal("cannot handle error")
	} else {
		tx.Rollback()
		log.Println(err)
	}
}

func TestCommitErrorByAfterCommitCallback(t *testing.T) {
	db, err := sql.Open("", "")
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	insertRecords(tx, t)
	BeforeCommitCallback(func(tx *sql.Tx, writeQueries []*sql.QueryLog) error {
		return nil
	})
	AfterCommitCallback(func(*sql.Tx) error {
		return errors.New("after commit error")
	}, func(tx *sql.Tx, isCriticalError bool, failureQueries []*sql.QueryLog) error {
		return errors.New("after commit error")
	})
	if err := tx.Commit(); err == nil {
		t.Fatal("cannot handle error")
	} else {
		tx.Rollback()
		log.Println(err)
	}
}

func TestCommitCallbackForTx(t *testing.T) {
	db, err := sql.Open("", "")
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	insertRecords(tx, t)
	isInvokedBeforeCommitCallback := false
	tx.BeforeCommitCallback(func(writeQueries []*sql.QueryLog) error {
		isInvokedBeforeCommitCallback = true
		return nil
	})
	isInvokedAfterCommitCallback := false
	tx.AfterCommitCallback(func() error {
		isInvokedAfterCommitCallback = true
		return nil
	}, func(isCriticalError bool, failureQueries []*sql.QueryLog) error {
		return nil
	})
	checkErr(t, tx.Commit())
	if !isInvokedBeforeCommitCallback {
		t.Fatal("cannot invoke callback for before commit")
	}
	if !isInvokedAfterCommitCallback {
		t.Fatal("cannot invoke callback for after commit")
	}
}

func testIsAlreadyCommittedQueryLog(t *testing.T, queryLog *sql.QueryLog) {
	initializeTables(t)
	db, err := sql.Open("", "")
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	insertRecords(tx, t)
	{
		isCommitted, err := tx.IsAlreadyCommittedQueryLog(queryLog)
		checkErr(t, err)
		if isCommitted {
			t.Fatal("cannot work IsAlreadyCommittedQueryLog")
		}
	}
	if _, err := tx.ExecWithQueryLog(queryLog); err != nil {
		t.Fatal("cannot work ExecWithQueryLog")
	}
	{
		isCommitted, err := tx.IsAlreadyCommittedQueryLog(queryLog)
		checkErr(t, err)
		if !isCommitted {
			t.Fatal("cannot work IsAlreadyCommittedQueryLog")
		}
	}
	checkErr(t, tx.Rollback())
}

func TestIsAlreadyCommittedQueryLogErrorCase(t *testing.T) {
	db, err := sql.Open("", "")
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	if _, err := tx.IsAlreadyCommittedQueryLog(&sql.QueryLog{
		Query: "SELECT * FROM user_stages",
	}); err == nil {
		t.Fatal("cannot handle error")
	} else {
		log.Println(err)
	}
	checkErr(t, tx.Rollback())
}

func TestIsAlreadyCommittedDeleteQueryLog(t *testing.T) {
	testIsAlreadyCommittedQueryLog(t, &sql.QueryLog{
		Query: "DELETE from user_stages WHERE id = ? AND user_id = ?",
		Args:  []interface{}{1, 10},
	})
}

func TestIsAlreadyCommittedInsertQueryLog(t *testing.T) {
	testIsAlreadyCommittedQueryLog(t, &sql.QueryLog{
		Query:        "INSERT INTO user_stages(user_id, name, age) VALUES (10, ?, ?)",
		Args:         []interface{}{"alice", 5},
		LastInsertID: 2,
	})
}

func TestIsAlreadyCommittedUpdateQueryLog(t *testing.T) {
	testIsAlreadyCommittedQueryLog(t, &sql.QueryLog{
		Query: "UPDATE user_stages set name = ?, age = 5 where user_id = ?",
		Args:  []interface{}{"alice", 10},
	})
}
