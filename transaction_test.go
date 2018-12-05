package octillery

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"

	"go.knocknote.io/octillery/connection"
	"go.knocknote.io/octillery/database/sql"
	"go.knocknote.io/octillery/path"
)

func init() {
}

func insertToUsers(tx *sql.Tx, t *testing.T) {
	result, err := tx.Exec("INSERT INTO users(id, name) VALUES (null, 'alice')")
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	log.Println("inserted users.id = ", id)
}

func insertToUserItems(tx *sql.Tx, t *testing.T) {
	result, err := tx.Exec("INSERT INTO user_items(id, user_id) VALUES (null, 10)")
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	log.Println("inserted user_items.id = ", id)
}

func insertToUserDecks(tx *sql.Tx, t *testing.T) {
	result, err := tx.Exec("INSERT INTO user_decks(id, user_id) values (null, 10)")
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	log.Println("inserted user_decks.id = ", id)
}

func insertToUserStages(tx *sql.Tx, t *testing.T) {
	result, err := tx.Exec("INSERT INTO user_stages(id, user_id) values (null, 10)")
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	log.Println("inserted user_stages.id = ", id)
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
    name varchar(255) NOT NULL
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
    user_id integer NOT NULL
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
	BeforeCommitCallback(func(tx *connection.TxConnection, writeQueries []*connection.QueryLog) error {
		if len(writeQueries) != 4 {
			t.Fatal("cannot capture write queries")
		}
		return nil
	})
	AfterCommitCallback(func(*connection.TxConnection) {
	}, func(tx *connection.TxConnection, isCriticalError bool, failureQueries []*connection.QueryLog) {
		t.Fatal("cannot commit")
	})
	if err := tx.Commit(); err != nil {
		t.Fatalf("%+v\n", err)
	}
}

func TestDistributedTransactionError(t *testing.T) {
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
	BeforeCommitCallback(func(tx *connection.TxConnection, writeQueries []*connection.QueryLog) error {
		if len(writeQueries) != 4 {
			t.Fatal("cannot capture write queries")
		}
		return nil
	})
	AfterCommitCallback(func(*connection.TxConnection) {
		t.Fatal("cannot handle error")
	}, func(tx *connection.TxConnection, isCriticalError bool, failureQueries []*connection.QueryLog) {
		if !isCriticalError {
			t.Fatal("cannot handle critical error")
		}
		if len(failureQueries) != 1 {
			t.Fatal("cannot capture failure query")
		}
		if failureQueries[0].Query != "INSERT INTO user_stages(id, user_id) values (null, 10)" {
			t.Fatal("cannot capture failure query")
		}
		if failureQueries[0].LastInsertID != 1 {
			t.Fatal("cannot capture failure query")
		}
	})
	if err := os.Remove("/tmp/user_stage.bin-journal"); err != nil {
		t.Fatalf("%+v\n", err)
	}
	if err := tx.Commit(); err == nil {
		t.Fatal("cannot handle error")
	} else {
		log.Println(err)
	}
}
