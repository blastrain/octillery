package octillery

import (
	"fmt"
	"log"
	"path/filepath"
	"testing"

	"go.knocknote.io/octillery/connection"
	"go.knocknote.io/octillery/database/sql"
	"go.knocknote.io/octillery/path"
)

func init() {
	BeforeCommitCallback(func(tx *connection.TxConnection, writeQueries []*connection.QueryLog) error {
		log.Println("BeforeCommit", writeQueries)
		return nil
	})
	AfterCommitCallback(func(*connection.TxConnection) {
		log.Println("AfterCommit")
	}, func(tx *connection.TxConnection, isCriticalError bool, failureQueries []*connection.QueryLog) {
		log.Println("AfterCommit", failureQueries)
	})
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

func TestDistributedTransaction(t *testing.T) {
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
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("%+v\n", err)
	}

	insertToUsers(tx, t)
	insertToUserItems(tx, t)
	insertToUserDecks(tx, t)
	insertToUserStages(tx, t)

	if err := tx.Commit(); err != nil {
		t.Fatalf("%+v\n", err)
	}
}
