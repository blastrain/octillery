package adapter

import (
	"database/sql"
	"testing"

	"github.com/pkg/errors"
	"go.knocknote.io/octillery/config"
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

var (
	adapterInstance DBAdapter
)

func init() {
	func() {
		defer func() {
			if err := recover(); err == nil {
				panic(errors.New("cannot handle error"))
			}
		}()
		Register("sqlite3", nil)
	}()
	adapterInstance = &TestAdapter{}
	Register("sqlite3", adapterInstance)
	Register("sqlite3", adapterInstance)
}

func TestAdapterInstance(t *testing.T) {
	instance, err := Adapter("sqlite3")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	if instance == nil {
		t.Fatal("cannot get adapter instance")
	}
	if instance != adapterInstance {
		t.Fatal("cannot get adapter instance")
	}
	unknownInstance, err := Adapter("unknown")
	if err == nil {
		t.Fatalf("cannot handle error")
	}
	if unknownInstance != nil {
		t.Fatalf("invalid adapter instance")
	}
}
