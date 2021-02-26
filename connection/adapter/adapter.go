package adapter

import (
	"database/sql"
	"sync"

	"github.com/pkg/errors"
	"go.knocknote.io/octillery/config"
	"go.knocknote.io/octillery/debug"
)

// DBAdapter is a adapter for common sequence each database driver.
//
// octillery currently supports mysql and postgres and sqlite3.
// If use the other new adapter, implement the following interface as plugin ( new_adapter.go ) and call adapter.Register("adapter_name", &NewAdapterStructure{}).
// Also, new_adapter.go file should put inside go.knocknote.io/octillery/plugin directory.
type DBAdapter interface {
	// get current unique id for all shards by sequencer
	CurrentSequenceID(conn *sql.DB, tableName string) (int64, error)

	// get next unique id for all shards by sequencer
	NextSequenceID(conn *sql.DB, tableName string) (int64, error)

	// create database if not exists by database configuration file.
	ExecDDL(config *config.DatabaseConfig) error

	// open connection by database configuration file
	OpenConnection(config *config.DatabaseConfig, queryValues string) (*sql.DB, error)

	// create table for sequencer if not exists
	CreateSequencerTableIfNotExists(conn *sql.DB, tableName string) error

	// insert first row to sequencer if not exists
	InsertRowToSequencerIfNotExists(conn *sql.DB, tableName string) error
}

var (
	adaptersMu sync.RWMutex
	adapters   = make(map[string]DBAdapter)
)

// Register register DBAdapter with driver name
func Register(name string, adapter DBAdapter) {
	adaptersMu.Lock()
	defer adaptersMu.Unlock()
	if adapter == nil {
		panic("Register adapter is nil")
	}
	if _, dup := adapters[name]; dup {
		debug.Printf("Register called twice for adapter %s", name)
	}
	adapters[name] = adapter
}

// Adapter get adapter by driver name
func Adapter(name string) (DBAdapter, error) {
	adapter := adapters[name]
	if adapter == nil {
		return nil, errors.Errorf("unknown adapter name %s", name)
	}
	return adapter, nil
}
