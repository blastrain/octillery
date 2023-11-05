package plugin

import (
	"database/sql"
	"fmt"

	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"github.com/aokabi/octillery/config"
	"github.com/aokabi/octillery/connection/adapter"
	osql "github.com/aokabi/octillery/database/sql"
	osqldriver "github.com/aokabi/octillery/database/sql/driver"
	"github.com/aokabi/octillery/debug"
	"github.com/aokabi/octillery/internal"
)

// SQLiteAdapter implements DBAdapter interface.
type SQLiteAdapter struct {
}

func init() {
	pluginName := "sqlite3"
	if internal.IsLoadedPlugin(pluginName) {
		return
	}
	var driver interface{}
	driver = &sqlite3.SQLiteDriver{}
	if drv, ok := driver.(osqldriver.Driver); ok {
		osql.RegisterByOctillery(pluginName, drv)
	} else {
		// In this case, sqlite3 package already call `sql.Register("sqlite3", &SQLiteDriver{})`.
		// So, octillery skip driver registration
	}
	adapter.Register(pluginName, &SQLiteAdapter{})
	internal.SetLoadedPlugin(pluginName)
}

// CurrentSequenceID get current unique id for all shards by sequencer
func (adapter *SQLiteAdapter) CurrentSequenceID(conn *sql.DB, tableName string) (int64, error) {
	var seqID int64
	// ignore error of ErrNoRows
	conn.QueryRow(fmt.Sprintf("select seq_id from %s where id = 0", tableName)).Scan(&seqID)
	return seqID, nil
}

// NextSequenceID get next unique id for all shards by sequencer
func (adapter *SQLiteAdapter) NextSequenceID(conn *sql.DB, tableName string) (int64, error) {
	var seqID int64
	if _, err := conn.Exec(fmt.Sprintf("update %s set seq_id = seq_id + 1 where id = 0", tableName)); err != nil {
		return 0, errors.Wrap(err, "cannot update seq_id")
	}
	if err := conn.QueryRow(fmt.Sprintf("select seq_id from %s where id = 0", tableName)).Scan(&seqID); err != nil {
		return 0, errors.Wrap(err, "cannot select seq_id")
	}
	return seqID, nil
}

// ExecDDL do nothing
func (adapter *SQLiteAdapter) ExecDDL(config *config.DatabaseConfig) error {
	return nil
}

// OpenConnection open connection by database configuration file
func (adapter *SQLiteAdapter) OpenConnection(config *config.DatabaseConfig, queryValues string) (*sql.DB, error) {
	filePath := config.NameOrPath
	debug.Printf("open connection %s", filePath)
	conn, err := sql.Open(config.Adapter, filePath)
	return conn, errors.Wrapf(err, "cannot open connection from %s", filePath)
}

// CreateSequencerTableIfNotExists create table for sequencer if not exists
func (adapter *SQLiteAdapter) CreateSequencerTableIfNotExists(conn *sql.DB, tableName string) error {
	_, err := conn.Exec(fmt.Sprintf("create table if not exists %s (id integer not null primary key autoincrement, seq_id integer not null)", tableName))
	return errors.Wrap(err, "cannot create table for sequencer")
}

// InsertRowToSequencerIfNotExists insert first row to sequencer if not exists
func (adapter *SQLiteAdapter) InsertRowToSequencerIfNotExists(conn *sql.DB, tableName string) error {
	_, err := conn.Exec(fmt.Sprintf("insert into %s(id, seq_id) values (0, 1)", tableName))
	return errors.Wrap(err, "cannot insert new row for sequncer")
}
