package plugin

import (
	"database/sql"
	"fmt"
	"strings"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"go.knocknote.io/octillery/config"
	"go.knocknote.io/octillery/connection/adapter"
	osql "go.knocknote.io/octillery/database/sql"
	osqldriver "go.knocknote.io/octillery/database/sql/driver"
	"go.knocknote.io/octillery/debug"
	"go.knocknote.io/octillery/internal"
)

// MySQLAdapter implements DBAdapter interface.
type MySQLAdapter struct {
}

func init() {
	pluginName := "mysql"
	if internal.IsLoadedPlugin(pluginName) {
		return
	}
	var driver interface{}
	driver = mysql.MySQLDriver{}
	if drv, ok := driver.(osqldriver.Driver); ok {
		// mysql package's import statement is already replaced to "go.knocknote.io/octillery/database/sql"
		osql.RegisterByOctillery(pluginName, drv)
	} else {
		// In this case, mysql package already call `sql.Register("mysql", &MySQLDriver{})`.
		// So, octillery skip driver registration
	}
	adapter.Register(pluginName, &MySQLAdapter{})
	internal.SetLoadedPlugin(pluginName)
}

// CurrentSequenceID get current unique id for all shards by sequencer
func (adapter *MySQLAdapter) CurrentSequenceID(conn *sql.DB, tableName string) (int64, error) {
	var seqID int64
	if _, err := conn.Exec(fmt.Sprintf("update %s set id = last_insert_id(id)", tableName)); err != nil {
		return 0, errors.Wrap(err, "cannot update id by last_insert_id(id)")
	}
	if err := conn.QueryRow("select last_insert_id()").Scan(&seqID); err != nil {
		return 0, errors.Wrap(err, "cannot select last_insert_id()")
	}
	return seqID, nil
}

// NextSequenceID get next unique id for all shards by sequencer
func (adapter *MySQLAdapter) NextSequenceID(conn *sql.DB, tableName string) (int64, error) {
	var seqID int64
	if _, err := conn.Exec(fmt.Sprintf("update %s set id = last_insert_id(id + 1)", tableName)); err != nil {
		return 0, errors.Wrap(err, "cannot update id for last_insert_id(id + 1)")
	}
	if err := conn.QueryRow("select last_insert_id()").Scan(&seqID); err != nil {
		return 0, errors.Wrap(err, "cannot select last_insert_id()")
	}
	return seqID, nil
}

// ExecDDL create database if not exists by database configuration file.
func (adapter *MySQLAdapter) ExecDDL(config *config.DatabaseConfig) error {
	if len(config.Mains) > 1 {
		return errors.New("Sorry, currently supports single main database only")
	}
	dbname := config.NameOrPath
	for _, main := range config.Mains {
		serverDsn := fmt.Sprintf("%s:%s@tcp(%s)/", config.Username, config.Password, main)
		serverConn, err := sql.Open(config.Adapter, serverDsn)
		defer serverConn.Close()
		if err != nil {
			return errors.Wrapf(err, "cannot open connection from %s", serverDsn)
		}
		if _, err := serverConn.Exec(fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s`, dbname)); err != nil {
			return errors.Wrapf(err, "cannot create database %s", dbname)
		}
		return nil
	}
	return errors.New("must define 'main' server")
}

// OpenConnection open connection by database configuration file
func (adapter *MySQLAdapter) OpenConnection(config *config.DatabaseConfig, queryString string) (*sql.DB, error) {
	if len(config.Mains) > 1 {
		return nil, errors.New("Sorry, currently supports single main database only")
	}
	dbname := config.NameOrPath
	for _, main := range config.Mains {
		dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?%s", config.Username, config.Password, main, dbname, queryString)
		debug.Printf("dsn = %s", strings.Replace(dsn, "%", "%%", -1))
		conn, err := sql.Open(config.Adapter, dsn)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return conn, nil
	}
	for _, subordinate := range config.Subordinates {
		dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?%s", config.Username, config.Password, subordinate, dbname, queryString)
		debug.Printf("TODO: not support subordinate. dsn = %s", dsn)
		break
	}

	for _, backup := range config.Backups {
		dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?%s", config.Username, config.Password, backup, dbname, queryString)
		debug.Printf("TODO: not support backup. dsn = %s", dsn)
	}
	return nil, errors.New("must define 'main' server")
}

// CreateSequencerTableIfNotExists create table for sequencer if not exists
func (adapter *MySQLAdapter) CreateSequencerTableIfNotExists(conn *sql.DB, tableName string) error {
	_, err := conn.Exec(fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    id integer NOT NULL PRIMARY KEY AUTO_INCREMENT
)`, tableName))
	return errors.Wrap(err, "cannot create table for sequencer")
}

// InsertRowToSequencerIfNotExists insert first row to sequencer if not exists
func (adapter *MySQLAdapter) InsertRowToSequencerIfNotExists(conn *sql.DB, tableName string) error {
	var rowCount uint64
	if err := conn.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&rowCount); err != nil {
		return errors.Wrapf(err, "cannot SELECT COUNT(*) FROM %s", tableName)
	}
	// ignore if already inserted row (perhaps id is 0)
	if rowCount > 0 {
		return nil
	}
	// insert id is 0, but inserted row's id is 1 because this table enabled AUTO_INCREMENT
	if _, err := conn.Exec(fmt.Sprintf("INSERT INTO %s(id) VALUES (0)", tableName)); err != nil {
		return errors.Wrap(err, "cannot insert new row to sequencer")
	}
	// force update first row's id to 0 because last_insert_id() returns 2 at first insert
	if _, err := conn.Exec(fmt.Sprintf("UPDATE %s SET id = 0", tableName)); err != nil {
		return errors.Wrap(err, "cannot update new row's id to sequencer")
	}
	return nil
}
