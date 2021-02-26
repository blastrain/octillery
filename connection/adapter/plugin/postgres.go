package plugin

import (
	"database/sql"
	"fmt"
	"strings"

	postgres "github.com/lib/pq"
	"github.com/pkg/errors"
	"go.knocknote.io/octillery/config"
	"go.knocknote.io/octillery/connection/adapter"
	osql "go.knocknote.io/octillery/database/sql"
	osqldriver "go.knocknote.io/octillery/database/sql/driver"
	"go.knocknote.io/octillery/debug"
	"go.knocknote.io/octillery/internal"
)

// PostgreSQLAdapter implements DBAdapter interface.
type PostgreSQLAdapter struct {
}

const (
	pluginName = "postgres"
)

func init() {
	if internal.IsLoadedPlugin(pluginName) {
		return
	}
	var driver interface{}
	driver = postgres.Driver{}
	if drv, ok := driver.(osqldriver.Driver); ok {
		// postgres package's import statement is already replaced to "go.knocknote.io/octillery/database/sql"
		osql.RegisterByOctillery(pluginName, drv)
	} else {
		// In this case, postgres package already call `sql.Register("postgres", &Driver{})`.
		// So, octillery skip driver registration
	}
	adapter.Register(pluginName, &PostgreSQLAdapter{})
	internal.SetLoadedPlugin(pluginName)
}

// CurrentSequenceID get current unique id for all shards by sequencer
func (adapter *PostgreSQLAdapter) CurrentSequenceID(conn *sql.DB, tableName string) (int64, error) {
	var seqID int64
	if err := conn.QueryRow(fmt.Sprintf("select last_value from %s", sequenceName(tableName))).Scan(&seqID); err != nil {
		return 0, errors.Wrap(err, "cannot select last_value")
	}
	return seqID, nil
}

// NextSequenceID get next unique id for all shards by sequencer
func (adapter *PostgreSQLAdapter) NextSequenceID(conn *sql.DB, tableName string) (int64, error) {
	var seqID int64
	if err := conn.QueryRow(fmt.Sprintf("select nextval('%s')", sequenceName(tableName))).Scan(&seqID); err != nil {
		return 0, errors.Wrapf(err, "cannot select nextval('%s')", sequenceName(tableName))
	}
	return seqID, nil
}

// ExecDDL do nothing
func (adapter *PostgreSQLAdapter) ExecDDL(config *config.DatabaseConfig) error {
	return nil
}

// OpenConnection open connection by database configuration file
func (adapter *PostgreSQLAdapter) OpenConnection(config *config.DatabaseConfig, queryString string) (*sql.DB, error) {
	if len(config.Masters) > 1 {
		return nil, errors.New("Sorry, currently supports single master database only")
	}
	dbname := config.NameOrPath
	for _, master := range config.Masters {
		dsn := fmt.Sprintf("%s://%s:%s@%s/%s?sslmode=disable&%s", pluginName, config.Username, config.Password, master, dbname, queryString)
		debug.Printf("dsn = %s", strings.Replace(dsn, "%", "%%", -1))
		conn, err := sql.Open(config.Adapter, dsn)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return conn, nil
	}
	for _, slave := range config.Slaves {
		dsn := fmt.Sprintf("%s://%s:%s@%s/%s?sslmode=disable&%s", pluginName, config.Username, config.Password, slave, dbname, queryString)
		debug.Printf("TODO: not support slave. dsn = %s", dsn)
		break
	}
	for _, backup := range config.Backups {
		dsn := fmt.Sprintf("%s://%s:%s@%s/%s?sslmode=disable&%s", pluginName, config.Username, config.Password, backup, dbname, queryString)
		debug.Printf("TODO: not support backup. dsn = %s", dsn)
	}
	return nil, errors.New("must define 'master' server")
}

// CreateSequencerTableIfNotExists create table for sequencer if not exists
func (adapter *PostgreSQLAdapter) CreateSequencerTableIfNotExists(conn *sql.DB, tableName string) error {
	_, err := conn.Exec(fmt.Sprintf(`CREATE SEQUENCE IF NOT EXISTS %s AS integer;`, sequenceName(tableName)))
	return errors.Wrap(err, "cannot create table for sequencer")
}

// InsertRowToSequencerIfNotExists do nothing
func (adapter *PostgreSQLAdapter) InsertRowToSequencerIfNotExists(conn *sql.DB, tableName string) error {
	return nil
}

func sequenceName(tableName string) string {
	return fmt.Sprintf("%s_id_seq", tableName)
}
