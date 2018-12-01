// Package octillery is a Go package for sharding databases.
//
// It can use with every OR Mapping library (xorm, gorp, gorm, dbr...) implementing database/sql interface, or raw SQL.
package octillery

import (
	"database/sql"
	"os"
	"strconv"

	"github.com/pkg/errors"
	"go.knocknote.io/octillery/config"
	"go.knocknote.io/octillery/connection"
	osql "go.knocknote.io/octillery/database/sql"
	"go.knocknote.io/octillery/debug"
	"go.knocknote.io/octillery/exec"
	_ "go.knocknote.io/octillery/plugin" // load database adapter plugin
	"go.knocknote.io/octillery/sqlparser"
)

// Version is the variable for versioning Octillery
const Version = "v1.0.0"

// LoadConfig load your database configuration file.
//
// If use with debug mode, set environment variable  ( `OCTILLERY_DEBUG=1` ) before call this method.
//
// Loaded configuration instance is set to internal global variable, therefore you can use only single configuration file at each application.
//
// Configuration format see go.knocknote.io/octillery/config
func LoadConfig(configPath string) error {
	isDebug, _ := strconv.ParseBool(os.Getenv("OCTILLERY_DEBUG"))
	debug.SetDebug(isDebug)
	cfg, err := config.Load(configPath)
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(connection.SetConfig(cfg))
}

// Exec invoke sql.Query or sql.Exec by query type.
//
// There is no need to worry about whether target databases are sharded or not.
func Exec(db *osql.DB, queryText string) ([]*sql.Rows, sql.Result, error) {
	connMgr := db.ConnectionManager()
	parser, err := sqlparser.New()
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	query, err := parser.Parse(queryText)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	conn, err := connMgr.ConnectionByTableName(query.Table())
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	if query.QueryType() == sqlparser.Select {
		if conn.IsShard {
			rows, err := exec.NewQueryExecutor(nil, conn, nil, query).Query()
			return rows, nil, errors.WithStack(err)
		}
		rows, err := conn.Connection.Query(queryText)
		return []*sql.Rows{rows}, nil, errors.WithStack(err)
	}

	if conn.IsShard {
		result, err := exec.NewQueryExecutor(nil, conn, nil, query).Exec()
		return nil, result, errors.WithStack(err)
	}
	result, err := conn.Connection.Exec(queryText)
	return nil, result, errors.WithStack(err)
}

func BeforeCommitCallback(callback func(*connection.TxConnection, []*connection.QueryLog) error) {
	connection.SetBeforeCommitCallback(callback)
}

func AfterCommitCallback(
	successCallback func(*connection.TxConnection),
	failureCallback func(*connection.TxConnection, bool, []*connection.QueryLog)) {
	connection.SetAfterCommitCallback(successCallback, failureCallback)
}
