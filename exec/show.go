package exec

import (
	"database/sql"
	"strings"

	"github.com/pkg/errors"
	"go.knocknote.io/octillery/debug"
	"go.knocknote.io/octillery/sqlparser"
)

// ShowQueryExecutor inherits QueryExecutorBase structure
type ShowQueryExecutor struct {
	*QueryExecutorBase
}

// NewShowQueryExecutor creates instance of ShowQueryExecutor
func NewShowQueryExecutor(base *QueryExecutorBase) *ShowQueryExecutor {
	return &ShowQueryExecutor{base}
}

// Query doesn't support in ShowQueryExecutor, returns always error.
func (e *ShowQueryExecutor) Query() ([]*sql.Rows, error) {
	return nil, errors.New("ShowQueryExecutor cannot invoke Query()")
}

// QueryRow doesn't support in ShowQueryExecutor, returns always error.
func (e *ShowQueryExecutor) QueryRow() (*sql.Row, error) {
	return nil, errors.New("ShowQueryExecutor cannot invoke QueryRow()")
}

// Exec executes `SHOW TABLE` DDL for shards
func (e *ShowQueryExecutor) Exec() (sql.Result, error) {
	debug.Printf("show table for shards")
	query, ok := e.query.(*sqlparser.QueryBase)
	if !ok {
		return nil, errors.New("cannot convert sqlparser.Query to *sqlparser.QueryBase")
	}
	var totalAffectedRows int64
	errs := []string{}
	for _, shardConn := range e.conn.ShardConnections.AllShard() {
		result, err := shardConn.Connection.Exec(query.Text, query.Args...)
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}
		if result != nil {
			affectedRows, err := result.(sql.Result).RowsAffected()
			if err != nil {
				errs = append(errs, err.Error())
			}
			totalAffectedRows = totalAffectedRows + affectedRows
		}
	}
	if len(errs) > 0 {
		return nil, errors.New(strings.Join(errs, ":"))
	}
	debug.Printf("totalAffectedRows = %d", totalAffectedRows)
	return &mergedResult{affectedRows: totalAffectedRows}, nil
}
