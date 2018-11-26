package exec

import (
	"database/sql"
	"strings"

	"github.com/pkg/errors"
	"go.knocknote.io/octillery/debug"
	"go.knocknote.io/octillery/sqlparser"
)

type DropQueryExecutor struct {
	*QueryExecutorBase
}

func NewDropQueryExecutor(base *QueryExecutorBase) *DropQueryExecutor {
	return &DropQueryExecutor{base}
}

func (e *DropQueryExecutor) Query() ([]*sql.Rows, error) {
	return nil, errors.New("DropQueryExecutor cannot invoke Query()")
}

func (e *DropQueryExecutor) QueryRow() (*sql.Row, error) {
	return nil, errors.New("DropQueryExecutor cannot invoke QueryRow()")
}

func (e *DropQueryExecutor) Exec() (sql.Result, error) {
	debug.Printf("drop table for shards")
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
