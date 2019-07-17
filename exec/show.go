package exec

import (
	"database/sql"

	"github.com/pkg/errors"
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

// Query show multiple rows from any one of shards.
func (e *ShowQueryExecutor) Query() ([]*sql.Rows, error) {
	query, ok := e.query.(*sqlparser.QueryBase)
	if !ok {
		return nil, errors.New("cannot convert to sqlparser.Query to *sqlparser.QueryBase")
	}

	oneRows := make([]*sql.Rows, 1)
	for _, shardConn := range e.conn.ShardConnections.AllShard() {
		rows, err := e.execQuery(shardConn, query.Text, query.Args...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		oneRows[0] = rows
		break
	}

	return oneRows, nil
}

// QueryRow show row from any one of shards.
func (e *ShowQueryExecutor) QueryRow() (*sql.Row, error) {
	query, ok := e.query.(*sqlparser.QueryBase)
	if !ok {
		return nil, errors.New("cannot convert to sqlparser.Query to *sqlparser.QueryBase")
	}

	var row *sql.Row
	for _, shardConn := range e.conn.ShardConnections.AllShard() {
		tmpRow, err := e.execQueryRow(shardConn, query.Text, query.Args...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		row = tmpRow
		break
	}

	return row, nil
}

// Exec doesn't support in ShowQueryExecutor, returns always error.
func (e *ShowQueryExecutor) Exec() (sql.Result, error) {
	return nil, errors.New("ShowQueryExecutor cannot invoke Exec()")
}
