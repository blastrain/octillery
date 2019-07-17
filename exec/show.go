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

	for _, shardConn := range e.conn.ShardConnections.AllShard() {
		rows, err := e.execQuery(shardConn, query.Text, query.Args...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return []*sql.Rows{rows}, nil
	}

	return nil, nil
}

// QueryRow show row from any one of shards.
func (e *ShowQueryExecutor) QueryRow() (*sql.Row, error) {
	query, ok := e.query.(*sqlparser.QueryBase)
	if !ok {
		return nil, errors.New("cannot convert to sqlparser.Query to *sqlparser.QueryBase")
	}

	for _, shardConn := range e.conn.ShardConnections.AllShard() {
		row, err := e.execQueryRow(shardConn, query.Text, query.Args...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return row, nil
	}

	return nil, nil
}

// Exec doesn't support in ShowQueryExecutor, returns always error.
func (e *ShowQueryExecutor) Exec() (sql.Result, error) {
	return nil, errors.New("ShowQueryExecutor cannot invoke Exec()")
}
