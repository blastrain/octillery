package exec

import (
	"database/sql"

	"github.com/pkg/errors"
	"go.knocknote.io/octillery/debug"
	"go.knocknote.io/octillery/sqlparser"
)

type TruncateQueryExecutor struct {
	*QueryExecutorBase
}

func NewTruncateQueryExecutor(base *QueryExecutorBase) *TruncateQueryExecutor {
	return &TruncateQueryExecutor{base}
}

func (e *TruncateQueryExecutor) Query() ([]*sql.Rows, error) {
	return nil, errors.New("TruncateQueryExecutor cannot invoke Query()")
}

func (e *TruncateQueryExecutor) QueryRow() (*sql.Row, error) {
	return nil, errors.New("TruncateQueryExecutor cannot invoke QueryRow()")
}

func (e *TruncateQueryExecutor) Exec() (sql.Result, error) {
	debug.Printf("truncate table for shards")
	query, ok := e.query.(*sqlparser.QueryBase)
	if !ok {
		return nil, errors.New("cannot convert sqlparser.Query to *sqlparser.QueryBase")
	}
	for _, shardConn := range e.conn.ShardConnections.AllShard() {
		if _, err := shardConn.Connection.Exec(query.Text); err != nil {
			return nil, errors.WithStack(err)
		}
	}
	return nil, nil
}
