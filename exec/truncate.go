package exec

import (
	"database/sql"

	"github.com/pkg/errors"
	"github.com/aokabi/octillery/debug"
	"github.com/aokabi/octillery/sqlparser"
)

// TruncateQueryExecutor inherits QueryExecutorBase structure
type TruncateQueryExecutor struct {
	*QueryExecutorBase
}

// NewTruncateQueryExecutor creates instance of TruncateQueryExecutor
func NewTruncateQueryExecutor(base *QueryExecutorBase) *TruncateQueryExecutor {
	return &TruncateQueryExecutor{base}
}

// Query doesn't support in TruncateQueryExecutor, returns always error.
func (e *TruncateQueryExecutor) Query() ([]*sql.Rows, error) {
	return nil, errors.New("TruncateQueryExecutor cannot invoke Query()")
}

// QueryRow doesn't support in TruncateQueryExecutor, returns always error.
func (e *TruncateQueryExecutor) QueryRow() (*sql.Row, error) {
	return nil, errors.New("TruncateQueryExecutor cannot invoke QueryRow()")
}

// Exec executes `TRUNCATE TABLE` DDL for shards.
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
