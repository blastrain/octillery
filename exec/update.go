package exec

import (
	"database/sql"

	"github.com/pkg/errors"
	"go.knocknote.io/octillery/debug"
	"go.knocknote.io/octillery/sqlparser"
)

// UpdateQueryExecutor inherits QueryExecutorBase structure
type UpdateQueryExecutor struct {
	*QueryExecutorBase
}

// NewUpdateQueryExecutor creates instance of UpdateQueryExecutor
func NewUpdateQueryExecutor(base *QueryExecutorBase) *UpdateQueryExecutor {
	return &UpdateQueryExecutor{base}
}

// Query doesn't support in UpdateQueryExecutor, returns always error.
func (e *UpdateQueryExecutor) Query() ([]*sql.Rows, error) {
	return nil, errors.New("UpdateQueryExecutor cannot invoke Query()")
}

// QueryRow doesn't support in UpdateQueryExecutor, returns always error.
func (e *UpdateQueryExecutor) QueryRow() (*sql.Row, error) {
	return nil, errors.New("UpdateQueryExecutor cannot invoke QueryRow()")
}

// Exec executes UPDATE query for shards.
func (e *UpdateQueryExecutor) Exec() (sql.Result, error) {
	query, ok := e.query.(*sqlparser.QueryBase)
	if !ok {
		return nil, errors.New("cannot convert sqlparser.Query to *sqlparser.QueryBase")
	}
	if e.conn.IsUsedSequencer && e.conn.Sequencer == nil {
		return nil, errors.New("cannot update row. sequencer's connection is nil")
	}
	if query.IsNotFoundShardKeyID() {
		return nil, errors.New("cannot update row. not found shard_key column in this query")
	}
	shardConn, err := e.conn.ShardConnectionByID(int64(query.ShardKeyID))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	debug.Printf("(DB:%s):%s", shardConn.ShardName, query.Text)
	result, err := e.exec(shardConn.Connection, query.Text, query.Args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return result.(sql.Result), nil
}
