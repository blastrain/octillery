package exec

import (
	"database/sql"

	"github.com/pkg/errors"
	"go.knocknote.io/octillery/debug"
	"go.knocknote.io/octillery/sqlparser"
)

// InsertQueryExecutor inherits QueryExecutorBase structure
type InsertQueryExecutor struct {
	*QueryExecutorBase
}

// NewInsertQueryExecutor creates instance of InsertQueryExecutor
func NewInsertQueryExecutor(base *QueryExecutorBase) *InsertQueryExecutor {
	return &InsertQueryExecutor{base}
}

// Query doesn't support in InsertQueryExecutor, returns always error.
func (e *InsertQueryExecutor) Query() ([]*sql.Rows, error) {
	return nil, errors.New("InsertQueryExecutor cannot invoke Query()")
}

// QueryRow doesn't support in InsertQueryExecutor, returns always error.
func (e *InsertQueryExecutor) QueryRow() (*sql.Row, error) {
	return nil, errors.New("InsertQueryExecutor cannot invoke QueryRow()")
}

func (e *InsertQueryExecutor) nextSequenceID(query *sqlparser.InsertQuery) (int64, error) {
	if !e.conn.IsUsedSequencer {
		return 0, nil
	}
	nextSequenceID, err := e.conn.NextSequenceID(query.TableName)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	debug.Printf("NEXT ID = %d", nextSequenceID)
	return nextSequenceID, nil
}

// Exec executes INSERT query for shards.
func (e *InsertQueryExecutor) Exec() (sql.Result, error) {
	query, ok := e.query.(*sqlparser.InsertQuery)
	if !ok {
		return nil, errors.New("cannot convert to sqlparser.Query to sqlparser.InsertQuery")
	}

	if e.conn.IsUsedSequencer && e.conn.Sequencer == nil {
		return nil, errors.New("cannot insert row. sequencer's connection is nil")
	}
	if e.conn.ShardConnections.ShardNum() == 0 {
		return nil, errors.New("cannot insert row. shard connections is nil")
	}

	nextSequenceID, err := e.nextSequenceID(query)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	query.SetNextSequenceID(nextSequenceID)
	shardKeyID := query.ShardKeyID
	if e.conn.IsEqualShardColumnToShardKeyColumn() {
		shardKeyID = sqlparser.Identifier(nextSequenceID)
	}
	if shardKeyID == sqlparser.UnknownID {
		return nil, errors.New("shard_key id is not found")
	}
	shardConn, err := e.conn.ShardConnectionByID(int64(shardKeyID))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	debug.Printf("(DB:%s):%s", shardConn.ShardName, query.String())
	result, err := e.exec(shardConn, query.String())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if e.conn.IsUsedSequencer {
		return &mergedResult{affectedRows: 1, lastInsertedID: nextSequenceID}, nil
	}
	return result.(sql.Result), nil
}
