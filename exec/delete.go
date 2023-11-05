package exec

import (
	"database/sql"
	"strings"

	"github.com/pkg/errors"
	"github.com/aokabi/octillery/debug"
	"github.com/aokabi/octillery/sqlparser"
)

// DeleteQueryExecutor inherits QueryExecutorBase structure
type DeleteQueryExecutor struct {
	*QueryExecutorBase
}

// NewDeleteQueryExecutor creates instance of DeleteQueryExecutor
func NewDeleteQueryExecutor(base *QueryExecutorBase) *DeleteQueryExecutor {
	return &DeleteQueryExecutor{base}
}

// Query doesn't support in DeleteQueryExecutor, returns always error.
func (e *DeleteQueryExecutor) Query() ([]*sql.Rows, error) {
	return nil, errors.New("DeleteQueryExecutor cannot invoke Query()")
}

// QueryRow doesn't support in DeleteQueryExecutor, returns always error.
func (e *DeleteQueryExecutor) QueryRow() (*sql.Row, error) {
	return nil, errors.New("DeleteQueryExecutor cannot invoke QueryRow()")
}

func (e *DeleteQueryExecutor) deleteShardTable(query *sqlparser.DeleteQuery) (sql.Result, error) {
	debug.Printf("delete shard table")

	var totalAffectedRows int64
	errs := []string{}
	for _, shardConn := range e.conn.ShardConnections.AllShard() {
		debug.Printf("(DB:%s):%s", shardConn.ShardName, query.Text)
		result, err := e.exec(shardConn, query.Text, query.Args...)
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}
		affectedRows, err := result.(sql.Result).RowsAffected()
		if err != nil {
			errs = append(errs, err.Error())
		}
		totalAffectedRows = totalAffectedRows + affectedRows
	}

	if len(errs) > 0 {
		return nil, errors.New(strings.Join(errs, ":"))
	}

	debug.Printf("totalAffectedRows = %d", totalAffectedRows)
	return &mergedResult{affectedRows: totalAffectedRows, err: nil}, nil
}

func (e *DeleteQueryExecutor) deleteForAllShard(query *sqlparser.DeleteQuery) (sql.Result, error) {
	debug.Printf("[WARN] delete query for all shards. too slow")
	// 1. select for all shards to get delete targets
	// 2. exec delete query to every shard
	// 3. if succeeded delete query, merge selected rows from every shard
	// 4. exec delete query for sequencer table
	return nil, errors.New("still not support to delete for all shards")
}

// Exec executes DELETE query for shards.
func (e *DeleteQueryExecutor) Exec() (sql.Result, error) {
	query, ok := e.query.(*sqlparser.DeleteQuery)
	if !ok {
		return nil, errors.New("cannot convert to sqlparser.Query to *sqlparser.DeleteQuery")
	}

	if e.conn.IsUsedSequencer && e.conn.Sequencer == nil {
		return nil, errors.New("cannot delete. sequencer's connection is nil")
	}

	if query.IsDeleteTable {
		return e.deleteShardTable(query)
	} else if query.IsAllShardQuery {
		return e.deleteForAllShard(query)
	}

	shardConn, err := e.conn.ShardConnectionByID(int64(query.ShardKeyID))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	result, err := e.exec(shardConn, query.Text, query.Args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return result.(sql.Result), nil
}
