package exec

import (
	"database/sql"
	"strings"

	"github.com/pkg/errors"
	"go.knocknote.io/octillery/debug"
	"go.knocknote.io/octillery/sqlparser"
)

// SelectQueryExecutor inherits QueryExecutorBase structure
type SelectQueryExecutor struct {
	*QueryExecutorBase
}

// NewSelectQueryExecutor creates instance of SelectQueryExecutor
func NewSelectQueryExecutor(base *QueryExecutorBase) *SelectQueryExecutor {
	return &SelectQueryExecutor{base}
}

// Query select multiple rows for shards.
func (e *SelectQueryExecutor) Query() ([]*sql.Rows, error) {
	query, ok := e.query.(*sqlparser.QueryBase)
	if !ok {
		return nil, errors.New("cannot convert to sqlparser.Query to *sqlparser.QueryBase")
	}

	if e.conn.IsUsedSequencer && e.conn.Sequencer == nil {
		return nil, errors.New("cannot execute query. sequencer's connection is nil")
	}
	allRows := make([]*sql.Rows, 0)
	if query.IsNotFoundShardKeyID() {
		debug.Printf("[WARN] query for all shards. current support only simple merge. doesn't support 'count' or 'order by' or 'limit'")
		errs := []string{}
		e.tx = nil // transaction is ignored at this query
		for _, shardConn := range e.conn.ShardConnections.AllShard() {
			debug.Printf("(DB:%s):%s", shardConn.ShardName, query.Text)
			rows, err := e.execQuery(shardConn, query.Text, query.Args...)
			if err != nil {
				errs = append(errs, err.Error())
				continue
			}
			allRows = append(allRows, rows)
		}
		if len(errs) > 0 {
			err := strings.Join(errs, ":")
			return allRows, errors.New(err)
		}
		return allRows, nil
	}

	shardConn, err := e.conn.ShardConnectionByID(int64(query.ShardKeyID))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	debug.Printf("(DB:%s):%s", shardConn.ShardName, query.Text)
	rows, err := e.execQuery(shardConn, query.Text, query.Args...)
	if err != nil {
		return allRows, errors.WithStack(err)
	}
	allRows = append(allRows, rows)
	return allRows, nil
}

// QueryRow select row from single shard.
func (e *SelectQueryExecutor) QueryRow() (*sql.Row, error) {
	query, ok := e.query.(*sqlparser.QueryBase)
	if !ok {
		return nil, errors.New("cannot convert to sqlparser.Query to *sqlparser.QueryBase")
	}

	if e.conn.IsUsedSequencer && e.conn.Sequencer == nil {
		return nil, errors.New("cannot select row. sequencer's connection is nil")
	}

	if query.IsNotFoundShardKeyID() {
		debug.Printf("[WARN] cannot call queryRow for all shards")
		return nil, nil
	}

	shardConn, err := e.conn.ShardConnectionByID(int64(query.ShardKeyID))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	debug.Printf("(DB:%s):%s", shardConn.ShardName, query.Text)
	row, err := e.execQueryRow(shardConn, query.Text, query.Args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return row, nil
}

// Exec doesn't support in SelectQueryExecutor, returns always error.
func (e *SelectQueryExecutor) Exec() (sql.Result, error) {
	return nil, errors.New("SelectQueryExecutor cannot invoke Exec()")
}
