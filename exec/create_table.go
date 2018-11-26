package exec

import (
	"database/sql"

	"github.com/pkg/errors"
	"go.knocknote.io/octillery/debug"
	"go.knocknote.io/octillery/sqlparser"
)

type CreateTableQueryExecutor struct {
	*QueryExecutorBase
}

func NewCreateTableQueryExecutor(base *QueryExecutorBase) *CreateTableQueryExecutor {
	return &CreateTableQueryExecutor{base}
}

func (e *CreateTableQueryExecutor) Query() ([]*sql.Rows, error) {
	return nil, errors.New("CreateTableQueryExecutor cannot invoke Query()")
}

func (e *CreateTableQueryExecutor) QueryRow() (*sql.Row, error) {
	return nil, errors.New("CreateTableQueryExecutor cannot invoke QueryRow()")
}

func (e *CreateTableQueryExecutor) Exec() (sql.Result, error) {
	debug.Printf("create table for shards")
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
