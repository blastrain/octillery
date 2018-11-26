package sqlparser

import (
	vtparser "github.com/knocknote/vitess-sqlparser/sqlparser"
)

type Identifier int64

const (
	UnknownID Identifier = -1
)

type QueryType int

const (
	Unknown QueryType = iota
	Select
	Insert
	Update
	Delete
	Drop
	CreateTable
	TruncateTable
)

type Query interface {
	Table() string
	QueryType() QueryType
}

func NewQueryBase(stmt vtparser.Statement, query string, args []interface{}) *QueryBase {
	return &QueryBase{
		Text:       query,
		Args:       args,
		Stmt:       stmt,
		ShardKeyID: UnknownID,
	}
}

type QueryBase struct {
	Text                       string
	Args                       []interface{}
	Type                       QueryType
	TableName                  string
	ShardKeyID                 Identifier
	ShardKeyIDPlaceholderIndex int
	Stmt                       vtparser.Statement
}

func (q *QueryBase) Table() string {
	return q.TableName
}

func (q *QueryBase) QueryType() QueryType {
	return q.Type
}

func (q *QueryBase) IsNotFoundShardKeyID() bool {
	return q.ShardKeyID == UnknownID
}

type InsertQuery struct {
	*QueryBase
	Stmt           *vtparser.Insert
	ColumnValues   []func() *vtparser.SQLVal
	nextSequenceID Identifier
}

func NewInsertQuery(queryBase *QueryBase, stmt *vtparser.Insert) *InsertQuery {
	values := stmt.Rows.(vtparser.Values)
	return &InsertQuery{
		QueryBase:    queryBase,
		Stmt:         stmt,
		ColumnValues: make([]func() *vtparser.SQLVal, len(values[0])),
	}
}

func (q *InsertQuery) NextSequenceID() Identifier {
	return q.nextSequenceID
}

func (q *InsertQuery) SetNextSequenceID(id int64) {
	q.nextSequenceID = Identifier(id)
}

func (q *InsertQuery) String() string {
	values := q.Stmt.Rows.(vtparser.Values)
	for idx, columnValue := range q.ColumnValues {
		if columnValue == nil {
			continue
		}
		values[0][idx] = columnValue()
	}
	return vtparser.String(q.Stmt)
}

type DeleteQuery struct {
	*QueryBase
	Stmt            *vtparser.Delete
	IsDeleteTable   bool
	IsAllShardQuery bool
}

func NewDeleteQuery(queryBase *QueryBase, stmt *vtparser.Delete) *DeleteQuery {
	return &DeleteQuery{
		QueryBase: queryBase,
		Stmt:      stmt,
	}
}

func (q *DeleteQuery) SetStateAfterParsing() {
	q.IsDeleteTable = q.IsNotFoundShardKeyID() &&
		q.Stmt.Where == nil && q.Stmt.OrderBy == nil && q.Stmt.Limit == nil
	q.IsAllShardQuery = q.IsNotFoundShardKeyID() &&
		(q.Stmt.Where != nil || q.Stmt.OrderBy != nil || q.Stmt.Limit != nil)
}
