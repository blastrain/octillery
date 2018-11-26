package sqlparser

import (
	"fmt"
	"log"
	"path/filepath"
	"testing"

	"go.knocknote.io/octillery/config"
	"go.knocknote.io/octillery/path"
)

func checkErr(t *testing.T, err error) {
	if err != nil {
		t.Fatalf("%+v", err)
	}
}

func init() {
	confPath := filepath.Join(path.ThisDirPath(), "..", "test_databases.yml")
	if _, err := config.Load(confPath); err != nil {
		panic(err)
	}
}

func TestDDL(t *testing.T) {
	parser, err := New()
	checkErr(t, err)
	t.Run("create table", func(t *testing.T) {
		query, err := parser.Parse("create table if not exists users (id integer not null primary key, name varchar(255))")
		checkErr(t, err)
		if query.QueryType() != CreateTable {
			t.Fatal("cannot parse 'create table' query")
		}
		if query.Table() != "users" {
			t.Fatal("cannot parse 'create table' query")
		}
	})
	t.Run("drop table", func(t *testing.T) {
		query, err := parser.Parse("drop table if exists users")
		checkErr(t, err)
		if query.QueryType() != Drop {
			t.Fatal("cannot parse 'drop table' query")
		}
		if query.Table() != "users" {
			t.Fatal("cannot parse 'drop table' query")
		}
	})
	t.Run("truncate table", func(t *testing.T) {
		query, err := parser.Parse("truncate table users")
		checkErr(t, err)
		if query.QueryType() != TruncateTable {
			t.Fatal("cannot parse 'truncate table' query")
		}
		if query.Table() != "users" {
			t.Fatal("cannot parse 'truncate table' query")
		}
	})
}

func validateSelectQuery(t *testing.T, query Query) {
	if query.QueryType() != Select {
		t.Fatal("cannot parse 'select' query")
	}
	if query.Table() != "users" {
		t.Fatal("cannot parse 'select' query")
	}
}

func TestSELECT(t *testing.T) {
	parser, err := New()
	checkErr(t, err)
	t.Run("simple select query", func(t *testing.T) {
		query, err := parser.Parse("select name from users where id = 1")
		checkErr(t, err)
		validateSelectQuery(t, query)
		selectQuery := query.(*QueryBase)
		if selectQuery.ShardKeyIDPlaceholderIndex != 0 {
			t.Fatal("cannot parse")
		}
	})
	t.Run("select query with placeholder", func(t *testing.T) {
		t.Run("placeholder is first time", func(t *testing.T) {
			t.Run("condition order is first", func(t *testing.T) {
				query, err := parser.Parse("select name from users where id = ?")
				checkErr(t, err)
				validateSelectQuery(t, query)
				selectQuery := query.(*QueryBase)
				if selectQuery.ShardKeyIDPlaceholderIndex != 1 {
					t.Fatal("cannot parse")
				}
			})
			t.Run("condition order is second", func(t *testing.T) {
				query, err := parser.Parse("select name from users where name = 'bob' and id = ?")
				checkErr(t, err)
				validateSelectQuery(t, query)
				selectQuery := query.(*QueryBase)
				if selectQuery.ShardKeyIDPlaceholderIndex != 1 {
					t.Fatal("cannot parse")
				}
			})
		})
		t.Run("placeholder is second time", func(t *testing.T) {
			query, err := parser.Parse("select name from users where name = ? and id = ?")
			checkErr(t, err)
			validateSelectQuery(t, query)
			selectQuery := query.(*QueryBase)
			if selectQuery.ShardKeyIDPlaceholderIndex != 2 {
				t.Fatal("cannot parse")
			}
		})
	})
}

func TestINSERT(t *testing.T) {
	parser, err := New()
	checkErr(t, err)
	t.Run("sharding table", func(t *testing.T) {
		t.Run("use shard_column table", func(t *testing.T) {
			tableName := "users"
			t.Run("simple insert query", func(t *testing.T) {
				text := fmt.Sprintf("insert into %s(id, name) values (null, 'bob')", tableName)
				query, err := parser.Parse(text)
				checkErr(t, err)
				if query.QueryType() != Insert {
					t.Fatal("cannot parse 'insert' query")
				}
				if query.Table() != tableName {
					t.Fatal("cannot parse 'insert' query")
				}
				insertQuery := query.(*InsertQuery)
				if len(insertQuery.ColumnValues) != 2 {
					t.Fatal("cannot parse")
				}
				insertQuery.SetNextSequenceID(1) // simulate sequencer's action
				if string(insertQuery.ColumnValues[0]().Val) != "1" {
					t.Fatal("cannot parse column values")
				}
				if insertQuery.ColumnValues[1] != nil {
					t.Fatal("cannot parse column values")
				}
			})
			t.Run("insert query with placeholder", func(t *testing.T) {
				text := fmt.Sprintf("insert into %s(id, name) values (null, ?)", tableName)
				query, err := parser.Parse(text, "bob")
				checkErr(t, err)
				if query.QueryType() != Insert {
					t.Fatal("cannot parse 'insert' query")
				}
				if query.Table() != tableName {
					t.Fatal("cannot parse 'insert' query")
				}
				insertQuery := query.(*InsertQuery)
				if len(insertQuery.ColumnValues) != 2 {
					t.Fatal("cannot parse")
				}
				insertQuery.SetNextSequenceID(2) // simulate sequencer's action
				if string(insertQuery.ColumnValues[0]().Val) != "2" {
					t.Fatal("cannot parse column values")
				}
				if string(insertQuery.ColumnValues[1]().Val) != "bob" {
					t.Fatal("cannot parse column values")
				}
			})
		})
		t.Run("use shard_key table", func(t *testing.T) {
			tableName := "user_items"
			t.Run("simple insert query", func(t *testing.T) {
				text := fmt.Sprintf("insert into %s(id, user_id) values (null, %d)", tableName, 1)
				query, err := parser.Parse(text)
				checkErr(t, err)
				if query.QueryType() != Insert {
					t.Fatal("cannot parse 'insert' query")
				}
				if query.Table() != tableName {
					t.Fatal("cannot parse 'insert' query")
				}
				insertQuery := query.(*InsertQuery)
				if len(insertQuery.ColumnValues) != 2 {
					t.Fatal("cannot parse")
				}
				if insertQuery.ColumnValues[0] != nil {
					t.Fatal("cannot parse column values")
				}
				if insertQuery.ColumnValues[1] != nil {
					t.Fatal("cannot parse column values")
				}
			})
			t.Run("insert query with placeholder", func(t *testing.T) {
				text := fmt.Sprintf("insert into %s(id, user_id) values (null, ?)", tableName)
				query, err := parser.Parse(text, uint64(1))
				checkErr(t, err)
				if query.QueryType() != Insert {
					t.Fatal("cannot parse 'insert' query")
				}
				if query.Table() != tableName {
					t.Fatal("cannot parse 'insert' query")
				}
				insertQuery := query.(*InsertQuery)
				if len(insertQuery.ColumnValues) != 2 {
					t.Fatal("cannot parse")
				}
				if insertQuery.ColumnValues[0] != nil {
					t.Fatal("cannot parse column values")
				}
				if string(insertQuery.ColumnValues[1]().Val) != "1" {
					t.Fatal("cannot parse column values")
				}
				if insertQuery.String() != "insert into user_items(id, user_id) values (null, 1)" {
					t.Fatal("cannot generate parsed query")
				}
			})
		})
		t.Run("use shard_column and shard_key table", func(t *testing.T) {
			tableName := "user_decks"
			t.Run("simple insert query", func(t *testing.T) {
				text := fmt.Sprintf("insert into %s(id, user_id) values (null, %d)", tableName, 1)
				query, err := parser.Parse(text)
				checkErr(t, err)
				if query.QueryType() != Insert {
					t.Fatal("cannot parse 'insert' query")
				}
				if query.Table() != tableName {
					t.Fatal("cannot parse 'insert' query")
				}
				insertQuery := query.(*InsertQuery)
				insertQuery.SetNextSequenceID(3) // simulate sequencer's action
				if len(insertQuery.ColumnValues) != 2 {
					t.Fatal("cannot parse")
				}
				if string(insertQuery.ColumnValues[0]().Val) != "3" {
					t.Fatal("cannot parse column values")
				}
				if insertQuery.ColumnValues[1] != nil {
					t.Fatal("cannot parse column values")
				}
			})
			t.Run("insert query with placeholder", func(t *testing.T) {
				text := fmt.Sprintf("insert into %s(id, user_id) values (null, ?)", tableName)
				query, err := parser.Parse(text, uint64(1))
				checkErr(t, err)
				if query.QueryType() != Insert {
					t.Fatal("cannot parse 'insert' query")
				}
				if query.Table() != tableName {
					t.Fatal("cannot parse 'insert' query")
				}
				insertQuery := query.(*InsertQuery)
				insertQuery.SetNextSequenceID(4) // simulate sequencer's action
				if len(insertQuery.ColumnValues) != 2 {
					t.Fatal("cannot parse")
				}
				if string(insertQuery.ColumnValues[0]().Val) != "4" {
					t.Fatal("cannot parse column values")
				}
				if string(insertQuery.ColumnValues[1]().Val) != "1" {
					t.Fatal("cannot parse column values")
				}
				if insertQuery.String() != "insert into user_decks(id, user_id) values (4, 1)" {
					t.Fatal("cannot generate parsed query")
				}
			})
		})
	})
	t.Run("not sharding table", func(t *testing.T) {
		tableName := "user_stages"
		t.Run("simple insert query", func(t *testing.T) {
			text := fmt.Sprintf("insert into %s(id, name) values (null, 'bob')", tableName)
			query, err := parser.Parse(text)
			checkErr(t, err)
			if query.QueryType() != Insert {
				t.Fatal("cannot parse 'insert' query")
			}
			if query.Table() != tableName {
				t.Fatal("cannot parse 'insert' query")
			}
			insertQuery := query.(*InsertQuery)
			if len(insertQuery.ColumnValues) != 2 {
				t.Fatal("cannot parse")
			}
			if insertQuery.ColumnValues[0] != nil {
				t.Fatal("cannot parse column values")
			}
			if insertQuery.ColumnValues[1] != nil {
				t.Fatal("cannot parse column values")
			}
		})
		t.Run("insert query with placeholder", func(t *testing.T) {
			text := fmt.Sprintf("insert into %s(id, name) values (null, ?)", tableName)
			query, err := parser.Parse(text, "bob")
			checkErr(t, err)
			if query.QueryType() != Insert {
				t.Fatal("cannot parse 'insert' query")
			}
			if query.Table() != tableName {
				t.Fatal("cannot parse 'insert' query")
			}
			insertQuery := query.(*InsertQuery)
			if len(insertQuery.ColumnValues) != 2 {
				t.Fatal("cannot parse")
			}
			if insertQuery.ColumnValues[0] != nil {
				t.Fatal("cannot parse column values")
			}
			if string(insertQuery.ColumnValues[1]().Val) != "bob" {
				t.Fatal("cannot parse column values")
			}
		})
	})
}

func TestUPDATE(t *testing.T) {
	parser, err := New()
	checkErr(t, err)
	t.Run("sharding table", func(t *testing.T) {
		t.Run("use shard_column table", func(t *testing.T) {
			tableName := "users"
			t.Run("simple update query", func(t *testing.T) {
				text := fmt.Sprintf("update %s set name = 'alice' where id = 1", tableName)
				query, err := parser.Parse(text)
				checkErr(t, err)
				if query.QueryType() != Update {
					t.Fatal("cannot parse 'update' query")
				}
				if query.Table() != tableName {
					t.Fatal("cannot parse 'update' query")
				}
				updateQuery := query.(*QueryBase)
				if updateQuery.ShardKeyID != 1 {
					t.Fatal("cannot parse")
				}
				if updateQuery.ShardKeyIDPlaceholderIndex != 0 {
					t.Fatal("cannot parse")
				}
			})
			t.Run("update query with placeholder", func(t *testing.T) {
				text := fmt.Sprintf("update %s set name = 'alice' where id = ?", tableName)
				query, err := parser.Parse(text, int64(1))
				checkErr(t, err)
				if query.QueryType() != Update {
					t.Fatal("cannot parse 'update' query")
				}
				if query.Table() != tableName {
					t.Fatal("cannot parse 'update' query")
				}
				updateQuery := query.(*QueryBase)
				if updateQuery.ShardKeyID != 1 {
					t.Fatal("cannot parse")
				}
				if updateQuery.ShardKeyIDPlaceholderIndex != 1 {
					t.Fatal("cannot parse")
				}
			})
		})
		t.Run("use shard_key table", func(t *testing.T) {
			tableName := "user_items"
			t.Run("simple update query", func(t *testing.T) {
				text := fmt.Sprintf("update %s set name = 'alice' where user_id = 1", tableName)
				query, err := parser.Parse(text)
				checkErr(t, err)
				if query.QueryType() != Update {
					t.Fatal("cannot parse 'update' query")
				}
				if query.Table() != tableName {
					t.Fatal("cannot parse 'update' query")
				}
				updateQuery := query.(*QueryBase)
				if updateQuery.ShardKeyID != 1 {
					t.Fatal("cannot parse")
				}
				if updateQuery.ShardKeyIDPlaceholderIndex != 0 {
					t.Fatal("cannot parse")
				}
			})
			t.Run("update query with placeholder", func(t *testing.T) {
				text := fmt.Sprintf("update %s set name = 'alice' where user_id = ?", tableName)
				query, err := parser.Parse(text, int64(1))
				checkErr(t, err)
				if query.QueryType() != Update {
					t.Fatal("cannot parse 'update' query")
				}
				if query.Table() != tableName {
					t.Fatal("cannot parse 'update' query")
				}
				updateQuery := query.(*QueryBase)
				if updateQuery.ShardKeyID != 1 {
					t.Fatal("cannot parse")
				}
				if updateQuery.ShardKeyIDPlaceholderIndex != 1 {
					t.Fatal("cannot parse")
				}
			})
		})
	})
}

func TestDELETE(t *testing.T) {
	parser, err := New()
	checkErr(t, err)
	t.Run("sharding table", func(t *testing.T) {
		t.Run("use shard_column table", func(t *testing.T) {
			tableName := "users"
			t.Run("simple delete query", func(t *testing.T) {
				text := fmt.Sprintf("delete from %s where id = 1", tableName)
				query, err := parser.Parse(text)
				checkErr(t, err)
				if query.QueryType() != Delete {
					t.Fatal("cannot parse 'delete' query")
				}
				if query.Table() != tableName {
					t.Fatal("cannot parse 'delete' query")
				}
				deleteQuery := query.(*DeleteQuery)
				if deleteQuery.ShardKeyID != 1 {
					t.Fatal("cannot parse")
				}
				if deleteQuery.ShardKeyIDPlaceholderIndex != 0 {
					t.Fatal("cannot parse")
				}
			})
			t.Run("delete query with placeholder", func(t *testing.T) {
				text := fmt.Sprintf("delete from %s where id = ?", tableName)
				query, err := parser.Parse(text, int64(1))
				checkErr(t, err)
				if query.QueryType() != Delete {
					t.Fatal("cannot parse 'delete' query")
				}
				if query.Table() != tableName {
					t.Fatal("cannot parse 'delete' query")
				}
				deleteQuery := query.(*DeleteQuery)
				if deleteQuery.ShardKeyID != 1 {
					t.Fatal("cannot parse")
				}
				if deleteQuery.ShardKeyIDPlaceholderIndex != 1 {
					t.Fatal("cannot parse")
				}
			})
		})
		t.Run("use shard_key table", func(t *testing.T) {
			tableName := "user_items"
			t.Run("simple delete query", func(t *testing.T) {
				text := fmt.Sprintf("delete from %s where user_id = 1", tableName)
				query, err := parser.Parse(text)
				checkErr(t, err)
				if query.QueryType() != Delete {
					t.Fatal("cannot parse 'delete' query")
				}
				if query.Table() != tableName {
					t.Fatal("cannot parse 'delete' query")
				}
				deleteQuery := query.(*DeleteQuery)
				if deleteQuery.ShardKeyID != 1 {
					t.Fatal("cannot parse")
				}
				if deleteQuery.ShardKeyIDPlaceholderIndex != 0 {
					t.Fatal("cannot parse")
				}
			})
			t.Run("delete query with placeholder", func(t *testing.T) {
				text := fmt.Sprintf("delete from %s where user_id = ?", tableName)
				query, err := parser.Parse(text, int64(1))
				checkErr(t, err)
				if query.QueryType() != Delete {
					t.Fatal("cannot parse 'delete' query")
				}
				if query.Table() != tableName {
					t.Fatal("cannot parse 'delete' query")
				}
				deleteQuery := query.(*DeleteQuery)
				if deleteQuery.ShardKeyID != 1 {
					t.Fatal("cannot parse")
				}
				if deleteQuery.ShardKeyIDPlaceholderIndex != 1 {
					t.Fatal("cannot parse")
				}
			})
		})
	})
}

func TestERROR(t *testing.T) {
	parser, err := New()
	checkErr(t, err)
	t.Run("invalid query", func(t *testing.T) {
		query, err := parser.Parse("invalid query")
		if query != nil {
			t.Fatal("invalid query value")
		}
		if err == nil {
			t.Fatal("cannot handle error")
		}
		log.Println(err)
	})
	t.Run("unsupport query", func(t *testing.T) {
		query, err := parser.Parse("show create table users")
		if query != nil {
			t.Fatal("invalid query value")
		}
		if err == nil {
			t.Fatal("cannot handle error")
		}
		log.Println(err)
	})
	t.Run("unsupport ddl statement", func(t *testing.T) {
		query, err := parser.Parse("alter table users add age int")
		checkErr(t, err)
		if query.QueryType() != Unknown {
			t.Fatal("cannot parse query type")
		}
	})
	t.Run("unsupport join query", func(t *testing.T) {
		query, err := parser.Parse("select * from users inner join user_items ON users.id = user_items.user_id")
		if query != nil {
			t.Fatal("invalid query value")
		}
		if err == nil {
			t.Fatal("cannot handle error")
		}
		log.Println(err)
	})
}
