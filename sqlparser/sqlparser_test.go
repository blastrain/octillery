package sqlparser

import (
	"fmt"
	"log"
	"path/filepath"
	"testing"
	"time"

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

func TestSHOW(t *testing.T) {
	parser, err := New()
	checkErr(t, err)
	t.Run("show create table", func(t *testing.T) {
		query, err := parser.Parse("show create table users")
		fmt.Printf("show.table:%v\n", query.Table())
		checkErr(t, err)
		if query.QueryType() != Show {
			t.Fatal("cannot parse 'show' query")
		}
		if query.Table() != "users" {
			t.Fatal("cannot parse 'show' query")
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

func testInsertWithShardColumnTable(t *testing.T, tableName string) {
	parser, err := New()
	checkErr(t, err)
	t.Run("simple insert query", func(t *testing.T) {
		text := fmt.Sprintf("insert into %s(id, name, is_deleted, created_at) values (null, 'bob', 0, '2019-08-01 12:00:00')", tableName)
		query, err := parser.Parse(text)
		checkErr(t, err)
		if query.QueryType() != Insert {
			t.Fatal("cannot parse 'insert' query")
		}
		if query.Table() != tableName {
			t.Fatal("cannot parse 'insert' query")
		}
		insertQuery := query.(*InsertQuery)
		if len(insertQuery.ColumnValues) != 4 {
			t.Fatal("cannot parse")
		}
		insertQuery.SetNextSequenceID(1) // simulate sequencer's action
		if string(insertQuery.ColumnValues[0]().Val) != "1" {
			t.Fatal("cannot parse column values")
		}
		if insertQuery.ColumnValues[1] != nil {
			t.Fatal("cannot parse column values")
		}
		if insertQuery.ColumnValues[2] != nil {
			t.Fatal("cannot parse column values")
		}
		if insertQuery.ColumnValues[3] != nil {
			t.Fatal("cannot parse column values")
		}
	})
	t.Run("insert query with placeholder", func(t *testing.T) {
		text := fmt.Sprintf("insert into %s(id, name, is_deleted, created_at) values (?, ?, ?, ?)", tableName)
		createdAt, _ := time.Parse("2006-01-02 15:04:05", "2019-08-01 12:00:00")
		query, err := parser.Parse(text, nil, "bob", false, createdAt)
		checkErr(t, err)
		if query.QueryType() != Insert {
			t.Fatal("cannot parse 'insert' query")
		}
		if query.Table() != tableName {
			t.Fatal("cannot parse 'insert' query")
		}
		insertQuery := query.(*InsertQuery)
		if len(insertQuery.ColumnValues) != 4 {
			t.Fatal("cannot parse")
		}
		insertQuery.SetNextSequenceID(2) // simulate sequencer's action
		if string(insertQuery.ColumnValues[0]().Val) != "2" {
			t.Fatal("cannot parse column values")
		}
		if string(insertQuery.ColumnValues[1]().Val) != "bob" {
			t.Fatal("cannot parse column values")
		}
		if string(insertQuery.ColumnValues[2]().Val) != "0" {
			t.Fatal("cannot parse column values")
		}
		if string(insertQuery.ColumnValues[3]().Val) != "2019-08-01 12:00:00" {
			t.Fatal("cannot parse column values")
		}

	})
}

func testInsertWithShardKeyTable(t *testing.T, tableName string) {
	parser, err := New()
	checkErr(t, err)
	t.Run("simple insert query", func(t *testing.T) {
		text := fmt.Sprintf("insert into %s(id, user_id, is_deleted, created_at) values (null, %d, true, '2019-08-01 12:00:00')", tableName, 1)
		query, err := parser.Parse(text)
		checkErr(t, err)
		if query.QueryType() != Insert {
			t.Fatal("cannot parse 'insert' query")
		}
		if query.Table() != tableName {
			t.Fatal("cannot parse 'insert' query")
		}
		insertQuery := query.(*InsertQuery)
		if len(insertQuery.ColumnValues) != 4 {
			t.Fatal("cannot parse")
		}
		if insertQuery.ColumnValues[0] != nil {
			t.Fatal("cannot parse column values")
		}
		if insertQuery.ColumnValues[1] != nil {
			t.Fatal("cannot parse column values")
		}
		if insertQuery.ColumnValues[2] != nil {
			t.Fatal("cannot parse column values")
		}
		if insertQuery.ColumnValues[3] != nil {
			t.Fatal("cannot parse column values")
		}
	})
	t.Run("insert query with placeholder", func(t *testing.T) {
		text := fmt.Sprintf("insert into %s(id, user_id, is_deleted, created_at) values (?, ?, ?, ?)", tableName)
		createdAt, _ := time.Parse("2006-01-02 15:04:05", "2019-08-01 12:00:00")
		query, err := parser.Parse(text, nil, uint64(1), true, createdAt)
		checkErr(t, err)
		if query.QueryType() != Insert {
			t.Fatal("cannot parse 'insert' query")
		}
		if query.Table() != tableName {
			t.Fatal("cannot parse 'insert' query")
		}
		insertQuery := query.(*InsertQuery)
		if len(insertQuery.ColumnValues) != 4 {
			t.Fatal("cannot parse")
		}
		if string(insertQuery.ColumnValues[0]().Val) != "null" {
			t.Fatal("cannot parse column values")
		}
		if string(insertQuery.ColumnValues[1]().Val) != "1" {
			t.Fatal("cannot parse column values")
		}
		if string(insertQuery.ColumnValues[2]().Val) != "1" {
			t.Fatal("cannot parse column values")
		}
		if string(insertQuery.ColumnValues[3]().Val) != "2019-08-01 12:00:00" {
			t.Fatal("cannot parse column values")
		}
		if insertQuery.String() != "insert into user_items(id, user_id, is_deleted, created_at) values (null, 1, 1, '2019-08-01 12:00:00')" {
			t.Fatal("cannot generate parsed query")
		}
	})
}

func testInsertWithShardColumnAndShardKeyTable(t *testing.T, tableName string) {
	parser, err := New()
	checkErr(t, err)
	t.Run("simple insert query", func(t *testing.T) {
		createdAt, _ := time.Parse("2006-01-02 15:04:05", "2019-08-01 12:00:00")
		text := fmt.Sprintf("insert into %s(id, user_id, is_deleted, created_at) values (null, %d, %t, '%s')", tableName, 1, false, createdAt)
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
		if len(insertQuery.ColumnValues) != 4 {
			t.Fatal("cannot parse")
		}
		if string(insertQuery.ColumnValues[0]().Val) != "3" {
			t.Fatal("cannot parse column values")
		}
		if insertQuery.ColumnValues[1] != nil {
			t.Fatal("cannot parse column values")
		}
		if insertQuery.ColumnValues[2] != nil {
			t.Fatal("cannot parse column values")
		}
		if insertQuery.ColumnValues[3] != nil {
			t.Fatal("cannot parse column values")
		}
	})
	t.Run("insert query with placeholder use int64 type", func(t *testing.T) {
		text := fmt.Sprintf("insert into %s(id, user_id, is_deleted, created_at) values (?, ?, ?, ?)", tableName)
		createdAt, _ := time.Parse("2006-01-02 15:04:05", "2019-08-01 12:00:00")
		query, err := parser.Parse(text, nil, int64(1), false, createdAt)
		checkErr(t, err)
		if query.QueryType() != Insert {
			t.Fatal("cannot parse 'insert' query")
		}
		if query.Table() != tableName {
			t.Fatal("cannot parse 'insert' query")
		}
		insertQuery := query.(*InsertQuery)
		insertQuery.SetNextSequenceID(4) // simulate sequencer's action
		if len(insertQuery.ColumnValues) != 4 {
			t.Fatal("cannot parse")
		}
		if string(insertQuery.ColumnValues[0]().Val) != "4" {
			t.Fatal("cannot parse column values")
		}
		if string(insertQuery.ColumnValues[1]().Val) != "1" {
			t.Fatal("cannot parse column values")
		}
		if string(insertQuery.ColumnValues[2]().Val) != "0" {
			t.Fatal("cannot parse column values")
		}
		if string(insertQuery.ColumnValues[3]().Val) != "2019-08-01 12:00:00" {
			t.Fatal("cannot parse column values")
		}
		if insertQuery.String() != "insert into user_decks(id, user_id, is_deleted, created_at) values (4, 1, 0, '2019-08-01 12:00:00')" {
			t.Fatal("cannot generate parsed query")
		}
	})
	t.Run("insert query with placeholder use int64 pointer", func(t *testing.T) {
		text := fmt.Sprintf("insert into %s(id, user_id, is_deleted, created_at) values (?, ?, ?, ?)", tableName)
		userId := int64(1)
		isDeleted := false
		createdAt, _ := time.Parse("2006-01-02 15:04:05", "2019-08-01 12:00:00")
		query, err := parser.Parse(text, nil, &userId, &isDeleted, createdAt)
		checkErr(t, err)
		if query.QueryType() != Insert {
			t.Fatal("cannot parse 'insert' query")
		}
		if query.Table() != tableName {
			t.Fatal("cannot parse 'insert' query")
		}
		insertQuery := query.(*InsertQuery)
		insertQuery.SetNextSequenceID(4) // simulate sequencer's action
		if len(insertQuery.ColumnValues) != 4 {
			t.Fatal("cannot parse")
		}
		if string(insertQuery.ColumnValues[0]().Val) != "4" {
			t.Fatal("cannot parse column values")
		}
		if string(insertQuery.ColumnValues[1]().Val) != "1" {
			t.Fatal("cannot parse column values")
		}
		if string(insertQuery.ColumnValues[2]().Val) != "0" {
			t.Fatal("cannot parse column values")
		}
		if string(insertQuery.ColumnValues[3]().Val) != "2019-08-01 12:00:00" {
			t.Fatal("cannot parse column values")
		}
		if insertQuery.String() != "insert into user_decks(id, user_id, is_deleted, created_at) values (4, 1, 0, '2019-08-01 12:00:00')" {
			t.Fatal("cannot generate parsed query")
		}
	})
	t.Run("insert query with placeholder use uint64 type", func(t *testing.T) {
		text := fmt.Sprintf("insert into %s(id, user_id, is_deleted, created_at) values (?, ?, ?, ?)", tableName)
		createdAt, _ := time.Parse("2006-01-02 15:04:05", "2019-08-01 12:00:00")
		query, err := parser.Parse(text, nil, uint64(1), true, createdAt)
		checkErr(t, err)
		if query.QueryType() != Insert {
			t.Fatal("cannot parse 'insert' query")
		}
		if query.Table() != tableName {
			t.Fatal("cannot parse 'insert' query")
		}
		insertQuery := query.(*InsertQuery)
		insertQuery.SetNextSequenceID(4) // simulate sequencer's action
		if len(insertQuery.ColumnValues) != 4 {
			t.Fatal("cannot parse")
		}
		if string(insertQuery.ColumnValues[0]().Val) != "4" {
			t.Fatal("cannot parse column values")
		}
		if string(insertQuery.ColumnValues[1]().Val) != "1" {
			t.Fatal("cannot parse column values")
		}
		if string(insertQuery.ColumnValues[2]().Val) != "1" {
			t.Fatal("cannot parse column values")
		}
		if string(insertQuery.ColumnValues[3]().Val) != "2019-08-01 12:00:00" {
			t.Fatal("cannot parse column values")
		}
		if insertQuery.String() != "insert into user_decks(id, user_id, is_deleted, created_at) values (4, 1, 1, '2019-08-01 12:00:00')" {
			t.Fatal("cannot generate parsed query")
		}
	})
	t.Run("insert query with placeholder use uint64 pointer", func(t *testing.T) {
		text := fmt.Sprintf("insert into %s(id, user_id, is_deleted, created_at) values (?, ?, ?, ?)", tableName)
		userId := uint64(1)
		isDeleted := true
		createdAt, _ := time.Parse("2006-01-02 15:04:05", "2019-08-01 12:00:00")
		query, err := parser.Parse(text, nil, &userId, &isDeleted, &createdAt)
		checkErr(t, err)
		if query.QueryType() != Insert {
			t.Fatal("cannot parse 'insert' query")
		}
		if query.Table() != tableName {
			t.Fatal("cannot parse 'insert' query")
		}
		insertQuery := query.(*InsertQuery)
		insertQuery.SetNextSequenceID(4) // simulate sequencer's action
		if len(insertQuery.ColumnValues) != 4 {
			t.Fatal("cannot parse")
		}
		if string(insertQuery.ColumnValues[0]().Val) != "4" {
			t.Fatal("cannot parse column values")
		}
		if string(insertQuery.ColumnValues[1]().Val) != "1" {
			t.Fatal("cannot parse column values")
		}
		if string(insertQuery.ColumnValues[2]().Val) != "1" {
			t.Fatal("cannot parse column values")
		}
		if string(insertQuery.ColumnValues[3]().Val) != "2019-08-01 12:00:00" {
			t.Fatal("cannot parse column values")
		}
		if insertQuery.String() != "insert into user_decks(id, user_id, is_deleted, created_at) values (4, 1, 1, '2019-08-01 12:00:00')" {
			t.Fatal("cannot generate parsed query")
		}
	})
	t.Run("insert query with placeholder use nil pointer", func(t *testing.T) {
		text := fmt.Sprintf("insert into %s(id, user_id, is_deleted, created_at) values (?, ?, ?, ?)", tableName)
		query, err := parser.Parse(text, nil, nil, nil, nil)
		checkErr(t, err)
		if query.QueryType() != Insert {
			t.Fatal("cannot parse 'insert' query")
		}
		if query.Table() != tableName {
			t.Fatal("cannot parse 'insert' query")
		}
		insertQuery := query.(*InsertQuery)
		insertQuery.SetNextSequenceID(4) // simulate sequencer's action
		if len(insertQuery.ColumnValues) != 4 {
			t.Fatal("cannot parse")
		}
		if string(insertQuery.ColumnValues[0]().Val) != "4" {
			t.Fatal("cannot parse column values")
		}
		if string(insertQuery.ColumnValues[1]().Val) != "null" {
			t.Fatal("cannot parse column values")
		}
		if string(insertQuery.ColumnValues[2]().Val) != "null" {
			t.Fatal("cannot parse column values")
		}
		if string(insertQuery.ColumnValues[3]().Val) != "null" {
			t.Fatal("cannot parse column values")
		}
		if insertQuery.String() != "insert into user_decks(id, user_id, is_deleted, created_at) values (4, null, null, null)" {
			t.Fatal("cannot generate parsed query")
		}
	})
}

func testINSERTWithShardingTable(t *testing.T) {
	t.Run("use shard_column table", func(t *testing.T) {
		t.Run("use shard_column table", func(t *testing.T) {
			testInsertWithShardColumnTable(t, "users")
		})
		t.Run("use shard_key table", func(t *testing.T) {
			testInsertWithShardKeyTable(t, "user_items")
		})
		t.Run("use shard_column and shard_key table", func(t *testing.T) {
			testInsertWithShardColumnAndShardKeyTable(t, "user_decks")
		})
	})
}

func testInsertWithNotShardingTable(t *testing.T) {
	parser, err := New()
	checkErr(t, err)
	tableName := "user_stages"
	t.Run("simple insert query", func(t *testing.T) {
		text := fmt.Sprintf("insert into %s(id, name, created_at) values (null, 'bob', '2019-08-01 12:00:00')", tableName)
		query, err := parser.Parse(text)
		checkErr(t, err)
		if query.QueryType() != Insert {
			t.Fatal("cannot parse 'insert' query")
		}
		if query.Table() != tableName {
			t.Fatal("cannot parse 'insert' query")
		}
		insertQuery := query.(*InsertQuery)
		if len(insertQuery.ColumnValues) != 3 {
			t.Fatal("cannot parse")
		}
		if insertQuery.ColumnValues[0] != nil {
			t.Fatal("cannot parse column values")
		}
		if insertQuery.ColumnValues[1] != nil {
			t.Fatal("cannot parse column values")
		}
	})
	t.Run("insert query with placeholder use struct", func(t *testing.T) {
		text := fmt.Sprintf("insert into %s(id, name, created_at) values (?, ?, ?)", tableName)
		createdAt, _ := time.Parse("2006-01-02 15:04:05", "2019-08-01 12:00:00")
		query, err := parser.Parse(text, nil, "bob", createdAt)
		checkErr(t, err)
		if query.QueryType() != Insert {
			t.Fatal("cannot parse 'insert' query")
		}
		if query.Table() != tableName {
			t.Fatal("cannot parse 'insert' query")
		}
		insertQuery := query.(*InsertQuery)
		if len(insertQuery.ColumnValues) != 3 {
			t.Fatal("cannot parse")
		}
		if string(insertQuery.ColumnValues[0]().Val) != "null" {
			t.Fatal("cannot parse column values")
		}
		if string(insertQuery.ColumnValues[1]().Val) != "bob" {
			t.Fatal("cannot parse column values")
		}
		if string(insertQuery.ColumnValues[2]().Val) != "2019-08-01 12:00:00" {
			t.Fatal("cannot parse column values")
		}
	})
	t.Run("insert query with placeholder use pointer", func(t *testing.T) {
		text := fmt.Sprintf("insert into %s(id, name, created_at) values (?, ?, ?)", tableName)
		createdAt, _ := time.Parse("2006-01-02 15:04:05", "2019-08-01 12:00:00")
		query, err := parser.Parse(text, nil, "bob", &createdAt)
		checkErr(t, err)
		if query.QueryType() != Insert {
			t.Fatal("cannot parse 'insert' query")
		}
		if query.Table() != tableName {
			t.Fatal("cannot parse 'insert' query")
		}
		insertQuery := query.(*InsertQuery)
		if len(insertQuery.ColumnValues) != 3 {
			t.Fatal("cannot parse")
		}
		if string(insertQuery.ColumnValues[0]().Val) != "null" {
			t.Fatal("cannot parse column values")
		}
		if string(insertQuery.ColumnValues[1]().Val) != "bob" {
			t.Fatal("cannot parse column values")
		}
		if string(insertQuery.ColumnValues[2]().Val) != "2019-08-01 12:00:00" {
			t.Fatal("cannot parse column values")
		}
	})
}

func TestINSERT(t *testing.T) {
	t.Run("sharding table", func(t *testing.T) {
		testINSERTWithShardingTable(t)
	})
	t.Run("not sharding table", func(t *testing.T) {
		testInsertWithNotShardingTable(t)
	})
}

func testUpdateWithShardColumnTable(t *testing.T, tableName string) {
	parser, err := New()
	checkErr(t, err)
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
}

func testUpdateWithShardKeyTable(t *testing.T, tableName string) {
	parser, err := New()
	checkErr(t, err)
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
}

func testUpdateWithShardingTable(t *testing.T) {
	t.Run("use shard_column table", func(t *testing.T) {
		testUpdateWithShardColumnTable(t, "users")
	})
	t.Run("use shard_key table", func(t *testing.T) {
		testUpdateWithShardKeyTable(t, "user_items")
	})
}

func TestUPDATE(t *testing.T) {
	t.Run("sharding table", func(t *testing.T) {
		testUpdateWithShardingTable(t)
	})
}

func testDeleteWithShardColumnTable(t *testing.T, tableName string) {
	parser, err := New()
	checkErr(t, err)
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
}

func testDeleteWithShardKeyTable(t *testing.T, tableName string) {
	parser, err := New()
	checkErr(t, err)
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
}

func testDeleteWithShardingTable(t *testing.T) {
	t.Run("use shard_column table", func(t *testing.T) {
		testDeleteWithShardColumnTable(t, "users")
	})
	t.Run("use shard_key table", func(t *testing.T) {
		testDeleteWithShardKeyTable(t, "user_items")
	})
}

func TestDELETE(t *testing.T) {
	t.Run("sharding table", func(t *testing.T) {
		testDeleteWithShardingTable(t)
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
		query, err := parser.Parse("show slave status")
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
