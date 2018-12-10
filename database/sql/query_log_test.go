package sql

import (
	"testing"

	"go.knocknote.io/octillery/connection"
	"go.knocknote.io/octillery/sqlparser"
)

func TestGetParsedQueryByQueryLog(t *testing.T) {
	db, err := Open("", "")
	checkErr(t, err)
	tx, err := db.Begin()
	checkErr(t, err)
	if _, err := tx.GetParsedQueryByQueryLog(&connection.QueryLog{
		Query: "invalid query",
	}); err == nil {
		t.Fatal("cannot handle error")
	}
}

func TestConvertWriteQueryIntoCountQuery(t *testing.T) {
	db, err := Open("", "")
	checkErr(t, err)
	tx, err := db.Begin()
	checkErr(t, err)
	readQuery, err := tx.GetParsedQueryByQueryLog(&connection.QueryLog{
		Query: "SELECT * FROM user_stages",
	})
	checkErr(t, err)
	if _, err := tx.ConvertWriteQueryIntoCountQuery(readQuery); err == nil {
		t.Fatal("cannot handle error")
	}
}

func TestConvertInsertQueryIntoCountQuery(t *testing.T) {
	db, err := Open("", "")
	checkErr(t, err)
	{
		tx, err := db.Begin()
		checkErr(t, err)
		queryLog := &connection.QueryLog{
			Query:        "INSERT INTO user_stages(user_id, name, age) VALUES (10, ?, ?)",
			Args:         []interface{}{"alice", 5},
			LastInsertID: 1,
		}
		writeQuery, err := tx.GetParsedQueryByQueryLog(queryLog)
		checkErr(t, err)
		if writeQuery.(*sqlparser.InsertQuery).String() != "insert into user_stages(id, user_id, name, age) values (1, 10, 'alice', 5)" {
			t.Fatalf("cannot get parsed query by query log %s", writeQuery.(*sqlparser.InsertQuery).String())
		}
		countQuery, err := tx.ConvertWriteQueryIntoCountQuery(writeQuery)
		checkErr(t, err)
		if countQuery.(*sqlparser.QueryBase).Text != "select count(*) from user_stages where id = 1 and user_id = 10 and name = 'alice' and age = 5" {
			t.Fatalf("cannot convert write query into count query %s", countQuery.(*sqlparser.QueryBase).Text)
		}
		checkErr(t, tx.Rollback())
	}
	{
		tx, err := db.Begin()
		checkErr(t, err)
		queryLog := &connection.QueryLog{
			Query:        "INSERT INTO users(id) VALUES (?)",
			LastInsertID: 1,
		}
		writeQuery, err := tx.GetParsedQueryByQueryLog(queryLog)
		checkErr(t, err)
		if writeQuery.(*sqlparser.InsertQuery).String() != "insert into users(id) values (1)" {
			t.Fatalf("cannot get parsed query by query log %s", writeQuery.(*sqlparser.InsertQuery).String())
		}
		countQuery, err := tx.ConvertWriteQueryIntoCountQuery(writeQuery)
		checkErr(t, err)
		if countQuery.(*sqlparser.QueryBase).Text != "select count(*) from users where id = 1" {
			t.Fatalf("cannot convert write query into count query %s", countQuery.(*sqlparser.QueryBase).Text)
		}
		checkErr(t, tx.Rollback())
	}
	{
		tx, err := db.Begin()
		checkErr(t, err)
		queryLog := &connection.QueryLog{
			Query:        "INSERT INTO user_items(id, user_id, name) VALUES (null, 10, 'alice')",
			LastInsertID: 2,
		}
		writeQuery, err := tx.GetParsedQueryByQueryLog(queryLog)
		checkErr(t, err)
		if writeQuery.(*sqlparser.InsertQuery).String() != "insert into user_items(id, user_id, name) values (2, 10, 'alice')" {
			t.Fatalf("cannot get parsed query by query log %s", writeQuery.(*sqlparser.InsertQuery).String())
		}
		countQuery, err := tx.ConvertWriteQueryIntoCountQuery(writeQuery)
		checkErr(t, err)
		if countQuery.(*sqlparser.QueryBase).Text != "select count(*) from user_items where id = 2 and user_id = 10 and name = 'alice'" {
			t.Fatalf("cannot convert write query into count query %s", countQuery.(*sqlparser.QueryBase).Text)
		}
		checkErr(t, tx.Rollback())
	}
}

func TestConvertUpdateQueryIntoCountQuery(t *testing.T) {
	db, err := Open("", "")
	checkErr(t, err)
	{
		tx, err := db.Begin()
		checkErr(t, err)
		queryLog := &connection.QueryLog{
			Query: "UPDATE user_stages set name = ?, age = 5 where user_id = ?",
			Args:  []interface{}{"alice", 10},
		}
		writeQuery, err := tx.GetParsedQueryByQueryLog(queryLog)
		checkErr(t, err)
		countQuery, err := tx.ConvertWriteQueryIntoCountQuery(writeQuery)
		checkErr(t, err)
		if countQuery.(*sqlparser.QueryBase).Text != "select count(*) from user_stages where user_id = 10 and name = 'alice' and age = 5" {
			t.Fatalf("cannot convert write query into count query %s", countQuery.(*sqlparser.QueryBase).Text)
		}
		checkErr(t, tx.Rollback())
	}
}

func TestConvertDeleteQueryIntoCountQuery(t *testing.T) {
	db, err := Open("", "")
	checkErr(t, err)
	{
		tx, err := db.Begin()
		checkErr(t, err)
		queryLog := &connection.QueryLog{
			Query: "DELETE from user_stages WHERE id = ? AND user_id = ?",
			Args:  []interface{}{1, 10},
		}
		writeQuery, err := tx.GetParsedQueryByQueryLog(queryLog)
		checkErr(t, err)
		countQuery, err := tx.ConvertWriteQueryIntoCountQuery(writeQuery)
		checkErr(t, err)
		if countQuery.(*sqlparser.QueryBase).Text != "select count(*) from user_stages where id = 1 and user_id = 10" {
			t.Fatalf("cannot convert write query into count query %s", countQuery.(*sqlparser.QueryBase).Text)
		}
		checkErr(t, tx.Rollback())
	}
}

func TestExecWithQueryLog(t *testing.T) {
	db, err := Open("", "")
	checkErr(t, err)
	{
		tx, err := db.Begin()
		checkErr(t, err)
		if _, err := tx.ExecWithQueryLog(&connection.QueryLog{
			Query: "invalid query",
		}); err == nil {
			t.Fatal("cannot handle error")
		}
	}
	{
		tx, err := db.Begin()
		checkErr(t, err)
		if _, err := tx.ExecWithQueryLog(&connection.QueryLog{
			Query: "DELETE FROM invalid_table WHERE id = 1",
		}); err == nil {
			t.Fatal("cannot handle error")
		}
	}
	{
		tx, err := db.Begin()
		checkErr(t, err)
		if _, err := tx.ExecWithQueryLog(&connection.QueryLog{
			Query: "SELECT * FROM user_stages WHERE id = 1",
		}); err == nil {
			t.Fatal("cannot handle error")
		}
	}
	{
		tx, err := db.Begin()
		checkErr(t, err)
		if _, err := tx.ExecWithQueryLog(&connection.QueryLog{
			Query:        "INSERT INTO user_stages(user_id) VALUES (10)",
			LastInsertID: 1,
		}); err != nil {
			t.Fatalf("%+v\n", err)
		}
		checkErr(t, tx.Rollback())
	}
	{
		tx, err := db.Begin()
		checkErr(t, err)
		if _, err := tx.ExecWithQueryLog(&connection.QueryLog{
			Query:        "INSERT INTO user_items(user_id) VALUES (10)",
			LastInsertID: 1,
		}); err != nil {
			t.Fatalf("%+v\n", err)
		}
		checkErr(t, tx.Rollback())
	}
	{
		tx, err := db.Begin()
		checkErr(t, err)
		if _, err := tx.ExecWithQueryLog(&connection.QueryLog{
			Query: "INSERT INTO user_items(id, user_id) VALUES (1, 10)",
		}); err != nil {
			t.Fatalf("%+v\n", err)
		}
		checkErr(t, tx.Rollback())
	}
	{
		tx, err := db.Begin()
		checkErr(t, err)
		if _, err := tx.ExecWithQueryLog(&connection.QueryLog{
			Query:        "INSERT INTO user_items(id, user_id) VALUES (null, 20)",
			LastInsertID: 1,
		}); err != nil {
			t.Fatalf("%+v\n", err)
		}
		checkErr(t, tx.Rollback())
	}
	{
		tx, err := db.Begin()
		checkErr(t, err)
		if _, err := tx.ExecWithQueryLog(&connection.QueryLog{
			Query:        "INSERT INTO user_items(id, user_id) VALUES (?, 30)",
			LastInsertID: 1,
		}); err != nil {
			t.Fatalf("%+v\n", err)
		}
		checkErr(t, tx.Rollback())
	}
	{
		tx, err := db.Begin()
		checkErr(t, err)
		if _, err := tx.ExecWithQueryLog(&connection.QueryLog{
			Query: "DELETE FROM user_stages WHERE user_id = ?",
			Args:  []interface{}{10},
		}); err != nil {
			t.Fatalf("%+v\n", err)
		}
		checkErr(t, tx.Rollback())
	}
}

func TestIsAlreadyCommittedQueryLog(t *testing.T) {
	db, err := Open("", "")
	checkErr(t, err)
	tx, err := db.Begin()
	checkErr(t, err)
	// call only
	if _, err := tx.IsAlreadyCommittedQueryLog(&connection.QueryLog{
		Query: "invalid query",
	}); err == nil {
		t.Fatal("cannot handle error")
	}
	if _, err := tx.IsAlreadyCommittedQueryLog(&connection.QueryLog{
		Query: "SELECT * FROM users",
	}); err == nil {
		t.Fatal("cannot handle error")
	}
	tx.IsAlreadyCommittedQueryLog(&connection.QueryLog{
		Query: "invalid query",
	})

	tx.IsAlreadyCommittedQueryLog(&connection.QueryLog{
		Query: "DELETE FROM user_stages WHERE user_id = ?",
		Args:  []interface{}{10},
	})
	tx.IsAlreadyCommittedQueryLog(&connection.QueryLog{
		Query: "DELETE FROM users WHERE id = 10",
	})
}
