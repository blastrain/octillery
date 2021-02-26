package main

import (
	"errors"
	"path/filepath"

	"github.com/gocraft/dbr"
	"go.knocknote.io/octillery"
	"go.knocknote.io/octillery/path"
)

type Member struct {
	ID      int64  `db:"id"`
	Number  int64  `db:"number"`
	Name    string `db:"name"`
	IsValid bool   `db:"is_valid"`
}

func main() {
	if err := octillery.LoadConfig(filepath.Join(path.ThisDirPath(), "databases.yml")); err != nil {
		panic(err)
	}
	conn, err := dbr.Open("postgres", "postgres://postgres:@127.0.0.1:5432/octillery_dbr?sslmode=disable", nil)
	if err != nil {
		panic(err)
	}
	sess := conn.NewSession(nil)
	if conn.DB != nil {
		if _, err := conn.DB.Exec(`
CREATE TABLE IF NOT EXISTS members(
    id serial NOT NULL PRIMARY KEY,
    number integer NOT NULL,
    name varchar(255),
    is_valid boolean NOT NULL
)`); err != nil {
			panic(err)
		}
	}
	if _, err := sess.DeleteFrom("members").Exec(); err != nil {
		panic(err)
	}

	result, err := sess.InsertInto("members").
		Columns("number", "name", "is_valid").
		Values(10, "Bob", true).
		Exec()
	if err != nil {
		panic(err)
	}

	count, err := result.RowsAffected()
	if err != nil {
		panic(err)
	}
	if count != 1 {
		panic(errors.New("cannot insert row"))
	}

	member := &Member{Number: 9, Name: "Ken", IsValid: false}

	sess.InsertInto("members").
		Columns("number", "name", "is_valid").
		Record(member).
		Exec()

	var members []Member
	sess.Select("*").From("members").Load(&members)

	if len(members) != 2 {
		panic(errors.New("cannot get members"))
	}

	attrsMap := map[string]interface{}{"number": 13, "name": "John"}
	if _, err := sess.Update("members").
		SetMap(attrsMap).
		Where("id = ?", members[0].ID).
		Exec(); err != nil {
		panic(err)
	}

	var m Member
	if _, err := sess.Select("*").
		From("members").
		Where("id = ?", members[0].ID).
		Load(&m); err != nil {
		panic(err)
	}

	if m.Name != "John" {
		panic(errors.New("cannot update row"))
	}
}
