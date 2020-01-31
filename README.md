# Octillery [![GoDoc](https://godoc.org/go.knocknote.io/octillery?status.svg)](https://godoc.org/go.knocknote.io/octillery) [![CircleCI](https://circleci.com/gh/knocknote/octillery.svg?style=shield)](https://circleci.com/gh/knocknote/octillery)  [![codecov](https://codecov.io/gh/knocknote/octillery/branch/master/graph/badge.svg?token=hRKqugQMsg)](https://codecov.io/gh/knocknote/octillery) [![Go Report Card](https://goreportcard.com/badge/go.knocknote.io/octillery)](https://goreportcard.com/report/go.knocknote.io/octillery)


<img width="300px" height="238px" src="https://user-images.githubusercontent.com/209884/29391665-d1d6e1d0-8333-11e7-9a33-1db3dc9d2f72.png"></img>

`Octillery` is a Go package for sharding databases.
It can use with every OR Mapping library ( `xorm` , `gorp` , `gorm` , `dbr` ...) implementing `database/sql` interface, or raw SQL.

Currently supports `MySQL` (for product) and `SQLite3` (for testing) .

# Motivation

We need database sharding library in Go. Of course, we know some libraries like ( https://github.com/evalphobia/wizard , https://github.com/go-pg/sharding ). But OR Mapping library they support are restricted and we want to write sharding configuration declaratively, also expect to pluggable for sharding algorithm or database adapter, and expect to configurable sharding key or whether use sequencer or not.

# Features

- Supports every OR Mapping library implementing `database/sql` interface ( `xorm` , `gorp` , `gorm` , `dbr` , ... )
- Supports using `database/sql` ( raw SQL ) directly
- Pluggable sharding algorithm ( preinstalled algorithms are `modulo` and `hashmap` )
- Pluggable database adapter ( preinstalled adapters are `mysql` and `sqlite3` )
- Declarative describing for sharding configuration in `YAML`
- Configurable sharding algorithm, database adapter, sharding key, whether use sequencer or not.
- Supports capture read/write queries just before passing to database driver
- Supports database migration by CLI ( powered by `schemalex` )
- Supports import seeds from CSV

# Install

## Install as a CLI tool

```shell
go get go.knocknote.io/octillery/cmd/octillery
```

## Install as a library

```shell
go get go.knocknote.io/octillery
```

# How It Works

## 1. How database sharding works

We explain by using `posts` table.

`posts` table schema is

```sql
CREATE TABLE `posts` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `user_id` bigint unsigned NOT NULL,
  `created_at` datetime NOT NULL,
  `updated_at` datetime NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_posts_01` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

And we want to shard this table to four databases for load distribution.

In this case, we can try to two approach according to requirements.

### 1. Using Sequencer

If you want `id` to be unique in all databases, you should use this approach.  
Architecture of this approach would be the following.

![architecture](https://user-images.githubusercontent.com/209884/48552381-c8612a80-e91b-11e8-8625-af0043b2536c.png)

Application create SQL ( like `insert into posts (id, user_id, ...) values (null, 1, ...)` ), in this point, id value is null because still not decide. In accordance with the above graph, insert this query to one of the databases.

1. Application requests id value to sequencer
2. Sequencer generates next unique id in all shards
3. Sequencer returns id value to application ( ex. `id = 1` )
4. Replace `id` value from `null` to `1` (ex. `insert into posts (id, user_id, ...) values (1, 1, ...)` )
5. Decide target based of `id` value by sharding algorithm ( default `modulo` ) and insert record to selected database.

By using sequencer approach, you can get **unique** `id` value in all databases.
Therefore, if you insert multiple records, database records should looks like the following.

<table >
<tr>
  <th colspan=2>posts_shard_1</th>
  <th colspan=2>posts_shard_2</th>
  <th colspan=2>posts_shard_3</th>
  <th colspan=2>posts_shard_4</th>
</tr>
<tr align="center">
  <td>id</td>
  <td>user_id</td>
  <td>id</td>
  <td>user_id</td>
  <td>id</td>
  <td>user_id</td>
  <td>id</td>
  <td>user_id</td>
</tr>
<tr align="center">
  <td>1</td>
  <td>1</td>
  <td>2</td>
  <td>2</td>
  <td>3</td>
  <td>3</td>
  <td>4</td>
  <td>4</td>
</tr>
<tr align="center">
  <td>5</td>
  <td>5</td>
  <td>6</td>
  <td>6</td>
  <td>7</td>
  <td>7</td>
  <td>8</td>
  <td>8</td>
</tr>
</table>

### 2. Using Sharding Key ( without Sequencer )
If you don't care about uniqueness of `id`, you can use sharding key approach.  
Architecture of this appraoch would be the following.

![architecture2](https://user-images.githubusercontent.com/209884/48595164-e455e200-e996-11e8-8207-f9a432812cc6.png)

1. Decide target based of `user_id` value by sharding algorithm ( default `modulo` ) and insert record to selected database.

By using sharding key approach, same id value will appear in multiple databases.
Therefore, if you insert multiple records, database record should looks like the following.

<table >
<tr>
  <th colspan=2>posts_shard_1</th>
  <th colspan=2>posts_shard_2</th>
  <th colspan=2>posts_shard_3</th>
  <th colspan=2>posts_shard_4</th>
</tr>
<tr align="center">
  <td>id</td>
  <td>user_id</td>
  <td>id</td>
  <td>user_id</td>
  <td>id</td>
  <td>user_id</td>
  <td>id</td>
  <td>user_id</td>
</tr>
<tr align="center">
  <td>1</td>
  <td>1</td>
  <td>1</td>
  <td>2</td>
  <td>1</td>
  <td>3</td>
  <td>1</td>
  <td>4</td>
</tr>
<tr align="center">
  <td>2</td>
  <td>5</td>
  <td>2</td>
  <td>6</td>
  <td>2</td>
  <td>7</td>
  <td>2</td>
  <td>8</td>
</tr>
</table>

## 2. Requirements of database sharding library

We explained how to sharding database at section 1.
From this we define requirements of database sharding library.

- Know about database sharding configuration
- Capture query just before passing to database driver
- Parse query and find sharding key
- If use sequencer, requests `id` value to sequencer and replace value of `id` column by it
- Select sharding target based of sharding key by sharding algorithm

## 3. How Octillery works

### How To Capture Query

`Octillery` CLI tool supports `transpose` command.
It replace import statement of `database/sql` to `go.knocknote.io/octillery/database/sql`.

`go.knocknote.io/octillery/database/sql` package has compatible interface of `database/sql`.

Therefore, OR Mapping library call `Octillery`'s interface. and it can capture all queries.

### How To Parse SQL

`Octillery` use [github.com/knocknote/vitess-sqlparser](https://github.com/knocknote/vitess-sqlparser) as SQL parser. It implements powered by `vitess` and `tidb` .

### How To Use New Database Adapter

`Octillery` supports `mysql` and `sqlite3` adapter by default.  
If you want to use new database adapter, need to the following two steps.

1. Write `DBAdapter` interface. ( see https://godoc.org/go.knocknote.io/octillery/connection/adapter )
2. Put new adapter file to `go.knocknote.io/octillery/plugin` directory

### How To Use New Database Sharding Algorithm

`Octillery` supports `modulo` and `hashmap` algorithm by default.  
If you want to use new algorithm, need to the following two steps.

1. Write `ShardingAlgorithm` interface. ( see https://godoc.org/go.knocknote.io/octillery/algorithm )
2. Put new algorithm file to `go.knocknote.io/octillery/algorithm` directory

# Usage

## 1. Install CLI tool

```shell
$ go get go.knocknote.io/octillery/cmd/octillery
```

## 2. Install library

```shell
$ go get go.knocknote.io/octillery
```


## 3. Replace already imported `database/sql` statement

```shell
$ octillery transpose
```
※ `--dry-run` option confirms without overwriting

## 4. Install database adapter

```shell
$ octillery install --mysql
```

## 5. Describe database cofiguration in YAML

`databases.yml`

```yaml
default: &default
  adapter: mysql
  encoding: utf8mb4
  username: root
  master:
    - localhost:3306

tables:
  posts:
    shard: true
    shard_key: user_id
    shards:
      - post_shard_1:
          <<: *default
          database: posts_shard_1
      - post_shard_2:
          <<: *default
          database: posts_shard_2
```

## 6. Migrate database

```shell
$ octillery migrate --config databases.yml /path/to/schema
```

※ `--dry-run` option confirms migration plan

## 7. Load configuration file

```go
package main

import (
	"go.knocknote.io/octillery"
	"go.knocknote.io/octillery/database/sql"
)

func main() {
	if err := octillery.LoadConfig("databases.yml"); err != nil {
		panic(err)
	}
	db, _ := sql.Open("mysql", "")
	db.QueryRow("...")
}
```

# Document

See [GoDoc](https://godoc.org/go.knocknote.io/octillery)

# Development

## Install dependencies

```shell
$ make deps
```

If update dependencies, the following

1. Modify `glide.yaml`
2. Run `make update-deps`
3. Commit `glide.yaml` and `glide.lock`

## Run tests

```shell
$ make test
```

# See also

- `sqlparser` : https://github.com/knocknote/vitess-sqlparser
- `schemalex` : https://github.com/schemalex/schemalex

# Committers

Masaaki Goshima ([@goccy](https://github.com/goccy))

# LICENSE

MIT
