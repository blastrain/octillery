package migrator

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	vtparser "github.com/knocknote/vitess-sqlparser/sqlparser"
	"github.com/pkg/errors"
	"go.knocknote.io/octillery/connection"
	"go.knocknote.io/octillery/sqlparser"
)

// DBMigratorPlugin interface for migration
type DBMigratorPlugin interface {
	Init([]sqlparser.Query)
	CompareSchema(*sql.DB, []string) ([]string, error)
}

var (
	migratorPluginsMu sync.RWMutex
	migratorPlugins   = make(map[string]func() DBMigratorPlugin)
)

// Migrator migrates database schema
type Migrator struct {
	DryRun bool
	Quiet  bool
	Plugin DBMigratorPlugin
}

type dsnWithConnection struct {
	dsn  string
	conn *sql.DB
}

type combinedQuery struct {
	queries []sqlparser.Query
	conn    *sql.DB
}

// Register register DBMigratorPlugin with adapter name
func Register(name string, pluginCreator func() DBMigratorPlugin) {
	migratorPluginsMu.Lock()
	defer migratorPluginsMu.Unlock()
	if pluginCreator == nil {
		panic("plugin creator is nil")
	}
	if _, dup := migratorPlugins[name]; dup {
		panic("register called twice for migrator plugin " + name)
	}
	migratorPlugins[name] = pluginCreator
}

// NewMigrator creates instance of Migrator
func NewMigrator(adapter string, dryRun bool, isQuiet bool) (*Migrator, error) {
	plugin := migratorPlugins[adapter]
	if plugin == nil {
		return nil, errors.Errorf("cannot find migrator plugin for %s", adapter)
	}
	return &Migrator{
		DryRun: dryRun,
		Quiet:  !dryRun && isQuiet,
		Plugin: plugin(),
	}, nil
}

// Migrate executes migrate
func (m *Migrator) Migrate(schemaPath string) error {
	queries, err := m.queries(schemaPath)
	if err != nil {
		return errors.WithStack(err)
	}
	m.Plugin.Init(queries)
	dsnToQueryMap := map[string]*combinedQuery{}
	for _, query := range queries {
		dsnConns, err := m.dsnWithConnections(query)
		if err != nil {
			return errors.WithStack(err)
		}
		for _, dsnConn := range dsnConns {
			dsn := dsnConn.dsn
			if _, exists := dsnToQueryMap[dsn]; exists {
				dsnToQueryMap[dsn].queries = append(dsnToQueryMap[dsn].queries, query)
			} else {
				dsnToQueryMap[dsn] = &combinedQuery{
					queries: []sqlparser.Query{query},
					conn:    dsnConn.conn,
				}
			}
		}
	}
	for dsn, combinedQuery := range dsnToQueryMap {
		allDDL := combinedQuery.allDDL()
		diff, err := m.Plugin.CompareSchema(combinedQuery.conn, allDDL)
		if err != nil {
			return errors.WithStack(err)
		}
		if len(diff) == 0 {
			continue
		}
		if !m.Quiet {
			fmt.Printf("[ %s ]\n\n", dsn)
		}
		for _, diff := range diff {
			if !m.Quiet {
				fmt.Printf("%s\n\n", diff)
			}
			if m.DryRun {
				continue
			}
			if _, err := combinedQuery.conn.Exec(diff); err != nil {
				return errors.WithStack(err)
			}
		}
	}
	return nil
}

func (m *Migrator) queries(schemaPath string) ([]sqlparser.Query, error) {
	parser, err := sqlparser.New()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	queries := []sqlparser.Query{}
	if err := filepath.Walk(schemaPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return errors.WithStack(err)
		}
		if info.IsDir() {
			return nil
		}
		schema, err := ioutil.ReadFile(path)
		if err != nil {
			return errors.WithStack(err)
		}
		query, err := parser.Parse(string(schema))
		if err != nil {
			return errors.WithStack(err)
		}
		queries = append(queries, query)
		return nil
	}); err != nil {
		return nil, errors.WithStack(err)
	}
	return queries, nil
}

func (c *combinedQuery) allDDL() []string {
	allDDL := []string{}
	for _, query := range c.queries {
		// normalize DDL because schemalex cannot parse PARTITION option
		normalizedDDL := vtparser.String(query.(*sqlparser.QueryBase).Stmt)
		allDDL = append(allDDL, normalizedDDL)
	}
	return allDDL
}

func (m *Migrator) dsnWithConnections(query sqlparser.Query) ([]*dsnWithConnection, error) {
	mgr, err := connection.NewConnectionManager()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	conn, err := mgr.ConnectionByTableName(query.Table())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	dsnConns := []*dsnWithConnection{}
	if conn.IsShard {
		for _, shard := range conn.ShardConnections.AllShard() {
			cfg := conn.Config.ShardConfigByName(shard.ShardName)
			dsn := fmt.Sprintf("%s/%s", cfg.Masters[0], cfg.NameOrPath)
			dsnConns = append(dsnConns, &dsnWithConnection{
				dsn:  dsn,
				conn: shard.Connection,
			})
		}
	} else {
		cfg := conn.Config
		dsn := fmt.Sprintf("%s/%s", cfg.Masters[0], cfg.NameOrPath)
		dsnConns = append(dsnConns, &dsnWithConnection{
			dsn:  dsn,
			conn: conn.Connection,
		})
	}
	return dsnConns, nil
}
