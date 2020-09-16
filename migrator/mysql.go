package migrator

import (
	"bytes"
	"database/sql"
	"io"
	"strings"
	"unicode"

	vtparser "github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/pkg/errors"
	"github.com/schemalex/schemalex"
	"github.com/schemalex/schemalex/diff"
	"go.knocknote.io/octillery/sqlparser"
)

type schemaTextSource string

func (s schemaTextSource) WriteSchema(dst io.Writer) error {
	if _, err := io.WriteString(dst, string(s)); err != nil {
		return errors.Wrap(err, `failed to copy text contents to dst`)
	}
	return nil
}

type serverSource struct {
	conn *sql.DB
}

// WriteSchema get normalized schema from mysql server and write it to dst.
// This method's original source code is `schemalex/source.go`
func (s *serverSource) WriteSchema(dst io.Writer) error {
	db := s.conn
	tableRows, err := db.Query("SHOW TABLES")
	if err != nil {
		return errors.Wrap(err, `failed to execute 'SHOW TABLES'`)
	}
	defer tableRows.Close()
	parser, err := sqlparser.New()
	if err != nil {
		return errors.WithStack(err)
	}
	var (
		table       string
		tableSchema string
		buf         bytes.Buffer
	)
	for tableRows.Next() {
		if err := tableRows.Scan(&table); err != nil {
			return errors.Wrap(err, `failed to scan tables`)
		}

		if err := db.QueryRow("SHOW CREATE TABLE `"+table+"`").Scan(&table, &tableSchema); err != nil {
			return errors.Wrapf(err, `failed to execute 'SHOW CREATE TABLE "%s"'`, table)
		}
		if buf.Len() > 0 {
			buf.WriteString("\n\n")
		}
		query, err := parser.Parse(tableSchema)
		if err != nil {
			return errors.WithStack(err)
		}
		// normalize DDL because schemalex cannot parse PARTITION option
		normalizedSchema := vtparser.String(query.(*sqlparser.QueryBase).Stmt)
		buf.WriteString(normalizedSchema)
		buf.WriteByte(';')
	}

	return errors.WithStack(schemalex.NewReaderSource(&buf).WriteSchema(dst))
}

// MySQLMigrator implements DBMigratorPlugin
type MySQLMigrator struct {
	tableNameToQueryMap map[string]sqlparser.Query
}

// Init create mapping from table name to sqlparser.Query
func (m *MySQLMigrator) Init(queries []sqlparser.Query) {
	m.tableNameToQueryMap = map[string]sqlparser.Query{}
	for _, query := range queries {
		m.tableNameToQueryMap[query.Table()] = query
	}
}

// CompareSchema compare schema on mysql server with local schema
func (m *MySQLMigrator) CompareSchema(conn *sql.DB, allDDL []string) ([]string, error) {
	from := &serverSource{conn: conn}
	to := schemaTextSource(strings.Join(allDDL, ";\n"))
	var buf bytes.Buffer
	p := schemalex.New()
	if err := diff.Sources(
		&buf,
		from,
		to,
		diff.WithTransaction(false), diff.WithParser(p),
	); err != nil {
		return nil, errors.WithStack(err)
	}
	schemaDiff := buf.String()
	if len(schemaDiff) == 0 {
		return nil, nil
	}
	parser, err := sqlparser.New()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	replacedDDL := []string{}
	splittedDDL := strings.Split(schemaDiff, ";")
	for _, ddl := range splittedDDL {
		trimmedDDL := strings.TrimFunc(ddl, func(r rune) bool {
			return unicode.IsSpace(r)
		})
		if trimmedDDL == "" {
			continue
		}
		if !strings.HasPrefix(trimmedDDL, "CREATE TABLE") {
			replacedDDL = append(replacedDDL, trimmedDDL)
			continue
		}

		// If diff is `CREATE TABLE` statement, use original DDL ( not eliminated PARTITION option )
		stmt, err := parser.Parse(trimmedDDL)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		tableName := stmt.Table()
		query := m.tableNameToQueryMap[tableName]
		replacedDDL = append(replacedDDL, strings.TrimFunc(query.(*sqlparser.QueryBase).Text, func(r rune) bool {
			return unicode.IsSpace(r) || string(r) == ";"
		}))
	}
	return replacedDDL, nil
}

func init() {
	Register("mysql", func() DBMigratorPlugin {
		return &MySQLMigrator{}
	})
}
