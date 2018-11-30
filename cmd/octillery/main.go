package main

import (
	"bufio"
	"bytes"
	coresql "database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unicode"

	flags "github.com/jessevdk/go-flags"
	vtparser "github.com/knocknote/vitess-sqlparser/sqlparser"
	"github.com/pkg/errors"
	"github.com/schemalex/schemalex"
	"github.com/schemalex/schemalex/diff"
	"go.knocknote.io/octillery"
	"go.knocknote.io/octillery/algorithm"
	"go.knocknote.io/octillery/config"
	"go.knocknote.io/octillery/connection"
	_ "go.knocknote.io/octillery/connection/adapter/plugin"
	"go.knocknote.io/octillery/database/sql"
	"go.knocknote.io/octillery/printer"
	"go.knocknote.io/octillery/sqlparser"
	"go.knocknote.io/octillery/transposer"
)

// Option type for command line options
type Option struct {
	Version   VersionCommand   `description:"print the version of octillery" command:"version"`
	Transpose TransposeCommand `description:"replace 'database/sql' to 'go.knocknote.io/octillery/database/sql'" command:"transpose"`
	Migrate   MigrateCommand   `description:"migrate database schema ( powered by schemalex )" command:"migrate"`
	Import    ImportCommand    `description:"import seeds" command:"import"`
	Console   ConsoleCommand   `description:"database console" command:"console"`
	Install   InstallCommand   `description:"install database adapter" command:"install"`
	Shard     ShardCommand     `description:"get sharded database information by sharding key" command:"shard"`
}

// VersionCommand type for version command
type VersionCommand struct {
}

// TransposeCommand type for transpose command
type TransposeCommand struct {
	DryRun bool     `long:"dry-run" description:"show diff only"`
	Ignore []string `long:"ignore"  description:"ignore directory or file"`
}

// MigrateCommand type for migrate command
type MigrateCommand struct {
	DryRun bool   `long:"dry-run"           description:"show diff only"`
	Quiet  bool   `long:"quiet"   short:"q" description:"not print logs during migration"`
	Config string `long:"config"  short:"c" description:"database configuration file path" required:"config path"`
}

// ImportCommand type for import command
type ImportCommand struct {
	Config string `long:"config" short:"c" description:"database configuration file path" required:"config path"`
}

// ConsoleCommand type for console command
type ConsoleCommand struct {
	Config string `long:"config" short:"c" description:"database configuration file path" required:"config path"`
}

// InstallCommand type for install command
type InstallCommand struct {
	MySQLAdapter  bool `long:"mysql"  description:"install mysql adapter"`
	SQLiteAdapter bool `long:"sqlite" description:"install sqlite3 adapter"`
}

// ShardCommand type for shard command
type ShardCommand struct {
	ShardID int64  `long:"id"     short:"i" description:"id of sharding key column" required:"id"`
	Config  string `long:"config" short:"c" description:"database configuration file path" required:"config path"`
}

var opts Option

// Execute executes version command
func (cmd *VersionCommand) Execute(args []string) error {
	fmt.Printf(
		"octillery version %s, built with go %s for %s/%s\n",
		octillery.Version,
		runtime.Version(),
		runtime.GOOS,
		runtime.GOARCH,
	)
	return nil
}

// Execute executes tranpose command
func (cmd *TransposeCommand) Execute(args []string) error {
	searchPath := "."
	if len(args) > 0 {
		searchPath = args[0]
	}
	pattern := regexp.MustCompile("^database/sql")
	packagePrefix := "go.knocknote.io/octillery"
	transposeClosure := func(packageName string) string {
		return fmt.Sprintf("%s/%s", packagePrefix, packageName)
	}

	if cmd.DryRun {
		return errors.WithStack(transposer.New().TransposeDryRun(pattern, searchPath, cmd.Ignore, transposeClosure))
	}
	return errors.WithStack(transposer.New().Transpose(pattern, searchPath, cmd.Ignore, transposeClosure))
}

type schemaTextSource string

func (s schemaTextSource) WriteSchema(dst io.Writer) error {
	if _, err := io.WriteString(dst, string(s)); err != nil {
		return errors.Wrap(err, `failed to copy text contents to dst`)
	}
	return nil
}

type serverSource struct {
	conn *coresql.DB
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
	var table string
	var tableSchema string
	var buf bytes.Buffer
	for tableRows.Next() {
		if err = tableRows.Scan(&table); err != nil {
			return errors.Wrap(err, `failed to scan tables`)
		}

		if err = db.QueryRow("SHOW CREATE TABLE `"+table+"`").Scan(&table, &tableSchema); err != nil {
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

func (cmd *MigrateCommand) compareSchema(from schemalex.SchemaSource, to schemalex.SchemaSource) (string, error) {
	var buf bytes.Buffer
	p := schemalex.New()
	if err := diff.Sources(
		&buf,
		from,
		to,
		diff.WithTransaction(false), diff.WithParser(p),
	); err != nil {
		return "", errors.WithStack(err)
	}
	return buf.String(), nil
}

// CompareResult type for results of comparing schema
type CompareResult struct {
	diff string
	dsn  string
	conn *coresql.DB
}

// CombinedQuery has all `sqlparser.Query` for a DNS
type CombinedQuery struct {
	queries []sqlparser.Query
	conn    *coresql.DB
}

func (c *CombinedQuery) allDDL() string {
	allDDL := []string{}
	for _, query := range c.queries {
		// normalize DDL because schemalex cannot parse PARTITION option
		normalizedDDL := vtparser.String(query.(*sqlparser.QueryBase).Stmt)
		allDDL = append(allDDL, normalizedDDL)
	}
	return strings.Join(allDDL, ";\n")
}

// Execute executes migrate command
// nolint: gocyclo
func (cmd *MigrateCommand) Execute(args []string) error {
	if len(args) == 0 {
		return errors.New("argument is required. it is path to directory includes schema file or direct path to schema file")
	}
	if err := octillery.LoadConfig(cmd.Config); err != nil {
		return errors.WithStack(err)
	}

	schamePath := args[0]
	parser, err := sqlparser.New()
	if err != nil {
		return errors.WithStack(err)
	}
	tableNameToOriginalQueryMap := map[string]sqlparser.Query{}
	queries := []sqlparser.Query{}
	if err := filepath.Walk(schamePath, func(path string, info os.FileInfo, err error) error {
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
		tableNameToOriginalQueryMap[query.Table()] = query
		queries = append(queries, query)
		return nil
	}); err != nil {
		return errors.WithStack(err)
	}

	mgr, err := connection.NewConnectionManager()
	if err != nil {
		return errors.WithStack(err)
	}
	dsnToQueryMap := map[string]*CombinedQuery{}
	for _, query := range queries {
		conn, err := mgr.ConnectionByTableName(query.Table())
		if err != nil {
			return errors.WithStack(err)
		}
		if conn.IsShard {
			for _, shard := range conn.ShardConnections.AllShard() {
				cfg := conn.Config.ShardConfigByName(shard.ShardName)
				dsn := fmt.Sprintf("%s/%s", cfg.Masters[0], cfg.NameOrPath)
				if _, exists := dsnToQueryMap[dsn]; exists {
					dsnToQueryMap[dsn].queries = append(dsnToQueryMap[dsn].queries, query)
				} else {
					dsnToQueryMap[dsn] = &CombinedQuery{
						queries: []sqlparser.Query{query},
						conn:    shard.Connection,
					}
				}
			}
		} else {
			cfg := conn.Config
			dsn := fmt.Sprintf("%s/%s", cfg.Masters[0], cfg.NameOrPath)
			if _, exists := dsnToQueryMap[dsn]; exists {
				dsnToQueryMap[dsn].queries = append(dsnToQueryMap[dsn].queries, query)
			} else {
				dsnToQueryMap[dsn] = &CombinedQuery{
					queries: []sqlparser.Query{query},
					conn:    conn.Connection,
				}
			}
		}
	}
	results := []*CompareResult{}
	for dsn, combinedQuery := range dsnToQueryMap {
		allDDL := combinedQuery.allDDL()
		fromSource := &serverSource{
			conn: combinedQuery.conn,
		}
		diff, err := cmd.compareSchema(fromSource, schemaTextSource(allDDL))
		if err != nil {
			return errors.WithStack(err)
		}
		if len(diff) == 0 {
			continue
		}

		replacedDDL := []string{}
		splittedDDL := strings.Split(diff, ";")
		for _, ddl := range splittedDDL {
			if ddl == "" || ddl == "\n" {
				continue
			}
			if !strings.HasPrefix(ddl, "CREATE TABLE") {
				replacedDDL = append(replacedDDL, ddl+";")
				continue
			}

			// If diff is `CREATE TABLE` statement, use original DDL ( not eliminated PARTITION option )
			stmt, err := parser.Parse(ddl)
			if err != nil {
				return errors.WithStack(err)
			}
			tableName := stmt.Table()
			query := tableNameToOriginalQueryMap[tableName]
			replacedDDL = append(replacedDDL, query.(*sqlparser.QueryBase).Text)
		}
		results = append(results, &CompareResult{
			diff: strings.Join(replacedDDL, "\n"),
			dsn:  dsn,
			conn: combinedQuery.conn,
		})
	}
	if cmd.DryRun {
		if len(results) > 0 {
			for _, result := range results {
				if result.diff == "" || result.diff == "\n" {
					continue
				}
				fmt.Printf("[ %s ]\n\n", result.dsn)
				for _, diff := range strings.Split(result.diff, ";") {
					trimmedDiff := strings.TrimFunc(diff, func(r rune) bool {
						return unicode.IsSpace(r)
					})
					if trimmedDiff == "" {
						continue
					}
					fmt.Printf("%s\n\n", trimmedDiff)
				}
			}
		}
	} else {
		for _, result := range results {
			if !cmd.Quiet {
				fmt.Printf("[ %s ]\n\n", result.dsn)
			}
			for _, diff := range strings.Split(result.diff, ";") {
				trimmedDiff := strings.TrimFunc(diff, func(r rune) bool {
					return unicode.IsSpace(r)
				})
				if trimmedDiff == "" {
					continue
				}
				if !cmd.Quiet {
					fmt.Printf("%s\n\n", trimmedDiff)
				}
				if _, err := result.conn.Exec(trimmedDiff); err != nil {
					return errors.WithStack(err)
				}
			}
		}
	}
	return nil
}

func (cmd *ImportCommand) schemaFromTableName(tableName string) (vtparser.Statement, error) {
	mgr, err := connection.NewConnectionManager()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer mgr.Close()
	conn, err := mgr.ConnectionByTableName(tableName)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var db *coresql.DB
	if conn.IsShard {
		for _, shard := range conn.ShardConnections.AllShard() {
			db = shard.Connection
			break
		}
	} else {
		db = conn.Connection
	}
	if db == nil {
		return nil, errors.New("cannot get database connection")
	}
	var table string
	var tableSchema string
	if err = db.QueryRow(fmt.Sprintf("SHOW CREATE TABLE `%s`", tableName)).Scan(&table, &tableSchema); err != nil {
		return nil, errors.Wrapf(err, `failed to execute 'SHOW CREATE TABLE "%s"'`, tableName)
	}
	parser, err := sqlparser.New()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	query, err := parser.Parse(tableSchema)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return query.(*sqlparser.QueryBase).Stmt, nil
}

var (
	unsignedPattern  = regexp.MustCompile(`(?i)unsigned`)
	charPattern      = regexp.MustCompile(`(?i)char`)
	blobPattern      = regexp.MustCompile(`(?i)blob`)
	datePattern      = regexp.MustCompile(`(?i)date`)
	dateTimePattern  = regexp.MustCompile(`(?i)datetime`)
	timePattern      = regexp.MustCompile(`(?i)time`)
	timeStampPattern = regexp.MustCompile(`(?i)timestamp`)
	yearPattern      = regexp.MustCompile(`(?i)year`)
	intPattern       = regexp.MustCompile(`(?i)int`)
	floatPattern     = regexp.MustCompile(`(?i)float`)
	doublePattern    = regexp.MustCompile(`(?i)double`)
	decimalPattern   = regexp.MustCompile(`(?i)decimal`)
	enumPattern      = regexp.MustCompile(`(?i)enum`)
	setPattern       = regexp.MustCompile(`(?i)set`)
	textPattern      = regexp.MustCompile(`(?i)text`)
)

// GoType type of Go for mapping from MySQL type
type GoType int

const (
	// UnknownType the undefined type
	UnknownType GoType = iota
	// GoString type of string
	GoString
	// GoBytes type of bytes
	GoBytes
	// GoUint type of uint
	GoUint
	// GoInt type of int
	GoInt
	// GoFloat type of float
	GoFloat
	// GoDateFormat type of time.Time
	GoDateFormat
	// GoTimeFormat type of time.Time
	GoTimeFormat
	// GoDateTimeFormat type of time.Time
	GoDateTimeFormat
	// GoTimeStampFormat type of time.Time
	GoTimeStampFormat
	// GoYearFormat type of time.Time
	GoYearFormat
)

// nolint: gocyclo
func (cmd *ImportCommand) convertMySQLTypeToGOType(typ string) GoType {
	if charPattern.MatchString(typ) ||
		enumPattern.MatchString(typ) ||
		setPattern.MatchString(typ) ||
		textPattern.MatchString(typ) {
		return GoString
	}
	if blobPattern.MatchString(typ) {
		return GoBytes
	}
	if floatPattern.MatchString(typ) || doublePattern.MatchString(typ) {
		return GoFloat
	}
	if unsignedPattern.MatchString(typ) {
		return GoUint
	}
	if intPattern.MatchString(typ) || decimalPattern.MatchString(typ) {
		return GoInt
	}
	if dateTimePattern.MatchString(typ) {
		return GoDateTimeFormat
	}
	if datePattern.MatchString(typ) {
		return GoDateFormat
	}
	if timeStampPattern.MatchString(typ) {
		return GoTimeStampFormat
	}
	if timePattern.MatchString(typ) {
		return GoTimeFormat
	}
	if yearPattern.MatchString(typ) {
		return GoYearFormat
	}
	return UnknownType
}

func (cmd *ImportCommand) columnTypes(schema vtparser.Statement) (map[string]GoType, error) {
	columnToTypeMap := map[string]GoType{}
	for _, column := range schema.(*vtparser.CreateTable).Columns {
		typ := cmd.convertMySQLTypeToGOType(column.Type)
		if typ == UnknownType {
			return columnToTypeMap, errors.Errorf("cannot map %s to Go type", column.Type)
		}
		columnToTypeMap[column.Name] = typ
	}
	return columnToTypeMap, nil
}

func (cmd *ImportCommand) timeValueWithFormat(format string, v string) (*time.Time, error) {
	if v == "null" {
		return nil, nil
	}
	value, err := time.Parse(format, v)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &value, nil
}

// nolint: gocyclo
func (cmd *ImportCommand) values(record []string, types []GoType, columns []string, tableName string) ([]interface{}, error) {
	values := []interface{}{}
	for idx, v := range record {
		typ := types[idx]
		switch typ {
		case GoInt:
			value, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, errors.Wrapf(err, "cannot convert %v to int64. table:[%s] column:[%s]", v, tableName, columns[idx])
			}
			values = append(values, value)
		case GoUint:
			value, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return nil, errors.Wrapf(err, "cannot convert %v to uint64. table:[%s] column:[%s]", v, tableName, columns[idx])
			}
			values = append(values, value)
		case GoFloat:
			value, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return nil, errors.Wrapf(err, "cannot convert %v to float64. table:[%s] column:[%s]", v, tableName, columns[idx])
			}
			values = append(values, value)
		case GoString:
			values = append(values, v)
		case GoBytes:
			values = append(values, []byte(v))
		case GoDateFormat:
			format := "2006-01-02"
			value, err := cmd.timeValueWithFormat(format, v)
			if err != nil {
				return nil, errors.Wrapf(err, "cannot convert %v to time.Time. table:[%s] column:[%s]", v, tableName, columns[idx])
			}
			values = append(values, value)
		case GoTimeFormat:
			format := "15:04:05"
			value, err := cmd.timeValueWithFormat(format, v)
			if err != nil {
				return nil, errors.Wrapf(err, "cannot convert %v to time.Time. table:[%s] column:[%s]", v, tableName, columns[idx])
			}
			values = append(values, value)
		case GoDateTimeFormat, GoTimeStampFormat:
			format := "2006-01-02 15:04:05"
			value, err := cmd.timeValueWithFormat(format, v)
			if err != nil {
				return nil, errors.Wrapf(err, "cannot convert %v to time.Time. table:[%s] column:[%s]", v, tableName, columns[idx])
			}
			values = append(values, value)
		case GoYearFormat:
			format := "2006"
			value, err := cmd.timeValueWithFormat(format, v)
			if err != nil {
				return nil, errors.Wrapf(err, "cannot convert %v to time.Time. table:[%s] column:[%s]", v, tableName, columns[idx])
			}
			values = append(values, value)
		default:
		}
	}
	return values, nil
}

// Execute executes import command
// nolint: gocyclo
func (cmd *ImportCommand) Execute(args []string) error {
	if len(args) == 0 {
		return errors.New("argument is required. it is path to directory includes schema file or direct path to schema file")
	}
	if err := octillery.LoadConfig(cmd.Config); err != nil {
		return errors.WithStack(err)
	}
	cfg, err := config.Get()
	if err != nil {
		return errors.WithStack(err)
	}

	seedsPath := args[0]

	importTables := map[string][][]string{}

	if err := filepath.Walk(seedsPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return errors.WithStack(err)
		}
		if info.IsDir() {
			return nil
		}
		ext := filepath.Ext(path)
		if ext != ".csv" {
			return nil
		}
		baseName := filepath.Base(path)
		tableName := baseName[:len(baseName)-len(ext)]
		if _, exists := cfg.Tables[tableName]; !exists {
			return errors.Errorf("invalid table name %s", tableName)
		}
		seeds, err := ioutil.ReadFile(path)
		if err != nil {
			return errors.WithStack(err)
		}
		reader := csv.NewReader(strings.NewReader(string(seeds)))
		records, err := reader.ReadAll()
		if err != nil {
			return errors.WithStack(err)
		}
		importTables[tableName] = records
		return nil
	}); err != nil {
		return errors.WithStack(err)
	}

	conn, err := sql.Open("", "?parseTime=true")
	if err != nil {
		return errors.WithStack(err)
	}
	defer conn.Close()

	for tableName, records := range importTables {
		if len(records) < 2 {
			continue
		}
		schema, err := cmd.schemaFromTableName(tableName)
		if err != nil {
			return errors.Wrapf(err, "cannot get schema. table is %s", tableName)
		}
		columnNameToTypeMap, err := cmd.columnTypes(schema)
		if err != nil {
			return errors.Wrapf(err, "cannot get column types. table is %s", tableName)
		}
		columns := records[0]
		types := []GoType{}
		for _, column := range columns {
			typ, exists := columnNameToTypeMap[column]
			if !exists {
				return errors.Errorf("cannot get Go type from column name %s. table is %s", column, tableName)
			}
			types = append(types, typ)
		}

		placeholders := []string{}
		for i := 0; i < len(columns); i++ {
			placeholders = append(placeholders, "?")
		}
		escapedColumns := []string{}
		for _, column := range columns {
			escapedColumns = append(escapedColumns, fmt.Sprintf("`%s`", column))
		}
		if !cfg.Tables[tableName].IsShard {
			// try to bulk insert if not sharding table
			placeholderTmpl := fmt.Sprintf("(%s)", strings.Join(placeholders, ","))
			recordsWithoutHeader := records[1:]
			maxPlaceholderNum := 1000
			if len(recordsWithoutHeader) < maxPlaceholderNum {
				maxPlaceholderNum = len(recordsWithoutHeader)
			}
			allBulkRequestNum := len(recordsWithoutHeader) / maxPlaceholderNum
			remainRecordNum := len(recordsWithoutHeader) - maxPlaceholderNum*allBulkRequestNum
			if _, err := conn.Exec(fmt.Sprintf("TRUNCATE TABLE `%s`", tableName)); err != nil {
				return errors.Wrapf(err, "cannot truncate table %s", tableName)
			}
			for i := 0; i < allBulkRequestNum; i++ {
				start := i * maxPlaceholderNum
				end := start + maxPlaceholderNum
				if (i + 1) == allBulkRequestNum {
					end += remainRecordNum
				}
				filteredRecords := recordsWithoutHeader[start:end]
				allPlaceholders := []string{}
				values := []interface{}{}
				for _, record := range filteredRecords {
					vals, err := cmd.values(record, types, columns, tableName)
					if err != nil {
						return errors.WithStack(err)
					}
					allPlaceholders = append(allPlaceholders, placeholderTmpl)
					values = append(values, vals...)
				}
				prepareText := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tableName, strings.Join(escapedColumns, ","), strings.Join(allPlaceholders, ","))
				if _, err := conn.Exec(prepareText, values...); err != nil {
					return errors.Wrapf(err, "cannot insert [%s]:%v", prepareText, values)
				}
			}
		} else {
			prepareText := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, strings.Join(escapedColumns, ","), strings.Join(placeholders, ","))
			stmt, err := conn.Prepare(prepareText)
			if err != nil {
				return errors.Wrapf(err, "cannot prepare [%s]", prepareText)
			}
			if _, err := conn.Exec(fmt.Sprintf("TRUNCATE TABLE `%s`", tableName)); err != nil {
				return errors.Wrapf(err, "cannot truncate table %s", tableName)
			}
			for _, record := range records[1:] {
				values, err := cmd.values(record, types, columns, tableName)
				if err != nil {
					return errors.WithStack(err)
				}
				if _, err := stmt.Exec(values...); err != nil {
					return errors.Wrapf(err, "cannot insert [%s]:%v", prepareText, values)
				}
			}
		}
	}
	return nil
}

// Execute executes console command
func (cmd *ConsoleCommand) Execute(args []string) error {
	if err := octillery.LoadConfig(cmd.Config); err != nil {
		return errors.WithStack(err)
	}
	db, err := sql.Open("", "")
	if err != nil {
		return errors.WithStack(err)
	}
	fmt.Print("octillery> ")
	s := bufio.NewScanner(os.Stdin)
	for s.Scan() {
		query := s.Text()
		if query == "quit" || query == "exit" {
			return nil
		}
		multiRows, result, err := octillery.Exec(db, query)
		if err != nil {
			fmt.Printf("%+v\n", err)
		} else if multiRows != nil {
			printer, err := printer.NewPrinter(multiRows)
			if err != nil {
				fmt.Printf("%+v\n", err)
				return nil
			}
			printer.Print()
		} else if result != nil {

		}
		fmt.Print("octillery> ")
	}
	return nil
}

func (cmd *InstallCommand) lookupOctillery() (string, error) {
	libraryPath := filepath.Join("go.knocknote.io", "octillery")
	cwd, err := os.Getwd()
	if err != nil {
		return "", errors.WithStack(err)
	}
	// First, lookup vendor/go.knocknote.io/octillery
	vendorPath := filepath.Join(cwd, "vendor", libraryPath)
	if _, err := os.Stat(vendorPath); !os.IsNotExist(err) {
		return vendorPath, nil
	}
	// Second, lookup $GOPATH/src/go.knocknote.io/octillery
	underGoPath := filepath.Join(os.Getenv("GOPATH"), "src", libraryPath)
	if _, err := os.Stat(underGoPath); !os.IsNotExist(err) {
		return underGoPath, nil
	}
	return "", errors.New("cannot find 'go.knocknote.io/octillery' library")
}

// Execute executes install command
func (cmd *InstallCommand) Execute(args []string) error {
	var sourcePath string
	if len(args) > 0 {
		path, err := filepath.Abs(args[0])
		if err != nil {
			return errors.WithStack(err)
		}
		sourcePath = path
	} else {
		path, err := cmd.lookupOctillery()
		if err != nil {
			return errors.WithStack(err)
		}
		sourcePath = path
	}
	adapterBasePath := filepath.Join(sourcePath, "connection", "adapter", "plugin")
	var adapterPath string
	if cmd.MySQLAdapter {
		adapterPath = filepath.Join(adapterBasePath, "mysql.go")
	} else if cmd.SQLiteAdapter {
		adapterPath = filepath.Join(adapterBasePath, "sqlite3.go")
	} else {
		return errors.New("unknown adapter name. currently supports '--mysql' or '--sqlite' only")
	}
	adapterData, err := ioutil.ReadFile(adapterPath)
	if err != nil {
		return errors.WithStack(err)
	}
	baseName := filepath.Base(adapterPath)
	pluginPath := filepath.Join(sourcePath, "plugin", baseName)
	log.Printf("install to %s\n", pluginPath)
	return errors.WithStack(ioutil.WriteFile(pluginPath, adapterData, 0644))
}

// Execute executes shard command
func (cmd *ShardCommand) Execute(args []string) error {
	if len(args) == 0 {
		return errors.New("required table name included configuration file")
	}
	cfg, err := config.Load(cmd.Config)
	if err != nil {
		return errors.WithStack(err)
	}
	tableName := args[0]
	tableConfig, exists := cfg.Tables[tableName]
	if !exists {
		return errors.Errorf("cannot find table name %s in configuration file", tableName)
	}
	if !tableConfig.IsShard {
		return errors.Errorf("%s table is not sharded", tableName)
	}
	logic, err := algorithm.LoadShardingAlgorithm(tableConfig.Algorithm)
	if err != nil {
		return errors.WithStack(err)
	}
	conns := []*coresql.DB{}
	connMap := map[*coresql.DB]*config.DatabaseConfig{}
	for _, shardMap := range tableConfig.Shards {
		// append dummy connection
		conn := &coresql.DB{}
		for _, shard := range shardMap {
			connMap[conn] = shard
		}
		conns = append(conns, conn)
	}
	if !logic.Init(conns) {
		return errors.New("cannot initialize sharding algorithm")
	}
	conn, err := logic.Shard(conns, cmd.ShardID)
	if err != nil {
		return errors.WithStack(err)
	}
	if shardConfig, exists := connMap[conn]; exists {
		dsn := ""
		if len(shardConfig.Masters) > 0 {
			dsn = shardConfig.Masters[0]
		}
		info := struct {
			Database string `json:"database"`
			DSN      string `json:"dsn"`
		}{
			Database: shardConfig.NameOrPath,
			DSN:      dsn,
		}
		bytes, err := json.Marshal(info)
		if err != nil {
			return errors.WithStack(err)
		}
		fmt.Println(string(bytes))
		return nil
	}
	return errors.New("cannot find target database")
}

func main() {
	parser := flags.NewParser(&opts, flags.Default)
	parser.Parse()
}
