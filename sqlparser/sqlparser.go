package sqlparser

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"time"

	vtparser "github.com/knocknote/vitess-sqlparser/sqlparser"
	"github.com/pkg/errors"
	"go.knocknote.io/octillery/config"
	"go.knocknote.io/octillery/debug"
)

// Parser the structure for parsing SQL
type Parser struct {
	cfg   *config.Config
	query *Query
}

var (
	replaceDoubleQuote   = regexp.MustCompile(`"`)
	removeSemiColon      = regexp.MustCompile(";")
	replaceAutoIncrement = regexp.MustCompile("autoincrement")
	replaceEngineParam   = regexp.MustCompile("engine=[A-Za-z-_0-9]+")
	replaceCharSetParam  = regexp.MustCompile("charset=[A-Za-z-_0-9]+")
)

var (
	ErrShardingKeyNotAllowNil = errors.New("sharding key does not allow nil")
)

func (p *Parser) shardColumnName(tableName string) string {
	return p.cfg.ShardColumnName(tableName)
}

func (p *Parser) shardKeyColumnName(tableName string) string {
	return p.cfg.ShardKeyColumnName(tableName)
}

func (p *Parser) isShardKeyColumn(valExpr vtparser.Expr, queryBase *QueryBase) bool {
	switch expr := valExpr.(type) {
	case *vtparser.ColName:
		if p.shardKeyColumnName(queryBase.TableName) == expr.Name.String() {
			return true
		}
	default:
		debug.Printf("default: %s", reflect.TypeOf(expr))
	}
	return false
}

func (p *Parser) ValueIndexByValArg(arg *vtparser.SQLVal) int {
	r := regexp.MustCompile(`:v([0-9]+)`)
	debug.Printf("ValArg: %s", string(arg.Val))
	results := r.FindAllStringSubmatch(string(arg.Val), -1)
	if len(results) > 0 && len(results[0]) > 1 {
		index, _ := strconv.Atoi(results[0][1])
		return index
	}
	return 0
}

func (p *Parser) parseShardColumnPlaceholderIndex(valExpr vtparser.Expr) int {
	switch expr := valExpr.(type) {
	case *vtparser.SQLVal:
		if expr.Type == vtparser.ValArg {
			return p.ValueIndexByValArg(expr)
		}
	default:
		debug.Printf("default: %s", reflect.TypeOf(expr))
	}
	return 0
}

func (p *Parser) parseVal(val *vtparser.SQLVal, queryBase *QueryBase) error {
	if val.Type != vtparser.ValArg {
		id, err := strconv.Atoi(string(val.Val))
		if err != nil {
			return errors.WithStack(err)
		}
		queryBase.ShardKeyID = Identifier(id)
		return nil
	}

	placeholderIndex := p.parseShardColumnPlaceholderIndex(val)
	if placeholderIndex == 0 {
		return errors.New("cannot parse shard_key column provided by query argument")
	}
	queryBase.ShardKeyIDPlaceholderIndex = placeholderIndex
	if len(queryBase.Args) >= placeholderIndex {
		arg := queryBase.Args[placeholderIndex-1]
		switch argType := arg.(type) {
		case int, int8, int16, int32, int64:
			queryBase.ShardKeyID = Identifier(argType.(int64))
		case uint, uint8, uint16, uint32, uint64:
			queryBase.ShardKeyID = Identifier(argType.(uint64))
		default:
			return errors.Errorf("unsupport shard_key type %s", reflect.TypeOf(arg))
		}
	}
	return nil
}

func (p *Parser) parseExpr(expr vtparser.Expr, queryBase *QueryBase) error {
	switch valExpr := expr.(type) {
	case *vtparser.SQLVal:
		// directly includes shard_key id in query
		if err := p.parseVal(valExpr, queryBase); err != nil {
			return errors.WithStack(err)
		}
	case *vtparser.AndExpr:
		if err := p.parseExpr(valExpr.Left, queryBase); err != nil {
			return errors.WithStack(err)
		}
		if err := p.parseExpr(valExpr.Right, queryBase); err != nil {
			return errors.WithStack(err)
		}
	case *vtparser.ComparisonExpr:
		if err := p.parseComparisonExpr(valExpr, queryBase); err != nil {
			return errors.WithStack(err)
		}
	case *vtparser.ParenExpr:
		if err := p.parseExpr(valExpr.Expr, queryBase); err != nil {
			return errors.WithStack(err)
		}
	default:
		return errors.Errorf("parse error. expr type '%s' does not supported", reflect.TypeOf(valExpr))
	}
	return nil
}

func (p *Parser) parseComparisonExpr(expr *vtparser.ComparisonExpr, queryBase *QueryBase) error {
	if !p.isShardKeyColumn(expr.Left, queryBase) {
		return nil
	}
	return errors.WithStack(p.parseExpr(expr.Right, queryBase))
}

func (p *Parser) parseWhere(where *vtparser.Where, queryBase *QueryBase) error {
	return errors.WithStack(p.parseExpr(where.Expr, queryBase))
}

func (p *Parser) parseAliasedTableExpr(stmt *vtparser.Select, tableExpr *vtparser.AliasedTableExpr, queryBase *QueryBase) error {
	switch expr := tableExpr.Expr.(type) {
	case vtparser.TableName:
		tableName := expr.Name.String()
		queryBase.TableName = tableName
		if !p.cfg.IsShardTable(tableName) {
			return nil
		}
		if stmt.Where == nil {
			return nil
		}
		return errors.WithStack(p.parseWhere(stmt.Where, queryBase))
	case *vtparser.Subquery:
		return errors.New("parse error. subquery does not supported")
	default:
	}
	return errors.Errorf("parse error. expr '%s' does not supported", reflect.TypeOf(tableExpr.Expr))
}

func (p *Parser) parseTableExpr(stmt *vtparser.Select, tableExpr vtparser.TableExpr, queryBase *QueryBase) error {
	switch expr := tableExpr.(type) {
	case *vtparser.AliasedTableExpr:
		return errors.WithStack(p.parseAliasedTableExpr(stmt, expr, queryBase))
	case *vtparser.ParenTableExpr:
	case *vtparser.JoinTableExpr:
		return errors.New("parse error. JOIN query does not supported")
	default:
		debug.Printf("default: %s", reflect.TypeOf(expr))
	}
	return nil
}

func (p *Parser) parseSelectStmt(stmt *vtparser.Select, queryBase *QueryBase) (Query, error) {
	queryBase.Type = Select
	for _, tableExpr := range stmt.From {
		if err := p.parseTableExpr(stmt, tableExpr, queryBase); err != nil {
			return nil, errors.WithStack(err)
		}
	}
	return queryBase, nil
}

func (p *Parser) replaceInsertValueFromValArg(query *InsertQuery, colIndex int, colName string, valArg string) error {
	r := regexp.MustCompile(`:v([0-9]+)`)
	results := r.FindAllStringSubmatch(valArg, -1)
	if len(results) == 0 || len(results[0]) == 0 {
		return nil
	}

	index, err := strconv.Atoi(results[0][1])
	if err != nil {
		return errors.WithStack(err)
	}
	if len(query.Args) <= index-1 {
		return nil
	}

	queryArg := query.Args[index-1]
	switch arg := queryArg.(type) {
	case string:
		query.ColumnValues[colIndex] = func() *vtparser.SQLVal {
			return &vtparser.SQLVal{
				Type: vtparser.StrVal,
				Val:  []byte(arg),
			}
		}
	case int, int8, int16, int32, int64:
		if colName == p.shardKeyColumnName(query.TableName) {
			query.ShardKeyID = Identifier(arg.(int64))
		}
		query.ColumnValues[colIndex] = createSQLIntTypeVal(arg)
	case *int:
		if colName == p.shardKeyColumnName(query.TableName) {
			if arg == nil {
				return errors.WithStack(ErrShardingKeyNotAllowNil)
			}
			query.ShardKeyID = Identifier(*arg)
		}
		if arg == nil {
			query.ColumnValues[colIndex] = createSQLNilTypeVal()
		} else {
			query.ColumnValues[colIndex] = createSQLIntTypeVal(*arg)
		}
	case *int8:
		if colName == p.shardKeyColumnName(query.TableName) {
			if arg == nil {
				return errors.WithStack(ErrShardingKeyNotAllowNil)
			}
			query.ShardKeyID = Identifier(*arg)
		}
		if arg == nil {
			query.ColumnValues[colIndex] = createSQLNilTypeVal()
		} else {
			query.ColumnValues[colIndex] = createSQLIntTypeVal(*arg)
		}
	case *int16:
		if colName == p.shardKeyColumnName(query.TableName) {
			if arg == nil {
				return errors.WithStack(ErrShardingKeyNotAllowNil)
			}
			query.ShardKeyID = Identifier(*arg)
		}
		if arg == nil {
			query.ColumnValues[colIndex] = createSQLNilTypeVal()
		} else {
			query.ColumnValues[colIndex] = createSQLIntTypeVal(*arg)
		}
	case *int32:
		if colName == p.shardKeyColumnName(query.TableName) {
			if arg == nil {
				return errors.WithStack(ErrShardingKeyNotAllowNil)
			}
			query.ShardKeyID = Identifier(*arg)
		}
		if arg == nil {
			query.ColumnValues[colIndex] = createSQLNilTypeVal()
		} else {
			query.ColumnValues[colIndex] = createSQLIntTypeVal(*arg)
		}
	case *int64:
		if colName == p.shardKeyColumnName(query.TableName) {
			if arg == nil {
				return errors.WithStack(ErrShardingKeyNotAllowNil)
			}
			query.ShardKeyID = Identifier(*arg)
		}
		if arg == nil {
			query.ColumnValues[colIndex] = createSQLNilTypeVal()
		} else {
			query.ColumnValues[colIndex] = createSQLIntTypeVal(*arg)
		}
	case uint, uint8, uint16, uint32, uint64:
		if colName == p.shardKeyColumnName(query.TableName) {
			query.ShardKeyID = Identifier(int64(arg.(uint64)))
		}
		query.ColumnValues[colIndex] = createSQLIntTypeVal(arg)
	case *uint:
		if colName == p.shardKeyColumnName(query.TableName) {
			if arg == nil {
				return errors.WithStack(ErrShardingKeyNotAllowNil)
			}
			query.ShardKeyID = Identifier(*arg)
		}
		if arg == nil {
			query.ColumnValues[colIndex] = createSQLNilTypeVal()
		} else {
			query.ColumnValues[colIndex] = createSQLIntTypeVal(*arg)
		}
	case *uint8:
		if colName == p.shardKeyColumnName(query.TableName) {
			if arg == nil {
				return errors.WithStack(ErrShardingKeyNotAllowNil)
			}
			query.ShardKeyID = Identifier(*arg)
		}
		if arg == nil {
			query.ColumnValues[colIndex] = createSQLNilTypeVal()
		} else {
			query.ColumnValues[colIndex] = createSQLIntTypeVal(*arg)
		}
	case *uint16:
		if colName == p.shardKeyColumnName(query.TableName) {
			if arg == nil {
				return errors.WithStack(ErrShardingKeyNotAllowNil)
			}
			query.ShardKeyID = Identifier(*arg)
		}
		if arg == nil {
			query.ColumnValues[colIndex] = createSQLNilTypeVal()
		} else {
			query.ColumnValues[colIndex] = createSQLIntTypeVal(*arg)
		}
	case *uint32:
		if colName == p.shardKeyColumnName(query.TableName) {
			if arg == nil {
				return errors.WithStack(ErrShardingKeyNotAllowNil)
			}
			query.ShardKeyID = Identifier(*arg)
		}
		if arg == nil {
			query.ColumnValues[colIndex] = createSQLNilTypeVal()
		} else {
			query.ColumnValues[colIndex] = createSQLIntTypeVal(*arg)
		}
	case *uint64:
		if colName == p.shardKeyColumnName(query.TableName) {
			if arg == nil {
				return errors.WithStack(ErrShardingKeyNotAllowNil)
			}
			query.ShardKeyID = Identifier(*arg)
		}
		if arg == nil {
			query.ColumnValues[colIndex] = createSQLNilTypeVal()
		} else {
			query.ColumnValues[colIndex] = createSQLIntTypeVal(*arg)
		}
	case bool:
		val := convertBoolToInt8(arg)
		query.ColumnValues[colIndex] = createSQLIntTypeVal(val)
	case *bool:
		if arg == nil {
			query.ColumnValues[colIndex] = func() *vtparser.SQLVal {
				return &vtparser.SQLVal{
					Type: vtparser.IntVal,
					Val:  []byte("null"),
				}
			}
		} else {
			val := convertBoolToInt8(*arg)
			query.ColumnValues[colIndex] = createSQLIntTypeVal(val)
		}
	case time.Time:
		query.ColumnValues[colIndex] = func() *vtparser.SQLVal {
			return &vtparser.SQLVal{
				Type: vtparser.StrVal,
				Val:  []byte(arg.Format("2006-01-02 15:04:05")),
			}
		}
	case *time.Time:
		if arg == nil {
			query.ColumnValues[colIndex] = func() *vtparser.SQLVal {
				return &vtparser.SQLVal{
					Type: vtparser.IntVal,
					Val:  []byte("null"),
				}
			}
		} else {
			query.ColumnValues[colIndex] = func() *vtparser.SQLVal {
				return &vtparser.SQLVal{
					Type: vtparser.StrVal,
					Val:  []byte(arg.Format("2006-01-02 15:04:05")),
				}
			}
		}
	case nil:
		query.ColumnValues[colIndex] = func() *vtparser.SQLVal {
			return &vtparser.SQLVal{
				Type: vtparser.IntVal,
				Val:  []byte("null"),
			}
		}
	default:
		debug.Printf("arg type = %s", reflect.TypeOf(arg))
	}
	return nil
}

func (p *Parser) replaceInsertValue(query *InsertQuery, colIndex int, colName string) error {
	if colName == p.shardColumnName(query.TableName) {
		query.ColumnValues[colIndex] = func() *vtparser.SQLVal {
			return &vtparser.SQLVal{
				Type: vtparser.IntVal,
				Val:  []byte(fmt.Sprint(query.NextSequenceID())),
			}
		}
		return nil
	}
	columnValues := query.Stmt.Rows.(vtparser.Values)[0]
	colValue, ok := columnValues[colIndex].(*vtparser.SQLVal)
	if !ok {
		return nil
	}
	if colValue.Type == vtparser.ValArg {
		if err := p.replaceInsertValueFromValArg(query, colIndex, colName, string(colValue.Val)); err != nil {
			return errors.WithStack(err)
		}
	} else if colName == p.shardKeyColumnName(query.TableName) {
		id, err := strconv.Atoi(string(colValue.Val))
		if err != nil {
			return errors.WithStack(err)
		}
		query.ShardKeyID = Identifier(id)
	}
	return nil
}

func (p *Parser) parseInsertStmt(stmt *vtparser.Insert, queryBase *QueryBase) (Query, error) {
	queryBase.Type = Insert
	queryBase.TableName = stmt.Table.Name.String()
	query := NewInsertQuery(queryBase, stmt)
	for idx, column := range stmt.Columns {
		colName := column.String()
		if err := p.replaceInsertValue(query, idx, colName); err != nil {
			return nil, errors.WithStack(err)
		}
	}
	return query, nil
}

func (p *Parser) parseUpdateExprs(exprs vtparser.UpdateExprs, queryBase *QueryBase) error {
	for _, updateExpr := range exprs {
		if p.shardKeyColumnName(queryBase.TableName) != updateExpr.Name.Name.String() {
			continue
		}
		if err := p.parseExpr(updateExpr.Expr, queryBase); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (p *Parser) simpleTableExprToName(expr vtparser.SimpleTableExpr) (string, error) {
	switch tableExpr := expr.(type) {
	case vtparser.TableName:
		return tableExpr.Name.String(), nil
	default:
	}
	return "", errors.Errorf("cannot parse TableExprs expr %s", reflect.TypeOf(expr))
}

func (p *Parser) tableExprsToName(exprs vtparser.TableExprs) (string, error) {
	for _, expr := range exprs {
		switch tableExpr := expr.(type) {
		case *vtparser.AliasedTableExpr:
			name, err := p.simpleTableExprToName(tableExpr.Expr)
			if err != nil {
				return "", errors.WithStack(err)
			}
			return name, nil
		case *vtparser.ParenTableExpr:
		case *vtparser.JoinTableExpr:
		default:
			debug.Printf("cannot parse TableExprs")
		}
	}
	return "", nil
}

func (p *Parser) parseUpdateStmt(stmt *vtparser.Update, queryBase *QueryBase) (Query, error) {
	tableName, err := p.tableExprsToName(stmt.TableExprs)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	queryBase.Stmt = stmt
	queryBase.Type = Update
	queryBase.TableName = tableName
	if !p.cfg.IsShardTable(tableName) {
		return queryBase, nil
	}

	if stmt.Exprs != nil {
		if err := p.parseUpdateExprs(stmt.Exprs, queryBase); err != nil {
			return nil, errors.WithStack(err)
		}
	}
	if stmt.Where != nil {
		if err := p.parseWhere(stmt.Where, queryBase); err != nil {
			return nil, errors.WithStack(err)
		}
	}
	return queryBase, nil
}

func (p *Parser) parseDeleteStmt(stmt *vtparser.Delete, queryBase *QueryBase) (Query, error) {
	tableName, err := p.tableExprsToName(stmt.TableExprs)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	queryBase.Type = Delete
	queryBase.Stmt = stmt
	queryBase.TableName = tableName
	query := NewDeleteQuery(queryBase, stmt)
	if !p.cfg.IsShardTable(tableName) {
		return query, nil
	}

	if stmt.Where != nil {
		if err := p.parseWhere(stmt.Where, queryBase); err != nil {
			return nil, errors.WithStack(err)
		}
	}
	query.setStateAfterParsing()
	return query, nil
}

func (p *Parser) parseCreateTable(stmt *vtparser.CreateTable, queryBase *QueryBase) (Query, error) {
	queryBase.Type = CreateTable
	queryBase.TableName = stmt.NewName.Name.String()
	return queryBase, nil
}

func (p *Parser) parseTruncateTable(stmt *vtparser.TruncateTable, queryBase *QueryBase) (Query, error) {
	queryBase.Type = TruncateTable
	queryBase.TableName = stmt.Table.Name.String()
	return queryBase, nil
}

func (p *Parser) parseDDLStmt(stmt *vtparser.DDL, queryBase *QueryBase) (Query, error) {
	switch stmt.Action {
	case "drop":
		queryBase.Type = Drop
		queryBase.TableName = stmt.Table.Name.String()
	default:
		debug.Printf("NewName = %s", stmt.NewName.Name.String())
		debug.Printf("Table   = %s", string(stmt.Table.Name.String()))
		debug.Printf("Action  = %s", stmt.Action)
	}
	return queryBase, nil
}

func (p *Parser) parseShowStmt(stmt *vtparser.Show, queryBase *QueryBase) (Query, error) {
	queryBase.Type = Show
	queryBase.TableName = stmt.TableName
	return queryBase, nil
}

func (p *Parser) formatQuery(query string) string {
	formattedQuery := replaceDoubleQuote.ReplaceAllString(query, "`")
	formattedQuery = removeSemiColon.ReplaceAllString(formattedQuery, "")
	formattedQuery = replaceAutoIncrement.ReplaceAllString(formattedQuery, "auto_increment")
	formattedQuery = replaceEngineParam.ReplaceAllString(formattedQuery, "")
	formattedQuery = replaceCharSetParam.ReplaceAllString(formattedQuery, "")
	return formattedQuery
}

// Parse parse SQL/DDL by [knocknote/vitess-sqlparser](https://github.com/knocknote/vitess-sqlparser),
// it returns Query interface includes table name or query type
// nolint: gocyclo
func (p *Parser) Parse(queryText string, args ...interface{}) (Query, error) {
	formattedQueryText := p.formatQuery(queryText)
	ast, err := vtparser.Parse(formattedQueryText)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	queryBase := NewQueryBase(ast, queryText, args)
	switch stmt := ast.(type) {
	case *vtparser.Select:
		query, err := p.parseSelectStmt(stmt, queryBase)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return query, nil
	case *vtparser.Insert:
		query, err := p.parseInsertStmt(stmt, queryBase)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return query, nil
	case *vtparser.Update:
		query, err := p.parseUpdateStmt(stmt, queryBase)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return query, nil
	case *vtparser.Delete:
		query, err := p.parseDeleteStmt(stmt, queryBase)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return query, nil
	case *vtparser.CreateTable:
		query, err := p.parseCreateTable(stmt, queryBase)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return query, nil
	case *vtparser.DDL:
		query, err := p.parseDDLStmt(stmt, queryBase)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return query, nil
	case *vtparser.TruncateTable:
		query, err := p.parseTruncateTable(stmt, queryBase)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return query, nil
	case *vtparser.Show:
		query, err := p.parseShowStmt(stmt, queryBase)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return query, nil
	default:
	}
	return nil, errors.Errorf("unsupported query type %s", reflect.TypeOf(ast))
}

// New creates Parser instance.
// If doesn't load configuration file before calling this, returns error.
func New() (*Parser, error) {
	cfg, err := config.Get()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &Parser{cfg: cfg}, nil
}

func createSQLIntTypeVal(val interface{}) func() *vtparser.SQLVal {
	return func() *vtparser.SQLVal {
		return &vtparser.SQLVal{
			Type: vtparser.IntVal,
			Val:  []byte(fmt.Sprintf("%d", val)),
		}
	}
}

func createSQLNilTypeVal() func() *vtparser.SQLVal {
	return func() *vtparser.SQLVal {
		return &vtparser.SQLVal{
			Type: vtparser.IntVal,
			Val:  []byte("null"),
		}
	}
}

func convertBoolToInt8(val bool) (res int8) {
	if val {
		res = 1
	}
	return res
}
