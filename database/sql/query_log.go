package sql

import (
	"fmt"

	vtparser "github.com/knocknote/vitess-sqlparser/sqlparser"
	"github.com/pkg/errors"
	"go.knocknote.io/octillery/exec"
	"go.knocknote.io/octillery/sqlparser"
)

// GetParsedQueryByQueryLog get instance of `sqlparser.Query` by QueryLog.
// If QueryLog has LastInsertID value, add to query it
func (t *Tx) GetParsedQueryByQueryLog(log *QueryLog) (sqlparser.Query, error) {
	parser, err := sqlparser.New()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	query, err := parser.Parse(log.Query, log.Args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if query.QueryType() != sqlparser.Insert {
		return query, nil
	}

	insertQuery := query.(*sqlparser.InsertQuery)
	insertQuery.SetNextSequenceID(log.LastInsertID)
	t.replaceInsertQueryByQueryLog(log, insertQuery)
	return insertQuery, nil
}

// ConvertWriteQueryIntoCountQuery convert INSERT/UPDATE/DELETE query to `SELECT COUNT(*)`
func (t *Tx) ConvertWriteQueryIntoCountQuery(query sqlparser.Query) (sqlparser.Query, error) {
	parser, err := sqlparser.New()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	switch query.QueryType() {
	case sqlparser.Insert:
		countQuery := t.convertInsertQueryIntoCountQuery(query.Table(), query.(*sqlparser.InsertQuery))
		resultQuery, err := parser.Parse(t.countQueryToText(countQuery))
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return resultQuery, nil
	case sqlparser.Update:
		countQuery := t.convertUpdateQueryIntoCountQuery(query.Table(), query.(*sqlparser.QueryBase))
		resultQuery, err := parser.Parse(t.countQueryToText(countQuery))
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return resultQuery, nil
	case sqlparser.Delete:
		countQuery := t.convertDeleteQueryIntoCountQuery(query.Table(), query.(*sqlparser.DeleteQuery))
		resultQuery, err := parser.Parse(t.countQueryToText(countQuery))
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return resultQuery, nil
	}
	return nil, errors.Errorf("cannot convert query type %d", query.QueryType())
}

// IsAlreadyCommittedQueryLog returns whether write query gave by QueryLog is committed or not.
func (t *Tx) IsAlreadyCommittedQueryLog(log *QueryLog) (bool, error) {
	writeQuery, err := t.GetParsedQueryByQueryLog(log)
	if err != nil {
		return false, errors.WithStack(err)
	}
	queryType := writeQuery.QueryType()
	if !queryType.IsWriteQuery() {
		return false, errors.Errorf("query log is not write query type. type is %d", queryType)
	}
	countQuery, err := t.ConvertWriteQueryIntoCountQuery(writeQuery)
	if err != nil {
		return false, errors.WithStack(err)
	}
	conn, err := t.connMgr.ConnectionByTableName(countQuery.Table())
	if err != nil {
		return false, errors.WithStack(err)
	}
	t.begin(conn)
	if conn.IsShard {
		row, err := exec.NewQueryExecutor(nil, conn, t.tx, countQuery).QueryRow()
		if err != nil {
			return false, errors.WithStack(err)
		}
		var count uint
		if err := row.Scan(&count); err != nil {
			return false, errors.WithStack(err)
		}
		if queryType == sqlparser.Delete {
			return count == 0, nil
		}
		return count > 0, nil
	}
	queryText := countQuery.(*sqlparser.QueryBase).Text
	row, err := t.tx.QueryRow(nil, conn, queryText)
	if err != nil {
		return false, errors.WithStack(err)
	}
	var count uint
	if err := row.Scan(&count); err != nil {
		return false, errors.WithStack(err)
	}
	if queryType == sqlparser.Delete {
		return count == 0, nil
	}
	return count > 0, nil
}

// ExecWithQueryLog exec query by *connection.QueryLog.
// This is able to use for recovery from distributed transaction error.
func (t *Tx) ExecWithQueryLog(log *QueryLog) (Result, error) {
	query, err := t.GetParsedQueryByQueryLog(log)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !query.QueryType().IsWriteQuery() {
		return nil, errors.Errorf("cannot exec query type %d", query.QueryType())
	}
	conn, err := t.connMgr.ConnectionByTableName(query.Table())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	t.begin(conn)
	if conn.IsShard {
		result, err := exec.NewQueryExecutor(t.ctx, conn, t.tx, query).Exec()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return result, nil
	}
	result, err := t.tx.Exec(t.ctx, conn, log.Query, log.Args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return result, nil
}

func (*Tx) replaceInsertQueryByQueryLog(log *QueryLog, query *sqlparser.InsertQuery) error {
	if log.LastInsertID == 0 {
		return nil
	}
	stmt := query.Stmt
	foundIDColumnIndex := -1
	for idx, column := range stmt.Columns {
		if column.String() == "id" {
			foundIDColumnIndex = idx
			break
		}
	}
	if foundIDColumnIndex >= 0 {
		val := vtparser.NewIntVal([]byte(fmt.Sprint(log.LastInsertID)))
		stmt.Rows.(vtparser.Values)[0][foundIDColumnIndex] = val
	} else {
		columns := vtparser.Columns{}
		columns = append(columns, vtparser.NewColIdent("id"))
		for _, column := range stmt.Columns {
			columns = append(columns, column)
		}
		stmt.Columns = columns
		values := vtparser.Values{vtparser.ValTuple{}}
		val := vtparser.NewIntVal([]byte(fmt.Sprint(log.LastInsertID)))
		values[0] = append(values[0], val)
		for idx, expr := range stmt.Rows.(vtparser.Values)[0] {
			if query.ColumnValues[idx] != nil {
				values[0] = append(values[0], query.ColumnValues[idx]())
			} else {
				values[0] = append(values[0], expr)
			}
		}
		stmt.Rows = values
		query.ColumnValues = []func() *vtparser.SQLVal{}
	}
	return nil
}

func (*Tx) countQuery(tableName string, whereExpr vtparser.Expr) *vtparser.Select {
	return &vtparser.Select{
		SelectExprs: vtparser.SelectExprs{
			&vtparser.AliasedExpr{
				Expr: &vtparser.FuncExpr{
					Name:  vtparser.NewColIdent("count"),
					Exprs: vtparser.SelectExprs{&vtparser.StarExpr{}},
				},
			},
		},
		From: vtparser.TableExprs{
			&vtparser.AliasedTableExpr{
				Expr: vtparser.TableName{Name: vtparser.NewTableIdent(tableName)},
			},
		},
		Where: vtparser.NewWhere("where", whereExpr),
	}
}

func (*Tx) countQueryToText(query *vtparser.Select) string {
	buf := vtparser.NewTrackedBuffer(nil)
	query.Format(buf)
	return buf.String()
}

func (t *Tx) convertInsertQueryIntoCountQuery(tableName string, insertQuery *sqlparser.InsertQuery) *vtparser.Select {
	stmt := insertQuery.Stmt
	args := insertQuery.Args
	columns := stmt.Columns
	values := stmt.Rows.(vtparser.Values)[0]
	exprs := []*vtparser.ComparisonExpr{}
	for idx, column := range columns {
		value := values[idx]
		exprs = append(exprs, t.createEqualComparisonExprWithArgs(
			&vtparser.ColName{Name: column}, value, args,
		))
	}
	return t.countQuery(tableName, t.mergeComparisonExprs(exprs))
}

func (t *Tx) convertUpdateQueryIntoCountQuery(tableName string, updateQuery *sqlparser.QueryBase) *vtparser.Select {
	stmt := updateQuery.Stmt.(*vtparser.Update)
	args := updateQuery.Args
	comparisonExprs := t.exprToComparisonExprs(stmt.Where.Expr, args)
	for _, expr := range stmt.Exprs {
		comparisonExprs = append(comparisonExprs, t.createEqualComparisonExprWithArgs(expr.Name, expr.Expr, args))
	}
	return t.countQuery(tableName, t.mergeComparisonExprs(comparisonExprs))
}

func (t *Tx) convertDeleteQueryIntoCountQuery(tableName string, deleteQuery *sqlparser.DeleteQuery) *vtparser.Select {
	exprs := t.exprToComparisonExprs(deleteQuery.Stmt.Where.Expr, deleteQuery.Args)
	return t.countQuery(tableName, t.mergeComparisonExprs(exprs))
}

func (t *Tx) mergeComparisonExprs(comparisonExprs []*vtparser.ComparisonExpr) vtparser.Expr {
	var expr vtparser.Expr
	for _, comparisonExpr := range comparisonExprs {
		if expr == nil {
			expr = comparisonExpr
		} else {
			expr = &vtparser.AndExpr{
				Left:  expr,
				Right: comparisonExpr,
			}
		}
	}
	return expr
}

func (t *Tx) createEqualComparisonExprWithArgs(left vtparser.Expr, right vtparser.Expr, args []interface{}) *vtparser.ComparisonExpr {
	parser, _ := sqlparser.New()
	val := right.(*vtparser.SQLVal)
	if val.Type == vtparser.ValArg {
		arg := args[parser.ValueIndexByValArg(val)-1]
		switch arg.(type) {
		case int:
			val = vtparser.NewIntVal([]byte(fmt.Sprint(arg.(int))))
		case string:
			val = vtparser.NewStrVal([]byte(arg.(string)))
		}
	}
	return &vtparser.ComparisonExpr{
		Operator: vtparser.EqualStr,
		Left:     left,
		Right:    val,
	}
}

func (t *Tx) exprToComparisonExprs(expr vtparser.Expr, args []interface{}) []*vtparser.ComparisonExpr {
	comparisonExprs := []*vtparser.ComparisonExpr{}
	switch e := expr.(type) {
	case *vtparser.AndExpr:
		comparisonExprs = append(comparisonExprs, t.exprToComparisonExprs(e.Left, args)...)
		comparisonExprs = append(comparisonExprs, t.exprToComparisonExprs(e.Right, args)...)
	case *vtparser.ComparisonExpr:
		comparisonExprs = append(comparisonExprs, t.createEqualComparisonExprWithArgs(e.Left, e.Right, args))
	}
	return comparisonExprs
}
