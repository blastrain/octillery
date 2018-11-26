package printer

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

type Row struct {
	values []string
}

type Printer struct {
	columns          []string
	maxColumnLengths []int
	allRows          []*Row
}

func NewPrinter(multiRows []*sql.Rows) (*Printer, error) {
	var columns []string
	var maxColumnLengths []int
	var allRows []*Row
	var err error
	for idx, rows := range multiRows {
		if idx == 0 {
			columns, err = rows.Columns()
			if err != nil {
				return nil, errors.WithStack(err)
			}
		}
		var fetchedColumns []interface{}
		for i := 0; i < len(columns); i += 1 {
			str := ""
			fetchedColumns = append(fetchedColumns, &str)
		}
		for rows.Next() {
			err := rows.Scan(fetchedColumns...)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			var values []string
			for _, value := range fetchedColumns {
				values = append(values, *value.(*string))
			}
			allRows = append(allRows, &Row{values: values})
		}
	}
	for columnIdx, _ := range columns {
		maxLength := len(columns[columnIdx])
		for _, row := range allRows {
			if maxLength < len(row.values[columnIdx]) {
				maxLength = len(row.values[columnIdx])
			}
		}
		maxColumnLengths = append(maxColumnLengths, maxLength)
	}
	return &Printer{
		columns:          columns,
		maxColumnLengths: maxColumnLengths,
		allRows:          allRows,
	}, nil
}

func (p *Printer) Print() {
	p.printRowDelimiter()
	for idx, column := range p.columns {
		fmt.Print("|")
		p.printColumn(idx, column)
	}
	fmt.Println("|")
	p.printRowDelimiter()
	for _, row := range p.allRows {
		for idx, value := range row.values {
			fmt.Print("|")
			p.printColumn(idx, value)
		}
		fmt.Println("|")
		p.printRowDelimiter()
	}
}

func (p *Printer) printRowDelimiter() {
	for idx, _ := range p.columns {
		fmt.Print("+")
		fmt.Print(strings.Repeat("-", p.maxColumnLengths[idx]+2))
	}
	fmt.Println("+")
}

func (p *Printer) printColumn(idx int, value string) {
	maxLength := p.maxColumnLengths[idx]
	length := maxLength - len(value) + 1
	fmt.Print(" " + value + strings.Repeat(" ", length))
}
