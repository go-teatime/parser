package sqlparser

import (
	"fmt"
	"strings"
)

func GenSelectFields(cols []*ColumnDef) string {
	var fields []string
	for _, col := range cols {
		fields = append(fields, fmt.Sprintf("`%s`", col.Name))
	}
	return strings.Join(fields, ",")
}
