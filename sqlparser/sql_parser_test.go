package sqlparser

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParseSql(t *testing.T) {
	_sql := `
-- Student
CREATE TABLE IF NOT EXISTS student
(
    id        BIGINT(20)       NOT NULL AUTO_INCREMENT, -- this is id
    name      VARCHAR(128)     NOT NULL,
    sex       ENUM ('male', 'female', 'unknown') DEFAULT 'unknown',
    addr      VARCHAR,
    ctime     INT(11) UNSIGNED NOT NULL  DEFAULT 0,
    PRIMARY KEY (id),
	KEY idx(name, sex)
)
    ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4;
`
	stmts, err := ParseSql(_sql)
	if err != nil {
		t.Fatal(err)
	}
	for _, st := range stmts {
		s, _ := json.MarshalIndent(st, "", "  ")
		t.Log(string(s))
	}
}

func TestMyGen(t *testing.T) {
	_sql := `
-- Student
CREATE TABLE IF NOT EXISTS student
(
    id        BIGINT(20)       NOT NULL AUTO_INCREMENT, -- this is id
    name      VARCHAR(128)     NOT NULL,
    sex       ENUM ('male', 'female', 'unknown') DEFAULT 'unknown',
    addr      VARCHAR,
    ctime     INT(11) UNSIGNED NOT NULL,
    PRIMARY KEY (id),
	KEY idx(name, sex)
)
    ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4;
`
	ctStmts, err := ParseSql(_sql)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range ctStmts {
		t.Log(stmt.Table)
		for _, col := range stmt.Cols {
			var fieldGoType string
			fieldName := col.Name
			fieldType := col.Tp.Tp
			if fieldType == string(FieldTypeEnum) {
				enumType := col.Tp.Elems[0].Type
				switch enumType {
				case ElementBool:
					fieldGoType = "bool"
				case ElementString:
					fieldGoType = "string"
				case ElementNumber:
					fieldGoType = "int"
				}
			} else if strings.Contains(strings.ToLower(fieldType), "int") {
				fieldGoType = "int"
			} else if strings.Contains(strings.ToLower(fieldType), "char") {
				fieldGoType = "string"
			} else if strings.Contains(strings.ToLower(fieldType), "bool") {
				fieldGoType = "bool"
			} else {
				t.Fatal("unknown field type:" + fieldType)
			}
			t.Log(fieldName + " " + fieldGoType)
		}
	}
}
