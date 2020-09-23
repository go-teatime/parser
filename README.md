# parser
* kinds of parser
* 主要用于辅助实现代码生成器

#### sqlparser
* 轻量级sqlParser, 无任何外部依赖; 暂时仅支持mysql的建表语法
* 相对其他一些sqlparser, 该parser会将sql语句的注释也会解析出来
    + 例如下面例子`id`行的`json:ID`这一句会解析后放置在`ColumnDef`的`Annotation`字段
    + 例如下面例子的第一行的`-- Student`中的`Student`会解析后放置在`CreateTableStmt`的`Annotation`字段
* 解析注释是为了方便定制一些特殊需求, 有点像golang的structTag
* Example:
```go
_sql := `
-- Student
CREATE TABLE IF NOT EXISTS student
(
    id        BIGINT(20)       NOT NULL AUTO_INCREMENT, --json:ID
    name      VARCHAR(128)     NOT NULL, --json:Name
    sex       ENUM ('male', 'female', 'unknown') DEFAULT 'unknown',
    addr      VARCHAR,
    ctime     INT(11) UNSIGNED NOT NULL  DEFAULT 0,
    PRIMARY KEY (id),
	KEY idx(name, sex)
)
    ENGINE = InnoDB
    DEFAULT CHARSET = utf8mb4;
`
// 将上面的建表语句转为CreateTableStmt数据结构
stmts, err := ParseSql(_sql)

// 解析完成输出的表结构体
type CreateTableStmt struct {
	IfNotExists bool
	Table       string         // 表名
	Cols        []*ColumnDef   // 字段
	Constraints []*Constraint  // 约束
	Options     []*TableOption // 表选项
	Annotation  string         // 表上最近的一条注释
	Sql         string
}

```


#### goparser
* 将单个go文件的常用结构提取出来, 转为以下数据结构
```go
type GoCodeInfo struct {
	PkgName string     // 包名
	Imports []string   // import的包
	Structs []GoStruct // 定义的结构体
	Funcs   []GoFunc   // 定义的函数
}
```
