// 精简版 SqlParser (仅支持MySql建表语法)
package sqlparser

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
)

func ParseSql(sql string) ([]CreateTableStmt, error) {
	stmts, err := parseTree(sql)
	if err != nil {
		return nil, err
	}
	ctStmts := make([]CreateTableStmt, 0)
	for _, st := range stmts {
		// st.PrettyPrint()
		ctStmt := &CreateTableStmt{
			Cols:        make([]*ColumnDef, 0),
			Constraints: make([]*Constraint, 0),
			Options:     make([]*TableOption, 0),
		}
		ctStmt.Load(st.Root)
		ctStmt.Annotation = st.Annotation
		ctStmt.Sql = st.Sql
		ctStmts = append(ctStmts, *ctStmt)
	}
	return ctStmts, nil
}

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

type ColumnDef struct {
	Name       string          // 字段名
	Tp         *FieldType      // 字段类型
	Options    []*ColumnOption // 字段选项
	Annotation string          // 字段最右边如果有注释则注入至此
}

type ColumnOption struct {
	Tp       ColumnOptionType // 选项类型
	StrValue string           // default会用到
}

type FieldType struct {
	Tp    string // 字段基础类型
	Flen  int    // 长度如 VARCHAR(100)的100
	Elems []Elem // 枚举会用到
}

type ElementType string

const (
	ElementBool   ElementType = "Bool"
	ElementString ElementType = "String"
	ElementNumber ElementType = "Number"
)

type Elem struct {
	Type  ElementType
	Value string
}

type ColumnOptionType string

const (
	ColumnOptionPrimaryKey    ColumnOptionType = "PrimaryKey"
	ColumnOptionNotNull       ColumnOptionType = "NotNull"
	ColumnOptionAutoIncrement ColumnOptionType = "AutoIncrement"
	ColumnOptionDefaultValue  ColumnOptionType = "DefaultValue"
	ColumnOptionUniqKey       ColumnOptionType = "UniqKey"
	ColumnOptionComment       ColumnOptionType = "Comment"
)

type TableOption struct {
	Tp        TableOptionType
	Default   bool
	StrValue  string
	UintValue uint64
}

type TableOptionType string

const (
	TableOptionEngine        TableOptionType = "Engine"
	TableOptionCharset       TableOptionType = "Charset"
	TableOptionAutoIncrement TableOptionType = "AutoIncrement"
)

type Constraint struct {
	IfNotExists bool
	Tp          ConstraintType
	Name        string
	Keys        []string
}

type ConstraintType string

const (
	ConstraintPrimaryKey ConstraintType = "PrimaryKey"
	ConstraintKey        ConstraintType = "Key"
	ConstraintIndex      ConstraintType = "Index"
	ConstraintUniq       ConstraintType = "Uniq"
	ConstraintUniqKey    ConstraintType = "UniqKey"
	ConstraintUniqIndex  ConstraintType = "UniqIndex"
)

type FieldBaseType string

const (
	FieldTypeEnum FieldBaseType = "Enum"
)

func (ctStmt *CreateTableStmt) Load(rootNode *Decl) {
	if rootNode.Token == CreateToken {
		for _, crtSubD := range rootNode.Decl {
			if crtSubD.Token == TableToken {
				for _, tblSubD := range crtSubD.Decl {
					if tblSubD.Token == StringToken {
						// tableName
						ctStmt.Table = tblSubD.Lexeme
						for _, col := range tblSubD.Decl {
							// fieldName
							_col := ColumnDef{
								Name:    col.Lexeme,
								Options: make([]*ColumnOption, 0),
							}
							// fieldType fieldOption
							for _, fldSubD := range col.Decl {
								if fldSubD.Token == NotNullToken {
									_col.Options = append(_col.Options, &ColumnOption{
										Tp: ColumnOptionNotNull,
									})
								}
								if fldSubD.Token == AutoincrementToken {
									_col.Options = append(_col.Options, &ColumnOption{
										Tp: ColumnOptionAutoIncrement,
									})
								}
								if fldSubD.Token == PrimaryToken {
									_col.Options = append(_col.Options, &ColumnOption{
										Tp: ColumnOptionPrimaryKey,
									})
								}
								if fldSubD.Token == UniqueToken {
									_col.Options = append(_col.Options, &ColumnOption{
										Tp: ColumnOptionUniqKey,
									})
								}
								if fldSubD.Token == DefaultToken {
									_col.Options = append(_col.Options, &ColumnOption{
										Tp:       ColumnOptionDefaultValue,
										StrValue: fldSubD.Decl[0].Lexeme,
									})
								}
								if fldSubD.Token == CommentToken {
									_col.Options = append(_col.Options, &ColumnOption{
										Tp:       ColumnOptionComment,
										StrValue: fldSubD.Lexeme,
									})
								}
								if fldSubD.Token == AnnotationToken {
									_col.Annotation = fldSubD.Lexeme
								}
								if fldSubD.Token == EnumToken {
									var elems []Elem
									for _, em := range fldSubD.Decl {
										var emType ElementType
										if em.Token == NumberToken {
											emType = ElementNumber
										} else if em.Token == TrueToken || em.Token == FalseToken {
											emType = ElementBool
										} else if em.Token == StringToken {
											emType = ElementString
										}
										elems = append(elems, Elem{
											Type:  emType,
											Value: em.Lexeme,
										})
									}
									_col.Tp = &FieldType{
										Tp:    string(FieldTypeEnum),
										Elems: elems,
									}
								}
								if fldSubD.Token == StringToken {
									fLen := 0
									if len(fldSubD.Decl) > 0 {
										fLen, _ = strconv.Atoi(fldSubD.Decl[0].Lexeme)
									}
									_col.Tp = &FieldType{
										Tp:   fldSubD.Lexeme,
										Flen: fLen,
									}
								}
							}
							ctStmt.Cols = append(ctStmt.Cols, &_col)
						}
					}
					// if not exists
					if tblSubD.Token == IfNotExistsToken {
						ctStmt.IfNotExists = true
					}
					// constraint
					if tblSubD.Token == PrimaryKeyToken ||
						tblSubD.Token == KeyToken ||
						tblSubD.Token == IndexToken ||
						tblSubD.Token == UniqueToken ||
						tblSubD.Token == UniqueIndexToken ||
						tblSubD.Token == UniqueKeyToken {
						var keys []string
						for _, k := range tblSubD.Decl {
							keys = append(keys, k.Lexeme)
						}
						var cType ConstraintType
						switch tblSubD.Token {
						case PrimaryKeyToken:
							cType = ConstraintPrimaryKey
						case KeyToken:
							cType = ConstraintKey
						case IndexToken:
							cType = ConstraintIndex
						case UniqueToken:
							cType = ConstraintUniq
						case UniqueIndexToken:
							cType = ConstraintUniqIndex
						case UniqueKeyToken:
							cType = ConstraintUniqKey
						}
						ctStmt.Constraints = append(ctStmt.Constraints, &Constraint{
							Tp:   cType,
							Name: tblSubD.Lexeme,
							Keys: keys,
						})
					}
				}
			}
			// tableOption
			if crtSubD.Token == EngineToken {
				ctStmt.Options = append(ctStmt.Options, &TableOption{
					Tp:       TableOptionEngine,
					StrValue: crtSubD.Decl[0].Lexeme,
				})
			}
			if crtSubD.Token == CharsetToken {
				ctStmt.Options = append(ctStmt.Options, &TableOption{
					Tp:       TableOptionCharset,
					StrValue: crtSubD.Decl[0].Lexeme,
				})
			}
			if crtSubD.Token == AutoincrementToken {
				v, _ := strconv.Atoi(crtSubD.Decl[0].Lexeme)
				ctStmt.Options = append(ctStmt.Options, &TableOption{
					Tp:        TableOptionAutoIncrement,
					UintValue: uint64(v),
				})
			}
		}
	}
	for _, d1 := range rootNode.Decl {
		ctStmt.Load(d1)
	}
}

func (ctStmt CreateTableStmt) GetSqlUniqKey() []string {
	for _, c := range ctStmt.Constraints {
		if c.Tp == ConstraintUniq || c.Tp == ConstraintUniqIndex || c.Tp == ConstraintUniqKey {
			return c.Keys
		}
	}
	return nil
}

func (ctStmt CreateTableStmt) GetSqlPrimaryKey() []string {
	for _, c := range ctStmt.Constraints {
		if c.Tp == ConstraintPrimaryKey {
			return c.Keys
		}
	}
	return nil
}

func (ctStmt CreateTableStmt) HaveAutoIncrId() bool {
	for _, col := range ctStmt.Cols {
		if strings.ToLower(col.Name) == "id" {
			for _, opt := range col.Options {
				if opt.Tp == ColumnOptionAutoIncrement {
					return true
				}
			}
			return false
		}
	}
	return false
}

func parseTree(sql string) ([]stmtTree, error) {
	p := newSqlParser(sql)
	stmts, err := p.Parse()
	if err != nil {
		return nil, err
	}
	if len(stmts) == 0 {
		return nil, errors.New("len(stmts) is 0")
	}
	return stmts, nil
}

type stmtTree struct {
	Root       *Decl
	Annotation string // sql前如果有注释则会放在此字段
	Sql        string
}

func (stmt stmtTree) PrettyPrint() {
	stmt.Root.PrettyPrint(0)
}

func (d Decl) PrettyPrint(depth int) {
	indent := ""
	for i := 0; i < depth; i++ {
		indent = fmt.Sprintf("%s    ", indent)
	}
	fmt.Printf("%s|-> %s\n", indent, d.Lexeme)
	for _, subD := range d.Decl {
		subD.PrettyPrint(depth + 1)
	}
}

type Decl struct {
	Token  int
	Lexeme string
	Decl   []*Decl
}

func NewDecl(t Token) *Decl {
	return &Decl{
		Token:  t.Token,
		Lexeme: t.Lexeme,
	}
}

func (d *Decl) Add(subDecl *Decl) {
	d.Decl = append(d.Decl, subDecl)
}

type sqlParser struct {
	lexer    lexer
	stmt     []stmtTree
	index    int
	tokenLen int
	tokens   []Token
}

func newSqlParser(sql string) *sqlParser {
	l := newLexer(sql)
	tokens, err := l.Parse()
	if err != nil {
		panic(err)
	}
	tokens = stripSpaces(tokens)
	// tokens = stripAnnotation(tokens) // 不需要注释信息则可以去掉
	return &sqlParser{
		lexer:    *l,
		stmt:     nil,
		index:    0,
		tokenLen: len(tokens),
		tokens:   tokens,
	}
}

func (p *sqlParser) Parse() ([]stmtTree, error) {
	tokens := p.tokens
	var anno string
	var startPos, endPos int
	for p.hasNext() {
		if tokens[p.index].Token == SemicolonToken {
			p.index++
			continue
		}
		if tokens[p.index].Token == SpaceToken {
			p.index++
			continue
		}
		if tokens[p.index].Token == AnnotationToken {
			anno = tokens[p.index].Lexeme
			p.index++
			continue
		}
		switch tokens[p.index].Token {
		case CreateToken:
			stmt, err := p.parseCreate()
			if err != nil {
				return nil, err
			}
			stmt.Annotation = anno
			endPos = tokens[p.index].Pos
			stmt.Sql = strings.TrimSpace(string(p.lexer.instruction[startPos:endPos]))
			p.stmt = append(p.stmt, *stmt)
			anno = ""
			startPos = endPos + 1
			break
		default:
			return nil, fmt.Errorf("parsing error near <%s>", tokens[p.index].Lexeme)
		}
	}

	return p.stmt, nil
}

func (p *sqlParser) parseCreate() (*stmtTree, error) {
	tokens := p.tokens
	st := &stmtTree{}
	createDecl := NewDecl(tokens[p.index])
	st.Root = createDecl

	if !p.hasNext() {
		return nil, fmt.Errorf("CREATE token must be followed by TABLE, INDEX")
	}
	p.index++

	switch tokens[p.index].Token {
	case TableToken:
		// TableBody
		d, err := p.parseTable()
		if err != nil {
			return nil, err
		}
		createDecl.Add(d)
		// TableOption
		for p.index < len(tokens) && p.hasNext() {
			switch p.cur().Token {
			case EngineToken:
				engineDecl, err := p.consumeToken(EngineToken)
				if err != nil {
					return nil, err
				}
				if _, err = p.consumeToken(EqualityToken); err != nil {
					return nil, err
				}
				engineTypeDecl, err := p.consumeToken(StringToken)
				if err != nil {
					return nil, err
				}
				engineDecl.Add(engineTypeDecl)
				createDecl.Add(engineDecl)
			case DefaultToken:
				_, err = p.consumeToken(DefaultToken)
				if err != nil {
					return nil, err
				}
			case CharsetToken:
				charsetDecl, err := p.consumeToken(CharsetToken)
				if err != nil {
					return nil, err
				}
				if _, err = p.consumeToken(EqualityToken); err != nil {
					return nil, err
				}
				charsetTypeDecl, err := p.consumeToken(StringToken)
				if err != nil {
					return nil, err
				}
				charsetDecl.Add(charsetTypeDecl)
				createDecl.Add(charsetDecl)
			case AutoincrementToken:
				autoincrDecl, err := p.consumeToken(AutoincrementToken)
				if err != nil {
					return nil, err
				}
				if _, err = p.consumeToken(EqualityToken); err != nil {
					return nil, err
				}
				startValueDecl, err := p.consumeToken(NumberToken)
				if err != nil {
					return nil, err
				}
				autoincrDecl.Add(startValueDecl)
				createDecl.Add(autoincrDecl)
			case SemicolonToken:
				return st, nil
			default:
				return nil, p.syntaxError()
			}
		}
		break
	default:
		return nil, fmt.Errorf("parsing error near <%s>", tokens[p.index].Lexeme)
	}

	return st, nil
}

func (p *sqlParser) parseTable() (*Decl, error) {
	tokens := p.tokens
	var err error
	tableDecl := NewDecl(tokens[p.index])
	p.index++
	// if not exists
	if p.is(IfToken) {
		_, err := p.consumeToken(IfToken)
		if err != nil {
			return nil, err
		}
		if p.is(NotToken) {
			_, err := p.consumeToken(NotToken)
			if err != nil {
				return nil, err
			}
			if !p.is(ExistsToken) {
				return nil, p.syntaxError()
			}
			_, err = p.consumeToken(ExistsToken)
			if err != nil {
				return nil, err
			}
			tableDecl.Add(NewDecl(Token{
				Token:  IfNotExistsToken,
				Lexeme: "IfNotExists",
			}))
		}
	}
	// tableName
	tbNameDecl, err := p.parseAttribute()
	if err != nil {
		return nil, p.syntaxError()
	}
	tableDecl.Add(tbNameDecl)
	if !p.hasNext() || tokens[p.index].Token != BracketOpeningToken {
		return nil, fmt.Errorf("table name token must be followed by table definition")
	}
	p.index++
	// field constraint
	for p.index < len(tokens) {
		// constraint
		switch p.cur().Token {
		case PrimaryToken, KeyToken, UniqueToken:
			constrtDecl, err := p.parseConstraint()
			if err != nil {
				return nil, err
			}
			tableDecl.Add(constrtDecl)
			continue
		case CommaToken:
			p.index++
			continue
		default:
		}
		if tokens[p.index].Token == BracketClosingToken {
			_, _ = p.consumeToken(BracketClosingToken)
			break
		}
		// fieldName
		fieldDecl, err := p.parseQuotedToken()
		if err != nil {
			return nil, err
		}
		tbNameDecl.Add(fieldDecl)
		// fieldType
		fieldTypeDecl, err := p.parseType()
		if err != nil {
			return nil, err
		}
		fieldDecl.Add(fieldTypeDecl)
		// fieldOption
		for p.isNot(BracketClosingToken, CommaToken) {
			switch p.cur().Token {
			case UniqueToken:
				uniqueDecl, err := p.consumeToken(UniqueToken)
				if err != nil {
					return nil, err
				}
				fieldDecl.Add(uniqueDecl)
			case NotToken:
				if _, err = p.isNext(NullToken); err == nil {
					_, err := p.consumeToken(NotToken)
					if err != nil {
						return nil, err
					}
					_, err = p.consumeToken(NullToken)
					if err != nil {
						return nil, err
					}
					fieldDecl.Add(NewDecl(Token{
						Token:  NotNullToken,
						Lexeme: "NotNull",
					}))
				}
			case PrimaryToken:
				if _, err = p.isNext(KeyToken); err == nil {
					newPrimary := NewDecl(tokens[p.index])
					fieldDecl.Add(newPrimary)

					if err = p.next(); err != nil {
						return nil, fmt.Errorf("unexpected end")
					}

					newKey := NewDecl(tokens[p.index])
					newPrimary.Add(newKey)

					if err = p.next(); err != nil {
						return nil, fmt.Errorf("unexpected end")
					}
				}
			case AutoincrementToken:
				autoincDecl, err := p.consumeToken(AutoincrementToken)
				if err != nil {
					return nil, err
				}
				fieldDecl.Add(autoincDecl)
			case WithToken:
				if strings.ToLower(fieldTypeDecl.Lexeme) == "timestamp" {
					withDecl, err := p.consumeToken(WithToken)
					if err != nil {
						return nil, err
					}
					timeDecl, err := p.consumeToken(TimeToken)
					if err != nil {
						return nil, err
					}
					zoneDecl, err := p.consumeToken(ZoneToken)
					if err != nil {
						return nil, err
					}
					fieldTypeDecl.Add(withDecl)
					withDecl.Add(timeDecl)
					timeDecl.Add(zoneDecl)
				}
			case DefaultToken:
				dDecl, err := p.consumeToken(DefaultToken)
				if err != nil {
					return nil, err
				}
				var vDecl *Decl
				if p.is(SimpleQuoteToken) {
					if _, err = p.consumeToken(SimpleQuoteToken); err != nil {
						return nil, err
					}
					vDecl, err = p.consumeToken(StringToken)
					if err != nil {
						return nil, err
					}
					if _, err = p.consumeToken(SimpleQuoteToken); err != nil {
						return nil, err
					}
				} else {
					vDecl, err = p.consumeToken(FalseToken, TrueToken, NumberToken, StringToken, LocalTimestampToken)
					if err != nil {
						return nil, err
					}
				}
				dDecl.Add(vDecl)
				fieldDecl.Add(dDecl)
			case CommentToken:
				cmDecl, err := p.consumeToken(CommentToken)
				if err != nil {
					return nil, err
				}
				if _, err = p.consumeToken(SimpleQuoteToken); err != nil {
					return nil, err
				}
				vDecl, err := p.consumeToken(StringToken)
				if err != nil {
					return nil, err
				}
				if _, err = p.consumeToken(SimpleQuoteToken); err != nil {
					return nil, err
				}
				cmDecl.Lexeme = vDecl.Lexeme
				fieldDecl.Add(cmDecl)
			default:
				return nil, p.syntaxError()
			}
		}
		if p.is(CommaToken) {
			p.index++
			if p.is(BracketClosingToken) {
				p.index++
				break
			}
			if p.is(AnnotationToken) {
				annoDecl, err := p.consumeToken(AnnotationToken)
				if err != nil {
					return nil, err
				}
				fieldDecl.Add(annoDecl)
			}
			continue
		} else if p.is(BracketClosingToken) {
			p.index++
			break
		}
		p.index++
	}
	return tableDecl, nil
}

func (p *sqlParser) parseConstraint() (*Decl, error) {
	var (
		constrtDecl *Decl
		err         error
	)
	switch p.cur().Token {
	case PrimaryToken:
		_, err := p.consumeToken(PrimaryToken)
		if err != nil {
			return nil, err
		}
		_, err = p.consumeToken(KeyToken)
		if err != nil {
			return nil, err
		}
		constrtDecl = NewDecl(Token{
			Token: PrimaryKeyToken,
		})
	case UniqueToken:
		uniqDecl, err := p.consumeToken(UniqueToken)
		if err != nil {
			return nil, err
		}
		if p.is(KeyToken) {
			_, err := p.consumeToken(KeyToken)
			if err != nil {
				return nil, err
			}
			constrtDecl = NewDecl(Token{
				Token: UniqueKeyToken,
			})
		} else if p.is(IndexToken) {
			_, err := p.consumeToken(IndexToken)
			if err != nil {
				return nil, err
			}
			constrtDecl = NewDecl(Token{
				Token: UniqueIndexToken,
			})
		} else {
			constrtDecl = uniqDecl
		}
	case KeyToken:
		keyDecl, err := p.consumeToken(KeyToken)
		if err != nil {
			return nil, err
		}
		constrtDecl = keyDecl
	case IndexToken:
		idxDecl, err := p.consumeToken(IndexToken)
		if err != nil {
			return nil, err
		}
		constrtDecl = idxDecl
	default:
		return nil, p.syntaxError()
	}
	constrtDecl.Lexeme = ""
	if p.is(StringToken) {
		keyNameDecl, err := p.consumeToken(StringToken)
		if err != nil {
			return nil, err
		}
		constrtDecl.Lexeme = keyNameDecl.Lexeme
	}
	_, err = p.consumeToken(BracketOpeningToken)
	if err != nil {
		return nil, err
	}
	for {
		d, err := p.parseQuotedToken()
		if err != nil {
			return nil, err
		}
		constrtDecl.Add(d)
		d, err = p.consumeToken(CommaToken, BracketClosingToken)
		if err != nil {
			return nil, err
		}
		if d.Token == BracketClosingToken {
			break
		}
	}
	return constrtDecl, nil
}

func (p *sqlParser) parseIndexKey() (*Decl, error) {
	if p.is(UniqueToken) {
		if _, err := p.consumeToken(UniqueToken); err != nil {
			return nil, err
		}
	}
	idxDecl, err := p.consumeToken(KeyToken)
	if err != nil {
		return nil, err
	}
	if p.is(StringToken) {
		if _, err = p.consumeToken(StringToken); err != nil {
			return nil, err
		}
	}
	_, err = p.consumeToken(BracketOpeningToken)
	if err != nil {
		return nil, err
	}
	for {
		d, err := p.parseQuotedToken()
		if err != nil {
			return nil, err
		}
		d, err = p.consumeToken(CommaToken, BracketClosingToken)
		if err != nil {
			return nil, err
		}
		if d.Token == BracketClosingToken {
			break
		}
	}
	return idxDecl, nil
}

func (p *sqlParser) parseType() (*Decl, error) {
	if p.is(EnumToken) {
		return p.parseEnum()
	}
	typeDecl, err := p.consumeToken(StringToken)
	if err != nil {
		return nil, err
	}
	if p.is(BracketOpeningToken) {
		_, err = p.consumeToken(BracketOpeningToken)
		if err != nil {
			return nil, err
		}
		sizeDecl, err := p.consumeToken(NumberToken)
		if err != nil {
			return nil, err
		}
		typeDecl.Add(sizeDecl)
		_, err = p.consumeToken(BracketClosingToken)
		if err != nil {
			return nil, err
		}
	}
	if p.is(UnsignedToken, SignedToken) {
		signedDecl, err := p.consumeToken(UnsignedToken, SignedToken)
		if err != nil {
			return nil, err
		}
		typeDecl.Add(signedDecl)

	}
	return typeDecl, nil
}

func (p *sqlParser) parseEnum() (*Decl, error) {
	enumDecl, err := p.consumeToken(EnumToken)
	if err != nil {
		return nil, err
	}
	_, err = p.consumeToken(BracketOpeningToken)
	if err != nil {
		return nil, err
	}
	for {
		var d *Decl
		if p.is(NumberToken) {
			d, err = p.consumeToken(NumberToken)
			if err != nil {
				return nil, err
			}
		} else if p.is(TrueToken, FalseToken) {
			d, err = p.consumeToken(TrueToken, FalseToken)
			if err != nil {
				return nil, err
			}
		} else if p.is(SimpleQuoteToken) {
			if _, err = p.consumeToken(SimpleQuoteToken); err != nil {
				return nil, err
			}
			d, err = p.consumeToken(StringToken)
			if err != nil {
				return nil, err
			}
			if _, err = p.consumeToken(SimpleQuoteToken); err != nil {
				return nil, err
			}
		} else {
			return nil, p.syntaxError()
		}
		enumDecl.Add(d)
		d, err = p.consumeToken(CommaToken, BracketClosingToken)
		if err != nil {
			return nil, err
		}
		if d.Token == BracketClosingToken {
			break
		}
	}
	return enumDecl, nil
}

func (p *sqlParser) parseAttribute() (*Decl, error) {
	quoted := false
	quoteToken := DoubleQuoteToken

	if p.is(DoubleQuoteToken) || p.is(BacktickToken) {
		quoteToken = p.cur().Token
		quoted = true
		if err := p.next(); err != nil {
			return nil, err
		}
	}
	if !p.is(StringToken, StarToken) {
		return nil, p.syntaxError()
	}
	decl := NewDecl(p.cur())

	if quoted {
		if _, err := p.mustHaveNext(quoteToken); err != nil {
			return nil, err
		}
	}
	if err := p.next(); err != nil {
		return decl, nil
	}

	if p.is(PeriodToken) {
		_, err := p.consumeToken(PeriodToken)
		if err != nil {
			return nil, err
		}
		attributeDecl, err := p.consumeToken(StringToken, StarToken)
		if err != nil {
			return nil, err
		}
		attributeDecl.Add(decl)
		return attributeDecl, nil
	}
	return decl, nil
}

func (p *sqlParser) parseQuotedToken() (*Decl, error) {
	quoted := false
	quoteToken := DoubleQuoteToken

	if p.is(DoubleQuoteToken) || p.is(BacktickToken) {
		quoted = true
		quoteToken = p.cur().Token
		if err := p.next(); err != nil {
			return nil, err
		}
	}
	if !p.is(StringToken) {
		return nil, p.syntaxError()
	}
	decl := NewDecl(p.cur())

	if quoted {
		if _, err := p.mustHaveNext(quoteToken); err != nil {
			return nil, err
		}
	}
	_ = p.next()
	return decl, nil
}

func ParseDate(data string) (*time.Time, error) {
	t, err := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", data)
	if err == nil {
		return &t, nil
	}

	t, err = time.Parse(time.RFC3339, data)
	if err == nil {
		return &t, nil
	}

	t, err = time.Parse("2006-Jan-02", data)
	if err == nil {
		return &t, nil
	}

	t, err = time.Parse("2006-01-02", data)
	if err == nil {
		return &t, nil
	}

	return nil, fmt.Errorf("not a date")
}

func (p *sqlParser) next() error {
	if !p.hasNext() {
		return fmt.Errorf("unexpected end")
	}
	p.index++
	return nil
}

func (p *sqlParser) hasNext() bool {
	if p.index+1 < len(p.tokens) {
		return true
	}
	return false
}

func (p *sqlParser) is(tokenTypes ...int) bool {

	for _, tokenType := range tokenTypes {
		curToken := p.cur().Token
		if curToken == tokenType {
			return true
		}
	}

	return false
}

func (p *sqlParser) isNot(tokenTypes ...int) bool {
	return !p.is(tokenTypes...)
}

func (p *sqlParser) isNext(tokenTypes ...int) (t Token, err error) {

	if !p.hasNext() {
		return t, p.syntaxError()
	}
	for _, tokenType := range tokenTypes {
		if p.tokens[p.index+1].Token == tokenType {
			return p.tokens[p.index+1], nil
		}
	}
	return t, p.syntaxError()
}

func (p *sqlParser) mustHaveNext(tokenTypes ...int) (t Token, err error) {

	if !p.hasNext() {
		return t, p.syntaxError()
	}
	if err = p.next(); err != nil {
		return t, err
	}
	for _, tokenType := range tokenTypes {
		if p.is(tokenType) {
			return p.tokens[p.index], nil
		}
	}
	return t, p.syntaxError()
}

func (p *sqlParser) cur() Token {
	return p.tokens[p.index]
}

func (p *sqlParser) consumeToken(tokenTypes ...int) (*Decl, error) {
	if !p.is(tokenTypes...) {
		return nil, p.syntaxError()
	}
	decl := NewDecl(p.tokens[p.index])
	_ = p.next()
	return decl, nil
}

func (p *sqlParser) syntaxError() error {
	if p.index == 0 {
		return fmt.Errorf("syntax error near %v %v", p.tokens[p.index].Lexeme, p.tokens[p.index+1].Lexeme)
	} else if !p.hasNext() {
		return fmt.Errorf("syntax error near %v %v", p.tokens[p.index-1].Lexeme, p.tokens[p.index].Lexeme)
	}
	return fmt.Errorf("syntax error near %v %v %v", p.tokens[p.index-1].Lexeme, p.tokens[p.index].Lexeme, p.tokens[p.index+1].Lexeme)
}

func stripSpaces(t []Token) (ret []Token) {
	for i := range t {
		if t[i].Token != SpaceToken {
			ret = append(ret, t[i])
		}
	}
	return ret
}

func stripAnnotation(t []Token) (ret []Token) {
	for i := range t {
		if t[i].Token != AnnotationToken {
			ret = append(ret, t[i])
		}
	}
	return ret
}

// lexer
const (
	// Punctuation token
	SpaceToken = iota
	SemicolonToken
	CommaToken
	BracketOpeningToken
	BracketClosingToken
	BacktickToken
	// QuoteToken
	DoubleQuoteToken
	SimpleQuoteToken
	StarToken
	EqualityToken
	PeriodToken
	// First order Token
	AnnotationToken
	CreateToken
	EngineToken
	CharsetToken
	// Second order Token
	TableToken
	ValuesToken
	IfToken
	NotToken
	ExistsToken
	NullToken
	AutoincrementToken
	WithToken
	TimeToken
	ZoneToken
	DefaultToken
	LocalTimestampToken
	FalseToken
	TrueToken
	UniqueToken
	NowToken
	CommentToken
	// Type Token
	PrimaryToken
	KeyToken
	IndexToken
	EnumToken
	StringToken
	NumberToken
	DateToken
	SignedToken
	UnsignedToken
	// multi words Token
	NotNullToken
	IfNotExistsToken
	PrimaryKeyToken
	UniqueKeyToken
	UniqueIndexToken
)

type lexer struct {
	tokens         []Token
	instruction    []byte
	instructionLen int
	pos            int
}

func newLexer(sql string) *lexer {
	bSql := []byte(sql)
	return &lexer{
		tokens:         nil,
		instruction:    bSql,
		instructionLen: len(bSql),
		pos:            0,
	}
}

type Token struct {
	Token  int
	Lexeme string
	Pos    int
}

type Matcher func() bool

func (l *lexer) Parse() ([]Token, error) {
	securityPos := 0
	var matchers []Matcher
	// Punctuation Matcher
	matchers = append(matchers, l.MatchSpaceToken)
	matchers = append(matchers, l.MatchSemicolonToken)
	matchers = append(matchers, l.MatchCommaToken)
	matchers = append(matchers, l.MatchBracketOpeningToken)
	matchers = append(matchers, l.MatchBracketClosingToken)
	matchers = append(matchers, l.MatchStarToken)
	matchers = append(matchers, l.MatchSimpleQuoteToken)
	matchers = append(matchers, l.MatchEqualityToken)
	matchers = append(matchers, l.MatchPeriodToken)
	matchers = append(matchers, l.MatchDoubleQuoteToken)
	matchers = append(matchers, l.MatchBacktickToken)
	// First order Matcher
	matchers = append(matchers, l.MatchAnnotationToken)
	matchers = append(matchers, l.MatchCreateToken)
	matchers = append(matchers, l.MatchEngineToken)
	matchers = append(matchers, l.MatchCharsetToken)
	// Second order Matcher
	matchers = append(matchers, l.MatchTableToken)
	matchers = append(matchers, l.MatchValuesToken)
	matchers = append(matchers, l.MatchIfToken)
	matchers = append(matchers, l.MatchNotToken)
	matchers = append(matchers, l.MatchExistsToken)
	matchers = append(matchers, l.MatchNullToken)
	matchers = append(matchers, l.MatchAutoincrementToken)
	matchers = append(matchers, l.MatchWithToken)
	matchers = append(matchers, l.MatchTimeToken)
	matchers = append(matchers, l.MatchZoneToken)
	matchers = append(matchers, l.MatchDefaultToken)
	matchers = append(matchers, l.MatchLocalTimestampToken)
	matchers = append(matchers, l.MatchFalseToken)
	matchers = append(matchers, l.MatchTrueToken)
	matchers = append(matchers, l.MatchUniqueToken)
	matchers = append(matchers, l.MatchNowToken)
	matchers = append(matchers, l.MatchCommentToken)
	matchers = append(matchers, l.MatchSignedToken)
	matchers = append(matchers, l.MatchUnsignedToken)
	// Type Matcher
	matchers = append(matchers, l.MatchPrimaryToken)
	matchers = append(matchers, l.MatchKeyToken)
	matchers = append(matchers, l.MatchIndexToken)
	matchers = append(matchers, l.MatchEnumToken)
	matchers = append(matchers, l.MatchEscapedStringToken)
	matchers = append(matchers, l.MatchDateToken)
	matchers = append(matchers, l.MatchNumberToken)
	matchers = append(matchers, l.MatchStringToken)
	var r bool
	for l.pos < l.instructionLen {
		r = false
		for _, m := range matchers {
			if r = m(); r == true {
				securityPos = l.pos
				break
			}
		}
		if r {
			continue
		}
		if l.pos == securityPos {
			return nil, fmt.Errorf("cannot lex instruction. Syntax error near %s", l.instruction[l.pos:])
		}
		securityPos = l.pos
	}
	return l.tokens, nil
}

func (l *lexer) MatchSpaceToken() bool {
	if unicode.IsSpace(rune(l.instruction[l.pos])) {
		t := Token{
			Token:  SpaceToken,
			Lexeme: " ",
		}
		l.tokens = append(l.tokens, t)
		l.pos++
		return true
	}

	return false
}

func (l *lexer) MatchAnnotationToken() bool {
	if l.pos+len("--")-1 > l.instructionLen {
		return false
	}
	if l.instruction[l.pos] == '-' && l.instruction[l.pos+1] == '-' {
		t1 := Token{
			Token: AnnotationToken,
		}
		l.pos += len("--")
		i := l.pos
		for i < l.instructionLen && l.instruction[i] != '\n' {
			i++
		}
		t1.Lexeme = strings.TrimSpace(string(l.instruction[l.pos:i]))
		l.pos = i
		l.tokens = append(l.tokens, t1)
		l.pos = i + len("\n")
		return true
	}
	return false
}

func (l *lexer) MatchNowToken() bool {
	return l.Match([]byte("now()"), NowToken)
}

func (l *lexer) MatchUniqueToken() bool {
	return l.Match([]byte("unique"), UniqueToken)
}

func (l *lexer) MatchLocalTimestampToken() bool {
	return l.Match([]byte("localtimestamp"), LocalTimestampToken)
}

func (l *lexer) MatchDefaultToken() bool {
	return l.Match([]byte("default"), DefaultToken)
}

func (l *lexer) MatchFalseToken() bool {
	return l.Match([]byte("false"), FalseToken)
}

func (l *lexer) MatchTrueToken() bool {
	return l.Match([]byte("true"), TrueToken)
}

func (l *lexer) MatchWithToken() bool {
	return l.Match([]byte("with"), WithToken)
}

func (l *lexer) MatchTimeToken() bool {
	return l.Match([]byte("time"), TimeToken)
}

func (l *lexer) MatchZoneToken() bool {
	return l.Match([]byte("zone"), ZoneToken)
}

func (l *lexer) MatchCreateToken() bool {
	return l.Match([]byte("create"), CreateToken)
}

func (l *lexer) MatchTableToken() bool {
	return l.Match([]byte("table"), TableToken)
}

func (l *lexer) MatchNullToken() bool {
	return l.Match([]byte("null"), NullToken)
}

func (l *lexer) MatchIfToken() bool {
	return l.Match([]byte("if"), IfToken)
}

func (l *lexer) MatchNotToken() bool {
	return l.Match([]byte("not"), NotToken)
}

func (l *lexer) MatchExistsToken() bool {
	return l.Match([]byte("exists"), ExistsToken)
}

func (l *lexer) MatchAutoincrementToken() bool {
	return l.Match([]byte("auto_increment"), AutoincrementToken)
}

func (l *lexer) MatchPrimaryToken() bool {
	return l.Match([]byte("primary"), PrimaryToken)
}

func (l *lexer) MatchKeyToken() bool {
	return l.Match([]byte("key"), KeyToken)
}

func (l *lexer) MatchIndexToken() bool {
	return l.Match([]byte("index"), IndexToken)
}

func (l *lexer) MatchEnumToken() bool {
	return l.Match([]byte("enum"), EnumToken)
}

func (l *lexer) MatchValuesToken() bool {
	return l.Match([]byte("values"), ValuesToken)
}

func (l *lexer) MatchCommentToken() bool {
	return l.Match([]byte("comment"), CommentToken)
}

func (l *lexer) MatchStringToken() bool {
	i := l.pos
	for i < l.instructionLen &&
		(unicode.IsLetter(rune(l.instruction[i])) ||
			unicode.IsDigit(rune(l.instruction[i])) ||
			l.instruction[i] == '_' ||
			l.instruction[i] == '@' /* || l.instruction[i] == '.'*/) {
		i++
	}

	if i != l.pos {
		t := Token{
			Token:  StringToken,
			Lexeme: string(l.instruction[l.pos:i]),
		}
		l.tokens = append(l.tokens, t)
		l.pos = i
		return true
	}

	return false
}

func (l *lexer) MatchNumberToken() bool {
	i := l.pos
	for i < l.instructionLen && unicode.IsDigit(rune(l.instruction[i])) {
		i++
	}

	if i != l.pos {
		t := Token{
			Token:  NumberToken,
			Lexeme: string(l.instruction[l.pos:i]),
		}
		l.tokens = append(l.tokens, t)
		l.pos = i
		return true
	}

	return false
}

func (l *lexer) MatchSemicolonToken() bool {
	return l.MatchSingle(';', SemicolonToken)
}

func (l *lexer) MatchPeriodToken() bool {
	return l.MatchSingle('.', PeriodToken)
}

func (l *lexer) MatchBracketOpeningToken() bool {
	return l.MatchSingle('(', BracketOpeningToken)
}

func (l *lexer) MatchBracketClosingToken() bool {
	return l.MatchSingle(')', BracketClosingToken)
}

func (l *lexer) MatchCommaToken() bool {
	return l.MatchSingle(',', CommaToken)
}

func (l *lexer) MatchStarToken() bool {
	return l.MatchSingle('*', StarToken)
}

func (l *lexer) MatchEqualityToken() bool {
	return l.MatchSingle('=', EqualityToken)
}

func (l *lexer) MatchBacktickToken() bool {
	return l.MatchSingle('`', BacktickToken)
}

func (l *lexer) MatchEngineToken() bool {
	return l.Match([]byte("engine"), EngineToken)
}

func (l *lexer) MatchCharsetToken() bool {
	return l.Match([]byte("charset"), CharsetToken)
}

func (l *lexer) MatchUnsignedToken() bool {
	return l.Match([]byte("unsigned"), UnsignedToken)
}

func (l *lexer) MatchSignedToken() bool {
	return l.Match([]byte("signed"), SignedToken)
}

// 2015-09-10 14:03:09.444695269 +0200 CEST);
func (l *lexer) MatchDateToken() bool {
	startPos := l.pos
	i := l.pos
	for i < l.instructionLen &&
		l.instruction[i] != ',' &&
		l.instruction[i] != ')' {
		i++
	}

	data := string(l.instruction[l.pos:i])

	_, err := ParseDate(data)
	if err != nil {
		return false
	}

	t := Token{
		Token:  StringToken,
		Lexeme: data,
		Pos:    startPos,
	}

	l.tokens = append(l.tokens, t)
	l.pos = i
	return true
}

func (l *lexer) MatchDoubleQuoteToken() bool {
	startPos := l.pos
	if l.instruction[l.pos] == '"' {

		t := Token{
			Token:  DoubleQuoteToken,
			Lexeme: "\"",
			Pos:    startPos,
		}
		l.tokens = append(l.tokens, t)
		l.pos++
		startPos = l.pos
		if l.MatchDoubleQuotedStringToken() {
			t := Token{
				Token:  DoubleQuoteToken,
				Lexeme: "\"",
				Pos:    startPos,
			}
			l.tokens = append(l.tokens, t)
			l.pos++
			return true
		}

		return true
	}

	return false
}

func (l *lexer) MatchEscapedStringToken() bool {
	startPos := l.pos
	i := l.pos
	if l.instruction[i] != '$' || l.instruction[i+1] != '$' {
		return false
	}
	i += 2

	for i+1 < l.instructionLen && !(l.instruction[i] == '$' && l.instruction[i+1] == '$') {
		i++
	}
	i++

	if i == l.instructionLen {
		return false
	}

	tok := NumberToken
	escaped := l.instruction[l.pos+2 : i-1]

	for _, r := range escaped {
		if unicode.IsDigit(rune(r)) == false {
			tok = StringToken
		}
	}

	_, err := ParseDate(string(escaped))
	if err == nil {
		tok = DateToken
	}

	t := Token{
		Token:  tok,
		Lexeme: string(escaped),
		Pos:    startPos,
	}
	l.tokens = append(l.tokens, t)
	l.pos = i + 1

	return true
}

func (l *lexer) MatchDoubleQuotedStringToken() bool {
	startPos := l.pos
	i := l.pos
	for i < l.instructionLen && l.instruction[i] != '"' {
		i++
	}

	t := Token{
		Token:  StringToken,
		Lexeme: string(l.instruction[l.pos:i]),
		Pos:    startPos,
	}
	l.tokens = append(l.tokens, t)
	l.pos = i

	return true
}

func (l *lexer) MatchSimpleQuoteToken() bool {
	if l.instruction[l.pos] == '\'' {

		t := Token{
			Token:  SimpleQuoteToken,
			Lexeme: "'",
		}
		l.tokens = append(l.tokens, t)
		l.pos++

		if l.MatchSingleQuotedStringToken() {
			t := Token{
				Token:  SimpleQuoteToken,
				Lexeme: "'",
			}
			l.tokens = append(l.tokens, t)
			l.pos++
			return true
		}

		return true
	}

	return false
}

func (l *lexer) MatchSingleQuotedStringToken() bool {
	startPos := l.pos
	i := l.pos
	for i < l.instructionLen && l.instruction[i] != '\'' {
		i++
	}

	t := Token{
		Token:  StringToken,
		Lexeme: string(l.instruction[l.pos:i]),
		Pos:    startPos,
	}
	l.tokens = append(l.tokens, t)
	l.pos = i

	return true
}

func (l *lexer) MatchSingle(char byte, token int) bool {
	startPos := l.pos
	if l.pos > l.instructionLen {
		return false
	}

	if l.instruction[l.pos] != char {
		return false
	}

	t := Token{
		Token:  token,
		Lexeme: string(char),
		Pos:    startPos,
	}

	l.tokens = append(l.tokens, t)
	l.pos++
	return true
}

func (l *lexer) Match(str []byte, token int) bool {
	startPos := l.pos
	if l.pos+len(str)-1 > l.instructionLen {
		return false
	}
	for i := range str {
		if unicode.ToLower(rune(l.instruction[l.pos+i])) != unicode.ToLower(rune(str[i])) {
			return false
		}
	}
	if l.instructionLen > l.pos+len(str) {
		if unicode.IsLetter(rune(l.instruction[l.pos+len(str)])) ||
			l.instruction[l.pos+len(str)] == '_' {
			return false
		}
	}

	t := Token{
		Token:  token,
		Lexeme: string(str),
		Pos:    startPos,
	}

	l.tokens = append(l.tokens, t)
	l.pos += len(t.Lexeme)
	return true
}
