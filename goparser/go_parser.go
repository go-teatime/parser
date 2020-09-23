package goparser

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/ioutil"
	"os"
	"strings"
)

type GoCodeInfo struct {
	PkgName string     // 包名
	Imports []string   // import的包
	Structs []GoStruct // 定义的结构体
	Funcs   []GoFunc   // 定义的函数
}

type GoStruct struct {
	Name                   string
	Type                   string
	FieldNames, FieldTypes []string
}

type GoFunc struct {
	Name                   string
	RecvName, RecvType     string
	ParamNames, ParamTypes []string
	RetTypes               []string
}

func (f GoFunc) String(funcHead bool) string {
	var bs []string
	if funcHead {
		bs = append(bs, "func")
	}
	if f.RecvType != "" {
		recvBlock := f.RecvType
		if f.RecvName != "" {
			recvBlock = f.RecvName + " " + recvBlock
		}
		bs = append(bs, fmt.Sprintf("(%s)", recvBlock))
	}
	composeFn := func(names, types []string) string {
		if len(names) == 0 && len(types) == 0 {
			return ""
		}
		if len(names) == 0 && len(types) > 0 {
			return strings.Join(types, ", ")
		}
		ts := make([]string, len(types))
		copy(ts, types)
		var lastType string
		for i := len(ts) - 1; i >= 0; i-- {
			if ts[i] == lastType {
				ts[i] = ""
			} else {
				lastType = ts[i]
			}
		}
		params := make([]string, 0)
		for i, _ := range names {
			params = append(params, fmt.Sprintf("%s %s", strings.ToLower(names[i][:1])+names[i][1:], ts[i]))
		}
		return strings.Join(params, ",")
	}
	bs = append(bs, fmt.Sprintf("%s(%s)", f.Name, composeFn(f.ParamNames, f.ParamTypes)))
	if len(f.RetTypes) > 0 {
		if len(f.RetTypes) == 1 {
			bs = append(bs, f.RetTypes[0])
		} else {
			bs = append(bs, fmt.Sprintf("(%s)", strings.Join(f.RetTypes, ", ")))
		}
	}
	return strings.Join(bs, " ")
}

func ParseGo(goCode string) (info GoCodeInfo, err error) {
	goCodeBuf := []byte(goCode)
	file, err := ioutil.TempFile("", "tmpfile")
	if err != nil {
		panic(err)
	}
	defer func() { _ = os.Remove(file.Name()) }()
	if _, err := file.Write([]byte(goCode)); err != nil {
		panic(err)
	}
	f, err := parser.ParseFile(token.NewFileSet(), file.Name(), nil, 0)
	if err != nil {
		return GoCodeInfo{}, err
	}
	gofile := GoCodeInfo{
		Imports: make([]string, 0),
		Structs: make([]GoStruct, 0),
		Funcs:   make([]GoFunc, 0),
	}
	ast.Inspect(f, func(node ast.Node) bool {
		nodeStr := func(node ast.Node) string {
			return string(goCodeBuf[node.Pos()-1 : node.End()-1])
		}
		// package
		if impSp, ok := node.(*ast.ImportSpec); ok {
			gofile.Imports = append(gofile.Imports, impSp.Path.Value)
			return true
		}
		// import
		if af, ok := node.(*ast.File); ok {
			gofile.PkgName = af.Name.Name
			return true
		}
		// type struct
		if gd, ok := node.(*ast.GenDecl); ok && gd.Tok == token.TYPE {
			if ts, ok1 := gd.Specs[0].(*ast.TypeSpec); ok1 {
				gs := GoStruct{
					Name: ts.Name.Name,
				}
				if st, ok := ts.Type.(*ast.StructType); ok {
					gs.Type = "struct"
					pNames, pTypes := make([]string, 0), make([]string, 0)
					for _, f := range st.Fields.List {
						for _, name := range f.Names {
							pNames = append(pNames, nodeStr(name))
							pTypes = append(pTypes, nodeStr(f.Type))
						}
					}
					gs.FieldNames = pNames
					gs.FieldTypes = pTypes
				} else {
					gs.Type = nodeStr(ts.Type)
				}
				gofile.Structs = append(gofile.Structs, gs)
			}
			return true
		}
		// functions
		if fd, ok := node.(*ast.FuncDecl); ok {
			sf := GoFunc{}
			sf.Name = fd.Name.Name
			// recv
			if fd.Recv != nil && len(fd.Recv.List) > 0 {
				if len(fd.Recv.List[0].Names) > 0 {
					sf.RecvName = fd.Recv.List[0].Names[0].String()
				}
				sf.RecvType = nodeStr(fd.Recv.List[0].Type)
			}
			// results
			if fd.Type.Results != nil && len(fd.Type.Results.List) > 0 {
				rts := make([]string, 0)
				for _, tr := range fd.Type.Results.List {
					rts = append(rts, nodeStr(tr.Type))
				}
				sf.RetTypes = rts
			}
			// params
			if fd.Type.Params != nil && len(fd.Type.Params.List) > 0 {
				pNames, pTypes := make([]string, 0), make([]string, 0)
				for _, p := range fd.Type.Params.List {
					pType := nodeStr(p.Type)
					for _, pName := range p.Names {
						pNames = append(pNames, pName.Name)
						pTypes = append(pTypes, pType)
					}
				}
				sf.ParamNames = pNames
				sf.ParamTypes = pTypes
			}
			gofile.Funcs = append(gofile.Funcs, sf)
			return true
		}
		return true
	})
	return gofile, nil
}
