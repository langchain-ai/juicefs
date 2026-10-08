/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package vfs

import (
	"go/ast"
	"go/parser"
	gotoken "go/token" // fill.go declares a type token
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// The epoch writer's fields live in structs embedded in upstream's sliceWriter, fileWriter and dataWriter
// (writer_epoch.go), so that each upstream struct carries one line for them. Go resolves a selector to the shallowest
// field or method, so if an upstream sync gave one of these structs a field or method with the name of an epoch field,
// the epoch writer could silently use upstream's instead of failing to build. For each field of sliceEpoch, fileEpoch
// and writerEpoch, this test checks two things. Through reflect, the name must resolve on the outer struct to that field
// of the embedded struct, which fails when a field of the outer struct shadows it, or a field promoted at the same
// depth makes it ambiguous. In the source of the package's non-test .go files (whatever their build constraints), no
// method with that name may be declared on the outer struct, with a value or pointer receiver: reflect lists exported
// methods only, and every epoch field is unexported. A method promoted from another embedded type is at the same depth
// as the field, and Go rejects that ambiguous selector where the field is used.
func TestEpochFieldsNotShadowed(t *testing.T) {
	methods := declaredMethods(t, "sliceWriter", "fileWriter", "dataWriter")
	for _, c := range []struct{ outer, inner reflect.Type }{
		{reflect.TypeOf((*sliceWriter)(nil)).Elem(), reflect.TypeOf((*sliceEpoch)(nil)).Elem()},
		{reflect.TypeOf((*fileWriter)(nil)).Elem(), reflect.TypeOf((*fileEpoch)(nil)).Elem()},
		{reflect.TypeOf((*dataWriter)(nil)).Elem(), reflect.TypeOf((*writerEpoch)(nil)).Elem()},
	} {
		if len(methods[c.outer.Name()]) == 0 {
			t.Fatalf("no method of %s found in the package's source: the scan is broken", c.outer.Name())
		}
		for i := 0; i < c.inner.NumField(); i++ {
			name := c.inner.Field(i).Name
			f, ok := c.outer.FieldByName(name)
			if !ok || len(f.Index) != 2 || c.outer.Field(f.Index[0]).Type != c.inner {
				t.Errorf("%s.%s does not resolve to the field of the embedded %s: an upstream field shadows it, or makes it ambiguous",
					c.outer.Name(), name, c.inner.Name())
			}
			if pos, ok := methods[c.outer.Name()][name]; ok {
				t.Errorf("%s: method %s.%s shadows the field of the embedded %s", pos, c.outer.Name(), name, c.inner.Name())
			}
		}
	}
}

// declaredMethods parses the non-test .go files of the package (the test's working directory), whatever their build
// constraints, and returns the methods declared on each of the named types, with a value or pointer receiver, and
// where each is declared.
func declaredMethods(t *testing.T, types ...string) map[string]map[string]gotoken.Position {
	t.Helper()
	methods := make(map[string]map[string]gotoken.Position, len(types))
	for _, typ := range types {
		methods[typ] = make(map[string]gotoken.Position)
	}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("list the package's files: %s", err)
	}
	fset := gotoken.NewFileSet()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %s", name, err)
		}
		for _, decl := range file.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Recv == nil || len(fd.Recv.List) == 0 {
				continue
			}
			if m, ok := methods[receiverTypeName(fd.Recv.List[0].Type)]; ok {
				m[fd.Name.Name] = fset.Position(fd.Name.Pos())
			}
		}
	}
	return methods
}

// receiverTypeName returns the name of the type of a method's receiver, T for T, *T or (*T); "" for anything else.
func receiverTypeName(expr ast.Expr) string {
	for {
		switch e := expr.(type) {
		case *ast.StarExpr:
			expr = e.X
		case *ast.ParenExpr:
			expr = e.X
		case *ast.Ident:
			return e.Name
		default:
			return ""
		}
	}
}
