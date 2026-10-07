/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
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
	"reflect"
	"testing"
)

// The epoch writer's fields live in structs embedded in upstream's sliceWriter, fileWriter and dataWriter
// (writer_epoch.go), so that each upstream struct carries one line for them. Go resolves a selector to the shallowest
// field or method, so if an upstream sync added a field or method with the name of an epoch field, the epoch writer
// would silently use upstream's instead of failing to build. This test fails instead.
func TestEpochFieldsNotShadowed(t *testing.T) {
	for _, c := range []struct{ outer, inner reflect.Type }{
		{reflect.TypeOf((*sliceWriter)(nil)).Elem(), reflect.TypeOf((*sliceEpoch)(nil)).Elem()},
		{reflect.TypeOf((*fileWriter)(nil)).Elem(), reflect.TypeOf((*fileEpoch)(nil)).Elem()},
		{reflect.TypeOf((*dataWriter)(nil)).Elem(), reflect.TypeOf((*writerEpoch)(nil)).Elem()},
	} {
		for i := 0; i < c.inner.NumField(); i++ {
			name := c.inner.Field(i).Name
			f, ok := c.outer.FieldByName(name)
			if !ok || len(f.Index) != 2 || c.outer.Field(f.Index[0]).Type != c.inner {
				t.Errorf("%s.%s does not resolve to the field of the embedded %s: an upstream field shadows it, or makes it ambiguous",
					c.outer.Name(), name, c.inner.Name())
			}
			if _, ok := reflect.PointerTo(c.outer).MethodByName(name); ok {
				t.Errorf("*%s has a method named %s, which shadows the field of the embedded %s", c.outer.Name(), name, c.inner.Name())
			}
		}
	}
}
