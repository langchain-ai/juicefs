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
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
)

func TestCalcObjectsHashPrefixFromSlice(t *testing.T) {
	keys := func(format meta.Format, id uint64) []string {
		var ks []string
		for _, o := range CalcObjects(format, id, 100<<10, 0, 100<<10) {
			ks = append(ks, o.Key)
		}
		return ks
	}
	format := meta.Format{Name: "vol", BlockSize: 64}
	require.Equal(t, []string{"vol/chunks/0/2/2001_0_65536", "vol/chunks/0/2/2001_1_36864"}, keys(format, 2001))

	format.HashPrefixFromSlice = 2000
	require.Equal(t, []string{"vol/chunks/0/1/1999_0_65536", "vol/chunks/0/1/1999_1_36864"}, keys(format, 1999))
	require.Equal(t, []string{"vol/chunks/D1/0/2001_0_65536", "vol/chunks/D1/0/2001_1_36864"}, keys(format, 2001))

	format = meta.Format{Name: "vol", BlockSize: 64, HashPrefix: true}
	require.Equal(t, []string{"vol/chunks/CF/0/1999_0_65536", "vol/chunks/CF/0/1999_1_36864"}, keys(format, 1999))
}
