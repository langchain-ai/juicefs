/*
 * JuiceFS, Copyright 2021 Juicedata, Inc.
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

package meta

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/stretchr/testify/assert"
)

func TestRemoveSecret(t *testing.T) {
	format := Format{Name: "test", SecretKey: "testSecret", EncryptKey: "testEncrypt", SessionToken: "token"}
	if err := format.Encrypt(); err != nil {
		t.Fatal(err)
	}

	format.RemoveSecret()
	if format.SecretKey != "removed" || format.EncryptKey != "removed" || format.SessionToken != "removed" {
		t.Fatalf("invalid format: %+v", format)
	}

	if err := format.Decrypt(); err != nil && !strings.Contains(err.Error(), "secret was removed") {
		t.Fatal(err)
	}
}

func TestEncrypt(t *testing.T) {
	cases := []struct {
		algo string
	}{
		{object.AES256GCM_RSA},
		{object.CHACHA20_RSA},
		{object.SM4GCM},
	}
	format := Format{Name: "test", SecretKey: "testSecret", SessionToken: "token", EncryptKey: "testEncrypt"}
	for _, c := range cases {
		format.EncryptAlgo = c.algo
		t.Run(c.algo, func(t *testing.T) {
			if err := format.Encrypt(); err != nil {
				t.Fatalf("Format encrypt: %s", err)
			}
			if format.SecretKey == "testSecret" || format.SessionToken == "token" || format.EncryptKey == "testEncrypt" {
				t.Fatalf("invalid format: %+v", format)
			}
			if err := format.Decrypt(); err != nil {
				t.Fatalf("Format decrypt: %s", err)
			}
			if format.SecretKey != "testSecret" || format.SessionToken != "token" || format.EncryptKey != "testEncrypt" {
				t.Fatalf("invalid format: %+v", format)
			}
		})
	}
}

func TestFormat_Update_KeyConflict(t *testing.T) {
	oldFormat := Format{Name: "test", UUID: "UUID-A"}

	newFormat := Format{Name: "test", UUID: "UUID-B", SecretKey: "secret"}
	if err := newFormat.Encrypt(); err != nil {
		t.Fatal(err)
	}
	assert.True(t, newFormat.KeyEncrypted)

	if err := newFormat.update(&oldFormat, false); err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "UUID-A", newFormat.UUID)
	assert.True(t, newFormat.KeyEncrypted)

	if err := newFormat.Decrypt(); err != nil {
		t.Fatalf("failed to decrypt with new UUID (which is old UUID A): %s", err)
	}

	assert.Equal(t, "secret", newFormat.SecretKey)
}

func TestFormatUpdateHashPrefixFromSlice(t *testing.T) {
	unset := Format{Name: "test", UUID: "UUID"}
	set := Format{Name: "test", UUID: "UUID", HashPrefixFromSlice: 2000}

	f := set
	assert.NoError(t, f.update(&unset, false), "setting it the first time")
	f = set
	assert.NoError(t, f.update(&set, false), "keeping it")
	for _, v := range []uint64{0, 1999, 2001} {
		f := Format{Name: "test", UUID: "UUID", HashPrefixFromSlice: v}
		assert.ErrorContains(t, f.update(&set, false), "hash prefix from slice", "changing it to %d", v)
		assert.NoError(t, f.update(&set, true), "forcing it to %d", v)
	}
}

func TestNextSliceID(t *testing.T) {
	// Two clients on one database (memkv gives each client its own store); the Redis rule is covered in cmd.
	for name, url := range map[string]string{
		"sqlite3": "sqlite3://" + filepath.Join(t.TempDir(), "next-slice-id.db"),
	} {
		t.Run(name, func(t *testing.T) {
			m := NewClient(url, nil)
			if err := m.Init(testFormat(), true); err != nil {
				t.Fatalf("initialize failed: %s", err)
			}
			defer m.Shutdown()
			if _, err := m.Load(true); err != nil {
				t.Fatalf("load: %s", err)
			}
			var first uint64
			if st := m.NewSlice(Background(), &first); st != 0 {
				t.Fatalf("new slice: %s", st)
			}
			next, err := NextSliceID(m)
			assert.NoError(t, err)
			// The client holds the whole batch that starts at first, and none of it may reach next.
			assert.Greater(t, next, first+SliceIDBatch-1)

			// The next batch starts at next (Redis) or one below it (SQL, TKV).
			m2 := NewClient(url, nil)
			defer m2.Shutdown()
			if _, err := m2.Load(true); err != nil {
				t.Fatalf("load: %s", err)
			}
			var second uint64
			if st := m2.NewSlice(Background(), &second); st != 0 {
				t.Fatalf("new slice: %s", st)
			}
			assert.GreaterOrEqual(t, second+1, next)
		})
	}
}
