//go:build !noredis
// +build !noredis

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

package meta

import (
	"errors"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/auth"
	"github.com/stretchr/testify/require"
)

type stubStreamingCredentials struct{}

func (stubStreamingCredentials) Subscribe(auth.CredentialsListener) (auth.Credentials, auth.UnsubscribeFunc, error) {
	return nil, func() error { return nil }, nil
}

func TestRedisStreamingCredentials(t *testing.T) {
	stub := stubStreamingCredentials{}
	newStub := func() (auth.StreamingCredentialsProvider, error) { return stub, nil }
	mustParse := func(t *testing.T, uri string) *redis.Options {
		t.Helper()
		opt, err := redis.ParseURL(uri)
		require.NoError(t, err)
		return opt
	}

	cases := []struct {
		name     string
		provider string
		uri      string
		factory  func() (auth.StreamingCredentialsProvider, error)
		want     auth.StreamingCredentialsProvider
		wantErr  string
	}{
		{name: "unset keeps static password", uri: "redis://:secret@localhost:6379/1"},
		{name: "azure over tls", provider: "azure", uri: "rediss://oid@localhost:10000/1", want: stub},
		{name: "azure rejects plaintext", provider: "azure", uri: "redis://localhost:6379/1", wantErr: "requires a rediss://"},
		{name: "azure rejects static password", provider: "azure", uri: "rediss://oid:secret@localhost:10000/1", wantErr: "cannot be combined with a static password"},
		{name: "unknown provider", provider: "aws", uri: "rediss://localhost:10000/1", wantErr: `unsupported redis auth-provider "aws"`},
		{
			name:     "factory error",
			provider: "azure",
			uri:      "rediss://localhost:10000/1",
			factory:  func() (auth.StreamingCredentialsProvider, error) { return nil, errors.New("boom") },
			wantErr:  "create azure redis credentials provider: boom",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			factory := tc.factory
			if factory == nil {
				factory = newStub
			}
			got, err := redisStreamingCredentials(tc.provider, mustParse(t, tc.uri), factory)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestNewRedisMetaRejectsInvalidAuthProvider(t *testing.T) {
	_, err := newRedisMeta("redis", "localhost:6379/1?auth-provider=azure", testConfig())
	require.ErrorContains(t, err, "requires a rediss://")
}
