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
	"context"
	"encoding/base64"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	entraid "github.com/redis/go-redis-entraid"
	"github.com/redis/go-redis-entraid/manager"
	"github.com/redis/go-redis-entraid/shared"
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

func TestAzureRedisCredentialsOptions(t *testing.T) {
	opts := azureRedisCredentialsOptions().TokenManagerOptions
	retry := opts.RetryOptions
	require.Equal(t, math.MaxInt, retry.MaxAttempts)
	require.Equal(t, time.Second, retry.InitialDelay)
	require.Equal(t, 30*time.Second, retry.MaxDelay)
	require.Equal(t, 2.0, retry.BackoffMultiplier)
	require.NotNil(t, retry.IsRetryable)
	// The library default gives up on errors that are not network timeouts.
	require.True(t, retry.IsRetryable(errors.New("AADSTS700024: client assertion is not within its valid time range")))
	require.True(t, retry.IsRetryable(context.DeadlineExceeded))
	// Zero keeps the library default ratio (0.7).
	require.Zero(t, opts.ExpirationRefreshRatio)
}

// scriptedIdentityProvider returns tokens or errors from a script, then
// repeats the last entry.
type scriptedIdentityProvider struct {
	mu    sync.Mutex
	steps []func() (shared.IdentityProviderResponse, error)
	calls int
}

func (p *scriptedIdentityProvider) RequestToken(context.Context) (shared.IdentityProviderResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	i := p.calls
	if i >= len(p.steps) {
		i = len(p.steps) - 1
	}
	p.calls++
	return p.steps[i]()
}

func fakeEntraToken(t *testing.T, oid string, ttl time.Duration) func() (shared.IdentityProviderResponse, error) {
	enc := base64.RawURLEncoding.EncodeToString
	raw := enc([]byte(`{"alg":"HS256","typ":"JWT"}`)) + "." + enc([]byte(`{"oid":"`+oid+`"}`)) + ".sig"
	return func() (shared.IdentityProviderResponse, error) {
		resp, err := shared.NewIDPResponse(shared.ResponseTypeAccessToken, azcore.AccessToken{Token: raw, ExpiresOn: time.Now().Add(ttl)})
		require.NoError(t, err)
		return resp, nil
	}
}

type recordingListener struct {
	next chan auth.Credentials
	errs chan error
}

func (l *recordingListener) OnNext(c auth.Credentials) { l.next <- c }
func (l *recordingListener) OnError(err error)         { l.errs <- err }

// TestAzureRedisRefreshSurvivesFailures runs the library token manager with
// our retry options and a fake identity provider. Six non-timeout failures
// in a row would stop the loop with the library defaults; with our options
// the loop keeps retrying and pushes the next good token.
func TestAzureRedisRefreshSurvivesFailures(t *testing.T) {
	failure := func() (shared.IdentityProviderResponse, error) {
		return nil, errors.New("AADSTS700024: client assertion is not within its valid time range")
	}
	idp := &scriptedIdentityProvider{steps: []func() (shared.IdentityProviderResponse, error){
		fakeEntraToken(t, "first-oid", 10*time.Second),
		failure, failure, failure, failure, failure, failure,
		fakeEntraToken(t, "second-oid", time.Hour),
	}}

	opts := azureRedisTokenManagerOptions()
	// Refresh the first token after ~1% of its lifetime and retry fast; keep
	// IsRetryable and MaxAttempts as configured.
	opts.ExpirationRefreshRatio = 0.01
	opts.RetryOptions.InitialDelay = time.Millisecond
	opts.RetryOptions.MaxDelay = 5 * time.Millisecond
	tm, err := manager.NewTokenManager(idp, opts)
	require.NoError(t, err)
	cp, err := entraid.NewCredentialsProvider(tm, entraid.CredentialsProviderOptions{TokenManagerOptions: opts})
	require.NoError(t, err)

	l := &recordingListener{next: make(chan auth.Credentials, 8), errs: make(chan error, 8)}
	creds, unsubscribe, err := cp.Subscribe(l)
	require.NoError(t, err)
	defer func() { _ = unsubscribe() }()
	user, _ := creds.BasicAuth()
	require.Equal(t, "first-oid", user)

	select {
	case c := <-l.next:
		user, _ := c.BasicAuth()
		require.Equal(t, "second-oid", user)
	case err := <-l.errs:
		t.Fatalf("refresh loop gave up: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("refresh loop did not recover")
	}
	idp.mu.Lock()
	require.GreaterOrEqual(t, idp.calls, 8)
	idp.mu.Unlock()
}
