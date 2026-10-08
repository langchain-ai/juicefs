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
	"errors"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/auth"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

type stubStreamingCredentials struct{}

func (stubStreamingCredentials) Subscribe(auth.CredentialsListener) (auth.Credentials, auth.UnsubscribeFunc, error) {
	return nil, func() error { return nil }, nil
}

func TestResolveRedisCredentials(t *testing.T) {
	stub := stubStreamingCredentials{}
	perConn := func(context.Context) (string, string, error) { return "default", "token", nil }
	factories := redisCredentialFactories{
		azure: func() (auth.StreamingCredentialsProvider, error) { return stub, nil },
		gcp:   func() (func(context.Context) (string, string, error), error) { return perConn, nil },
		aws:   func(string, string) (func(context.Context) (string, string, error), error) { return perConn, nil },
	}
	mustParse := func(t *testing.T, uri string) *redis.Options {
		t.Helper()
		opt, err := redis.ParseURL(uri)
		require.NoError(t, err)
		return opt
	}

	cases := []struct {
		name          string
		provider      string
		uri           string
		factories     *redisCredentialFactories
		wantStreaming auth.StreamingCredentialsProvider
		wantPerConn   bool
		wantErr       string
	}{
		{name: "unset keeps static password", uri: "redis://:secret@localhost:6379/1"},
		{name: "azure over tls", provider: "azure", uri: "rediss://oid@localhost:10000/1", wantStreaming: stub},
		{name: "azure rejects plaintext", provider: "azure", uri: "redis://localhost:6379/1", wantErr: "requires a rediss://"},
		{name: "azure rejects static password", provider: "azure", uri: "rediss://oid:secret@localhost:10000/1", wantErr: "cannot be combined with a static password"},
		{name: "gcp over plaintext", provider: "gcp", uri: "redis://localhost:6379/1", wantPerConn: true},
		{name: "gcp over tls", provider: "gcp", uri: "rediss://localhost:6378/1", wantPerConn: true},
		{name: "gcp rejects static password", provider: "gcp", uri: "redis://:secret@localhost:6379/1", wantErr: "auth-provider=gcp cannot be combined with a static password"},
		{name: "aws over tls", provider: "aws", uri: "rediss://iam-user@localhost:6379/1", wantPerConn: true},
		{name: "aws rejects plaintext", provider: "aws", uri: "redis://iam-user@localhost:6379/1", wantErr: "requires a verified rediss://"},
		{name: "aws rejects missing username", provider: "aws", uri: "rediss://localhost:6379/1", wantErr: "requires an IAM username"},
		{name: "aws rejects static password", provider: "aws", uri: "rediss://iam-user:secret@localhost:6379/1", wantErr: "cannot be combined with a static password"},
		{name: "unknown provider", provider: "unknown", uri: "rediss://localhost:10000/1", wantErr: `unsupported redis auth-provider "unknown"`},
		{
			name:      "azure factory error",
			provider:  "azure",
			uri:       "rediss://localhost:10000/1",
			factories: &redisCredentialFactories{azure: func() (auth.StreamingCredentialsProvider, error) { return nil, errors.New("boom") }},
			wantErr:   "create azure redis credentials provider: boom",
		},
		{
			name:      "gcp factory error",
			provider:  "gcp",
			uri:       "redis://localhost:6379/1",
			factories: &redisCredentialFactories{gcp: func() (func(context.Context) (string, string, error), error) { return nil, errors.New("no adc") }},
			wantErr:   "create gcp redis credentials provider: no adc",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := factories
			if tc.factories != nil {
				f = *tc.factories
			}
			got, err := resolveRedisCredentials(tc.provider, mustParse(t, tc.uri), f)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantStreaming, got.streaming)
			require.Equal(t, tc.wantPerConn, got.perConn != nil)
		})
	}
}

type countingTokenSource struct {
	calls  int
	expiry time.Time
	err    error
}

func (s *countingTokenSource) Token() (*oauth2.Token, error) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	return &oauth2.Token{AccessToken: "token-" + string(rune('0'+s.calls)), Expiry: s.expiry}, nil
}

func TestGCPRedisCredentials(t *testing.T) {
	t.Run("logs in as default with the access token", func(t *testing.T) {
		creds := gcpRedisCredentials(&countingTokenSource{expiry: time.Now().Add(time.Hour)})
		username, password, err := creds(context.Background())
		require.NoError(t, err)
		require.Equal(t, "default", username)
		require.Equal(t, "token-1", password)
	})

	t.Run("reuses a token until the refresh buffer", func(t *testing.T) {
		source := &countingTokenSource{expiry: time.Now().Add(time.Hour)}
		creds := gcpRedisCredentials(oauth2.ReuseTokenSourceWithExpiry(nil, source, gcpRedisTokenRefreshBuffer))
		for range 3 {
			_, password, err := creds(context.Background())
			require.NoError(t, err)
			require.Equal(t, "token-1", password)
		}
		require.Equal(t, 1, source.calls)
	})

	t.Run("refreshes a token inside the refresh buffer", func(t *testing.T) {
		source := &countingTokenSource{expiry: time.Now().Add(gcpRedisTokenRefreshBuffer / 2)}
		creds := gcpRedisCredentials(oauth2.ReuseTokenSourceWithExpiry(nil, source, gcpRedisTokenRefreshBuffer))
		_, first, err := creds(context.Background())
		require.NoError(t, err)
		_, second, err := creds(context.Background())
		require.NoError(t, err)
		require.NotEqual(t, first, second)
		require.Equal(t, 2, source.calls)
	})

	t.Run("token errors fail the connection", func(t *testing.T) {
		creds := gcpRedisCredentials(&countingTokenSource{err: errors.New("metadata server unavailable")})
		_, _, err := creds(context.Background())
		require.ErrorContains(t, err, "get gcp access token for redis: metadata server unavailable")
	})
}

func TestNewRedisMetaRejectsInvalidAuthProvider(t *testing.T) {
	_, err := newRedisMeta("redis", "localhost:6379/1?auth-provider=azure", testConfig())
	require.ErrorContains(t, err, "requires a rediss://")

	_, err = newRedisMeta("redis", ":secret@localhost:6379/1?auth-provider=gcp", testConfig())
	require.ErrorContains(t, err, "auth-provider=gcp cannot be combined with a static password")
}

func TestAWSRedisCredentials(t *testing.T) {
	cfg := aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("test-key", "test-secret", "test-session")}
	provider := awsRedisCredentials(cfg, "my-cache", "iam-user")
	username, token, err := provider(context.Background())
	require.NoError(t, err)
	require.Equal(t, "iam-user", username)
	u, err := url.Parse("http://" + token)
	require.NoError(t, err)
	require.Equal(t, "my-cache", u.Host)
	require.Equal(t, "connect", u.Query().Get("Action"))
	require.Equal(t, username, u.Query().Get("User"))
	require.Equal(t, "900", u.Query().Get("X-Amz-Expires"))
	require.Equal(t, "test-session", u.Query().Get("X-Amz-Security-Token"))
	require.Contains(t, u.Query().Get("X-Amz-Credential"), "/us-east-1/elasticache/aws4_request")
	require.NotEmpty(t, u.Query().Get("X-Amz-Signature"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cfg.Credentials = aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) { return aws.Credentials{}, ctx.Err() })
	_, _, err = awsRedisCredentials(cfg, "my-cache", "iam-user")(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestAWSRedisConnectionLifetime(t *testing.T) {
	factories := redisCredentialFactories{aws: func(string, string) (func(context.Context) (string, string, error), error) {
		return func(context.Context) (string, string, error) { return "iam-user", "token", nil }, nil
	}}
	for _, lifetime := range []time.Duration{0, 13 * time.Hour, time.Hour} {
		opt, err := redis.ParseURL("rediss://iam-user@localhost:6379")
		require.NoError(t, err)
		opt.ConnMaxLifetime = lifetime
		_, err = resolveRedisCredentials("aws", opt, factories)
		require.NoError(t, err)
		expected := lifetime
		if lifetime == 0 || lifetime > 11*time.Hour {
			expected = 11 * time.Hour
		}
		require.Equal(t, expected, opt.ConnMaxLifetime)
	}
}

func TestNewAWSRedisCredentialsRejectsUnsupportedEndpoints(t *testing.T) {
	for _, addr := range []string{"localhost:6379", "cache.example.com:6379", "cache.serverless.use1.cache.amazonaws.com:6379", "master.cache.example.use1.cache.amazonaws.com,other:6379", "invalid"} {
		_, err := newAWSRedisCredentials(addr, "iam-user")
		require.Error(t, err)
	}
}

func TestAWSRedisCredentialsRejectsInsecureTLS(t *testing.T) {
	opt, err := redis.ParseURL("rediss://iam-user@localhost:6379")
	require.NoError(t, err)
	opt.TLSConfig.InsecureSkipVerify = true
	_, err = resolveRedisCredentials("aws", opt, redisCredentialFactories{})
	require.ErrorContains(t, err, "requires a verified rediss://")
}
