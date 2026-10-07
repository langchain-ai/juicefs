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
	"fmt"
	"time"

	entraid "github.com/redis/go-redis-entraid"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/auth"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// redisCredentials holds the credentials for an `auth-provider`. Azure
// reauthenticates open connections before each token expires. GCP authenticates
// each new connection, since Memorystore keeps a connection authenticated after
// its token expires.
type redisCredentials struct {
	streaming auth.StreamingCredentialsProvider
	perConn   func(ctx context.Context) (username string, password string, err error)
}

type redisCredentialFactories struct {
	azure func() (auth.StreamingCredentialsProvider, error)
	gcp   func() (func(ctx context.Context) (string, string, error), error)
}

var defaultRedisCredentialFactories = redisCredentialFactories{
	azure: newAzureRedisCredentials,
	gcp:   newGCPRedisCredentials,
}

// resolveRedisCredentials resolves the `auth-provider` metadata URL option.
// An empty value keeps static password authentication.
func resolveRedisCredentials(authProvider string, opt *redis.Options, factories redisCredentialFactories) (redisCredentials, error) {
	switch authProvider {
	case "":
		return redisCredentials{}, nil
	case "azure":
		if opt.Password != "" {
			return redisCredentials{}, fmt.Errorf("redis auth-provider=azure cannot be combined with a static password")
		}
		// Entra tokens are bearer credentials; never send them without TLS.
		if opt.TLSConfig == nil {
			return redisCredentials{}, fmt.Errorf("redis auth-provider=azure requires a rediss:// metadata URL")
		}
		creds, err := factories.azure()
		if err != nil {
			return redisCredentials{}, fmt.Errorf("create azure redis credentials provider: %w", err)
		}
		return redisCredentials{streaming: creds}, nil
	case "gcp":
		if opt.Password != "" {
			return redisCredentials{}, fmt.Errorf("redis auth-provider=gcp cannot be combined with a static password")
		}
		creds, err := factories.gcp()
		if err != nil {
			return redisCredentials{}, fmt.Errorf("create gcp redis credentials provider: %w", err)
		}
		return redisCredentials{perConn: creds}, nil
	default:
		return redisCredentials{}, fmt.Errorf("unsupported redis auth-provider %q", authProvider)
	}
}

// newAzureRedisCredentials uses the default Azure credential chain, which covers
// AKS workload identity and managed identity.
func newAzureRedisCredentials() (auth.StreamingCredentialsProvider, error) {
	return entraid.NewDefaultAzureCredentialsProvider(entraid.DefaultAzureCredentialsProviderOptions{})
}

const (
	// Memorystore IAM authentication logs in as `default` with an access token.
	gcpRedisUsername = "default"
	// A token is refreshed this long before it expires, so a new connection
	// never authenticates with one about to lapse.
	gcpRedisTokenRefreshBuffer = 5 * time.Minute
)

// newGCPRedisCredentials uses Application Default Credentials, which covers GKE
// workload identity and the node service account.
func newGCPRedisCredentials() (func(ctx context.Context) (string, string, error), error) {
	creds, err := google.FindDefaultCredentials(context.Background(), "https://www.googleapis.com/auth/cloud-platform")
	if err != nil {
		return nil, err
	}
	tokens := oauth2.ReuseTokenSourceWithExpiry(nil, creds.TokenSource, gcpRedisTokenRefreshBuffer)
	return gcpRedisCredentials(tokens), nil
}

func gcpRedisCredentials(tokens oauth2.TokenSource) func(ctx context.Context) (string, string, error) {
	return func(context.Context) (string, string, error) {
		token, err := tokens.Token()
		if err != nil {
			return "", "", fmt.Errorf("get gcp access token for redis: %w", err)
		}
		return gcpRedisUsername, token.AccessToken, nil
	}
}
