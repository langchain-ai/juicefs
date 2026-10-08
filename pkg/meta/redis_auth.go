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
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"

	entraid "github.com/redis/go-redis-entraid"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/auth"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// redisCredentials holds streaming Azure or per-connection GCP and AWS credentials.
type redisCredentials struct {
	streaming auth.StreamingCredentialsProvider
	perConn   func(ctx context.Context) (username string, password string, err error)
}

type redisCredentialFactories struct {
	azure func() (auth.StreamingCredentialsProvider, error)
	gcp   func() (func(ctx context.Context) (string, string, error), error)
	aws   func(string, string) (func(ctx context.Context) (string, string, error), error)
}

var defaultRedisCredentialFactories = redisCredentialFactories{
	azure: newAzureRedisCredentials,
	gcp:   newGCPRedisCredentials,
	aws:   newAWSRedisCredentials,
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
	case "aws":
		if opt.Password != "" {
			return redisCredentials{}, fmt.Errorf("redis auth-provider=aws cannot be combined with a static password")
		}
		if opt.TLSConfig == nil || opt.TLSConfig.InsecureSkipVerify {
			return redisCredentials{}, fmt.Errorf("redis auth-provider=aws requires a verified rediss:// metadata URL")
		}
		if opt.Username == "" {
			return redisCredentials{}, fmt.Errorf("redis auth-provider=aws requires an IAM username in the metadata URL")
		}
		creds, err := factories.aws(opt.Addr, opt.Username)
		if err != nil {
			return redisCredentials{}, fmt.Errorf("create aws redis credentials provider: %w", err)
		}
		if opt.ConnMaxLifetime <= 0 || opt.ConnMaxLifetime > 11*time.Hour {
			opt.ConnMaxLifetime = 11 * time.Hour
		}
		return redisCredentials{perConn: creds}, nil
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

func newAWSRedisCredentials(addr, username string) (func(context.Context) (string, string, error), error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("invalid AWS Redis endpoint")
	}
	labels := strings.Split(strings.ToLower(host), ".")
	if len(labels) < 3 || !strings.HasSuffix(strings.ToLower(host), ".cache.amazonaws.com") || strings.Contains(host, ".serverless.") || strings.Contains(host, ",") {
		return nil, fmt.Errorf("aws redis credentials require a provisioned ElastiCache endpoint")
	}
	cacheName := labels[0]
	switch cacheName {
	case "master", "replica", "clustercfg":
		cacheName = labels[1]
	}
	cfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, err
	}
	if cfg.Region == "" {
		return nil, fmt.Errorf("AWS region is required")
	}
	return awsRedisCredentials(cfg, cacheName, username), nil
}

func awsRedisCredentials(cfg aws.Config, cacheName, username string) func(context.Context) (string, string, error) {
	signer := v4.NewSigner()
	return func(ctx context.Context) (string, string, error) {
		creds, err := cfg.Credentials.Retrieve(ctx)
		if err != nil {
			return "", "", fmt.Errorf("retrieve AWS credentials for redis: %w", err)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+cacheName+"/", nil)
		if err != nil {
			return "", "", fmt.Errorf("invalid ElastiCache name")
		}
		req.URL.RawQuery = url.Values{"Action": {"connect"}, "User": {username}, "X-Amz-Expires": {"900"}}.Encode()
		signed, _, err := signer.PresignHTTP(ctx, creds, req, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "elasticache", cfg.Region, time.Now())
		if err != nil {
			return "", "", fmt.Errorf("sign AWS redis credentials: %w", err)
		}
		return username, strings.TrimPrefix(signed, "http://"), nil
	}
}
