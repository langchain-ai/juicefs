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
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/redis/go-redis/v9"
)

func configureRedisIAM(ctx context.Context, opt *redis.Options, region, cacheName, serverlessValue string) error {
	if opt.TLSConfig == nil || opt.TLSConfig.InsecureSkipVerify {
		return fmt.Errorf("AWS IAM authentication requires rediss:// with certificate verification")
	}
	if opt.Username == "" || opt.Password != "" {
		return fmt.Errorf("AWS IAM authentication requires a username and no password")
	}
	if !regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9-]*$`).MatchString(cacheName) {
		return fmt.Errorf("AWS IAM authentication requires aws-cache-name to be a replication group ID or serverless cache name")
	}
	serverless := false
	if serverlessValue != "" {
		var err error
		serverless, err = strconv.ParseBool(serverlessValue)
		if err != nil {
			return fmt.Errorf("aws-serverless must be a boolean")
		}
	}
	var loadOpts []func(*config.LoadOptions) error
	if region != "" {
		loadOpts = append(loadOpts, config.WithRegion(region))
	}
	cfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return fmt.Errorf("load AWS configuration: %w", err)
	}
	if cfg.Region == "" {
		return fmt.Errorf("AWS IAM authentication requires aws-region or an AWS SDK region configuration")
	}
	opt.CredentialsProviderContext = redisIAMCredentials(cfg, opt.Username, strings.ToLower(cacheName), serverless)
	// ElastiCache disconnects IAM-authenticated connections after 12 hours.
	if opt.ConnMaxLifetime <= 0 || opt.ConnMaxLifetime > 11*time.Hour {
		opt.ConnMaxLifetime = 11 * time.Hour
	}
	return nil
}

func redisIAMCredentials(cfg aws.Config, username, cacheName string, serverless bool) func(context.Context) (string, string, error) {
	return func(ctx context.Context) (string, string, error) {
		creds, err := cfg.Credentials.Retrieve(ctx)
		if err != nil {
			return "", "", fmt.Errorf("retrieve AWS credentials: %w", err)
		}
		query := url.Values{"Action": {"connect"}, "User": {username}, "X-Amz-Expires": {"900"}}
		if serverless {
			query.Set("ResourceType", "ServerlessCache")
		}
		// This URL is signed locally, never sent over HTTP.
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+cacheName+"/?"+query.Encode(), nil)
		if err != nil {
			return "", "", fmt.Errorf("create ElastiCache IAM signing request: %w", err)
		}
		payloadHash := fmt.Sprintf("%x", sha256.Sum256(nil))
		signed, _, err := v4.NewSigner().PresignHTTP(ctx, creds, req, payloadHash, "elasticache", cfg.Region, time.Now())
		if err != nil {
			return "", "", fmt.Errorf("sign ElastiCache IAM token: %w", err)
		}
		return username, strings.TrimPrefix(signed, "http://"), nil
	}
}
