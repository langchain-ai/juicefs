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
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/redis/go-redis/v9"
)

func TestRedisIAMCredentials(t *testing.T) {
	for _, serverless := range []bool{false, true} {
		t.Run(fmt.Sprint(serverless), func(t *testing.T) {
			calls := 0
			cfg := aws.Config{Region: "us-east-1", Credentials: aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
				if err := ctx.Err(); err != nil {
					return aws.Credentials{}, err
				}
				calls++
				return aws.Credentials{AccessKeyID: fmt.Sprintf("test-access-%d", calls), SecretAccessKey: "test-secret", SessionToken: "test-session"}, nil
			})}
			provider := redisIAMCredentials(cfg, "test-user", "test-cache", serverless)
			for i := 1; i <= 2; i++ {
				user, token, err := provider(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if user != "test-user" || !strings.HasPrefix(token, "test-cache/?") {
					t.Fatal("unexpected username or token host")
				}
				u, err := url.Parse("http://" + token)
				if err != nil {
					t.Fatal(err)
				}
				q := u.Query()
				for key, want := range map[string]string{
					"Action": "connect", "User": "test-user", "X-Amz-Expires": "900",
					"X-Amz-Algorithm": "AWS4-HMAC-SHA256", "X-Amz-SignedHeaders": "host", "X-Amz-Security-Token": "test-session",
				} {
					if q.Get(key) != want {
						t.Errorf("incorrect %s", key)
					}
				}
				if !strings.HasPrefix(q.Get("X-Amz-Credential"), fmt.Sprintf("test-access-%d/", i)) || !strings.HasSuffix(q.Get("X-Amz-Credential"), "/us-east-1/elasticache/aws4_request") {
					t.Error("incorrect credential scope or stale credentials")
				}
				if len(q.Get("X-Amz-Signature")) != 64 || q.Get("X-Amz-Date") == "" {
					t.Error("missing signature or timestamp")
				}
				wantType := ""
				if serverless {
					wantType = "ServerlessCache"
				}
				if q.Get("ResourceType") != wantType {
					t.Error("incorrect resource type")
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			user, token, err := provider(ctx)
			if !errors.Is(err, context.Canceled) || user != "" || token != "" {
				t.Fatal("credential errors must propagate without returning a token")
			}
		})
	}
}

func TestRedisIAMConfiguration(t *testing.T) {
	// Keep tests independent of the developer's AWS profile and credentials.
	t.Setenv("AWS_CONFIG_FILE", t.TempDir()+"/config")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", t.TempDir()+"/credentials")
	t.Setenv("AWS_PROFILE", "")
	for _, tc := range []struct {
		name              string
		modify            func(*redis.Options)
		cache, serverless string
		wantError         bool
	}{
		{"valid", func(*redis.Options) {}, "test-cache", "", false},
		{"serverless", func(*redis.Options) {}, "test-cache", "true", false},
		{"no TLS", func(o *redis.Options) { o.TLSConfig = nil }, "test-cache", "", true},
		{"unverified TLS", func(o *redis.Options) { o.TLSConfig.InsecureSkipVerify = true }, "test-cache", "", true},
		{"no username", func(o *redis.Options) { o.Username = "" }, "test-cache", "", true},
		{"password", func(o *redis.Options) { o.Password = "test-password" }, "test-cache", "", true},
		{"no cache", func(*redis.Options) {}, "", "", true},
		{"endpoint instead of cache", func(*redis.Options) {}, "cache.example.com", "", true},
		{"invalid serverless", func(*redis.Options) {}, "test-cache", "maybe", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opt := &redis.Options{TLSConfig: &tls.Config{}, Username: "test-user"}
			tc.modify(opt)
			err := configureRedisIAM(context.Background(), opt, "us-east-1", tc.cache, tc.serverless)
			if (err != nil) != tc.wantError {
				t.Fatalf("unexpected error: %v", err)
			}
			if !tc.wantError && (opt.CredentialsProviderContext == nil || opt.ConnMaxLifetime != 11*time.Hour) {
				t.Fatal("provider and connection lifetime must be configured")
			}
		})
	}
	for _, lifetime := range []time.Duration{time.Hour, 24 * time.Hour} {
		opt := &redis.Options{TLSConfig: &tls.Config{}, Username: "test-user", ConnMaxLifetime: lifetime}
		if err := configureRedisIAM(context.Background(), opt, "us-east-1", "test-cache", ""); err != nil {
			t.Fatal(err)
		}
		if opt.ConnMaxLifetime != min(lifetime, 11*time.Hour) {
			t.Fatal("incorrect lifetime cap")
		}
	}
}

func TestRedisIAMURLValidation(t *testing.T) {
	for _, tc := range []struct{ driver, addr, want string }{
		{"redis", "user@localhost:6379/0?auth=aws-iam&aws-cache-name=test-cache", "requires rediss://"},
		{"rediss", "user@localhost:6379/0?auth=aws-iam&aws-cache-name=test-cache&insecure-skip-verify=true", "certificate verification"},
		{"rediss", "localhost:6379/0?auth=aws-iam&aws-cache-name=test-cache", "requires a username"},
		{"rediss", "user@localhost:6379/0?auth=aws-iam", "requires aws-cache-name"},
		{"rediss", "user@master,localhost:26379/0?auth=aws-iam&aws-cache-name=test-cache", "does not support Redis Sentinel"},
		{"rediss", "user@localhost:6379/0?auth=unknown", "require auth=aws-iam"},
		{"rediss", "user@localhost:6379/0?aws-cache-name=test-cache", "require auth=aws-iam"},
	} {
		t.Run(tc.addr, func(t *testing.T) {
			_, err := newRedisMeta(tc.driver, tc.addr, testConfig())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q, got %v", tc.want, err)
			}
		})
	}
}
