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
	"fmt"

	entraid "github.com/redis/go-redis-entraid"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/auth"
)

// redisStreamingCredentials resolves the `auth-provider` metadata URL option.
// An empty value keeps static password authentication.
func redisStreamingCredentials(authProvider string, opt *redis.Options, newAzure func() (auth.StreamingCredentialsProvider, error)) (auth.StreamingCredentialsProvider, error) {
	switch authProvider {
	case "":
		return nil, nil
	case "azure":
		if opt.Password != "" {
			return nil, fmt.Errorf("redis auth-provider=azure cannot be combined with a static password")
		}
		// Entra tokens are bearer credentials; never send them without TLS.
		if opt.TLSConfig == nil {
			return nil, fmt.Errorf("redis auth-provider=azure requires a rediss:// metadata URL")
		}
		creds, err := newAzure()
		if err != nil {
			return nil, fmt.Errorf("create azure redis credentials provider: %w", err)
		}
		return creds, nil
	default:
		return nil, fmt.Errorf("unsupported redis auth-provider %q", authProvider)
	}
}

// newAzureRedisCredentials uses the default Azure credential chain, which covers
// AKS workload identity and managed identity.
func newAzureRedisCredentials() (auth.StreamingCredentialsProvider, error) {
	return entraid.NewDefaultAzureCredentialsProvider(entraid.DefaultAzureCredentialsProviderOptions{})
}
