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

package chunk

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/murmur3"
)

// A cache group lets clients that mount the same volume share their block caches. Each block has one owner among the
// group's members, chosen by rendezvous hashing of the block key and the member address. A client that misses its own
// cache asks the owner, which answers from its cache or loads the block from object storage once (caching it) and
// answers. A cold client then pays one LAN round trip per block instead of an object-storage request, and the group
// fetches each block from object storage about once. Any failure to reach the owner falls back to object storage.
//
// The protocol has no authentication, like JuiceFS Enterprise's cache groups: members must sit on a trusted network,
// and every member can already read the whole volume.

const (
	cacheGroupPath           = "/jfs/cache-group/v1/block"
	cacheGroupChecksumHdr    = "X-Jfs-Crc32c"
	cacheGroupDefaultTO      = 5 * time.Second
	cacheGroupRefresh        = 10 * time.Second
	cacheGroupDownFor        = 30 * time.Second
	cacheGroupDNSPrefix      = "dns:"
	cacheGroupMaxIdlePerPeer = 64
)

var (
	errCacheGroupSelf = errors.New("this member owns the block")
)

type fromPeerKey struct{}

// fromPeer reports whether ctx belongs to a request another member sent, which must never be forwarded again.
func fromPeer(ctx context.Context) bool {
	v, _ := ctx.Value(fromPeerKey{}).(bool)
	return v
}

type cacheGroup struct {
	self     string
	peers    string // comma-separated host:port list, or dns:<name>:<port>
	members  atomic.Pointer[[]string]
	client   *http.Client
	timeout  time.Duration
	server   *http.Server
	downMu   sync.Mutex
	down     map[string]time.Time
	resolve  func(host string) ([]string, error)
	requests *prometheus.CounterVec
	latency  *prometheus.HistogramVec
	served   *prometheus.CounterVec
	bytes    *prometheus.CounterVec
}

func newCacheGroup(conf *Config) (*cacheGroup, error) {
	if conf.CacheGroupPeers == "" {
		return nil, nil
	}
	self := conf.CacheGroupSelf
	if self == "" {
		var err error
		if self, err = defaultCacheGroupSelf(conf.CacheGroupListen); err != nil {
			return nil, err
		}
	}
	timeout := conf.CacheGroupTimeout
	if timeout <= 0 {
		timeout = cacheGroupDefaultTO
	}
	g := &cacheGroup{
		self:    self,
		peers:   conf.CacheGroupPeers,
		timeout: timeout,
		down:    make(map[string]time.Time),
		resolve: net.LookupHost,
		client: &http.Client{Transport: &http.Transport{
			Proxy:               nil,
			DialContext:         (&net.Dialer{Timeout: time.Second, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConnsPerHost: cacheGroupMaxIdlePerPeer,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
		}},
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_group_requests",
			Help: "Blocks asked of a cache group owner, by result: hit (owner had it cached), loaded (owner fetched it from object storage), error (fell back to object storage), down (owner skipped as recently failed).",
		}, []string{"result"}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cache_group_request_durations_histogram_seconds",
			Help:    "Duration of block requests to cache group owners, by result.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 16),
		}, []string{"result"}),
		served: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_group_served",
			Help: "Blocks this member served to other members, by source: cache or object storage.",
		}, []string{"source"}),
		bytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_group_bytes",
			Help: "Block bytes moved through the cache group, by direction: fetched (from owners) or served (to members).",
		}, []string{"direction"}),
	}
	if err := g.refresh(); err != nil {
		logger.Warnf("cache group: resolve members %q: %s", g.peers, err)
	}
	go func() {
		for range time.Tick(cacheGroupRefresh) {
			if err := g.refresh(); err != nil {
				logger.Warnf("cache group: resolve members %q: %s", g.peers, err)
			}
		}
	}()
	return g, nil
}

// defaultCacheGroupSelf advertises the listen address, filling in the first non-loopback IPv4 address when the listen
// address has no host.
func defaultCacheGroupSelf(listen string) (string, error) {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "", fmt.Errorf("cache group listen address %q: %w", listen, err)
	}
	if host != "" && host != "0.0.0.0" && host != "::" {
		return listen, nil
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, a := range addrs {
		if ip, ok := a.(*net.IPNet); ok && !ip.IP.IsLoopback() && ip.IP.To4() != nil {
			return net.JoinHostPort(ip.IP.String(), port), nil
		}
	}
	return "", errors.New("cache group: no address to advertise; set cache-group-self")
}

// refresh resolves the member list. A DNS name (e.g. a Kubernetes headless Service) is re-resolved every
// cacheGroupRefresh, so members follow pods as they come and go.
func (g *cacheGroup) refresh() error {
	var members []string
	if rest, ok := strings.CutPrefix(g.peers, cacheGroupDNSPrefix); ok {
		host, port, err := net.SplitHostPort(rest)
		if err != nil {
			return err
		}
		ips, err := g.resolve(host)
		if err != nil {
			return err
		}
		for _, ip := range ips {
			members = append(members, net.JoinHostPort(ip, port))
		}
	} else {
		for _, m := range strings.Split(g.peers, ",") {
			if m = strings.TrimSpace(m); m != "" {
				members = append(members, m)
			}
		}
	}
	if len(members) == 0 {
		return errors.New("no members")
	}
	sort.Strings(members)
	g.members.Store(&members)
	return nil
}

// owner returns the member that owns key: the one with the highest hash of key and member address. Adding or removing
// a member moves only the keys it owns.
func (g *cacheGroup) owner(key string) string {
	members := g.members.Load()
	if members == nil || len(*members) == 0 {
		return g.self
	}
	var best string
	var bestScore uint64
	for _, m := range *members {
		h := murmur3.New64()
		_, _ = h.Write([]byte(key))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(m))
		if s := h.Sum64(); best == "" || s > bestScore {
			best, bestScore = m, s
		}
	}
	return best
}

// remote reports whether a load of key would ask another member.
func (g *cacheGroup) remote(ctx context.Context, key string) bool {
	return g != nil && !fromPeer(ctx) && g.owner(key) != g.self
}

func (g *cacheGroup) isDown(member string) bool {
	g.downMu.Lock()
	defer g.downMu.Unlock()
	until, ok := g.down[member]
	if ok && time.Now().After(until) {
		delete(g.down, member)
		return false
	}
	return ok
}

func (g *cacheGroup) markDown(member string) {
	g.downMu.Lock()
	g.down[member] = time.Now().Add(cacheGroupDownFor)
	g.downMu.Unlock()
}

// fetch reads key's whole block from its owner into buf, which must be the block's exact size. It returns
// errCacheGroupSelf when this member owns the block, and any other error means the caller should go to object storage.
func (g *cacheGroup) fetch(ctx context.Context, key string, buf []byte) error {
	member := g.owner(key)
	if member == g.self {
		return errCacheGroupSelf
	}
	if g.isDown(member) {
		g.requests.WithLabelValues("down").Inc()
		return fmt.Errorf("cache group member %s is down", member)
	}
	start := time.Now()
	result := "error"
	defer func() {
		g.requests.WithLabelValues(result).Inc()
		g.latency.WithLabelValues(result).Observe(time.Since(start).Seconds())
	}()
	cctx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()
	u := "http://" + member + cacheGroupPath + "?key=" + url.QueryEscape(key)
	req, err := http.NewRequestWithContext(cctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	resp, err := g.client.Do(req)
	if err != nil {
		if ctx.Err() == nil {
			g.markDown(member)
		}
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		if resp.StatusCode >= 500 {
			g.markDown(member)
		}
		return fmt.Errorf("cache group member %s: %s", member, resp.Status)
	}
	if n, err := io.ReadFull(resp.Body, buf); err != nil {
		return fmt.Errorf("cache group member %s: read %d of %d bytes: %w", member, n, len(buf), err)
	}
	want, err := strconv.ParseUint(resp.Header.Get(cacheGroupChecksumHdr), 10, 32)
	if err != nil || uint32(want) != crc32.Checksum(buf, crc32c) {
		return fmt.Errorf("cache group member %s: checksum mismatch for %s", member, key)
	}
	result = resp.Header.Get("X-Jfs-Source")
	if result != "cache" {
		result = "loaded"
	} else {
		result = "hit"
	}
	g.bytes.WithLabelValues("fetched").Add(float64(len(buf)))
	return nil
}

func (g *cacheGroup) serve(store *cachedStore, listen string) error {
	mux := http.NewServeMux()
	mux.HandleFunc(cacheGroupPath, func(w http.ResponseWriter, r *http.Request) {
		store.serveCacheGroupBlock(w, r)
	})
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	g.server = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := g.server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Errorf("cache group server on %s: %s", listen, err)
		}
	}()
	logger.Infof("cache group: serving on %s as %s, members from %q", listen, g.self, g.peers)
	return nil
}

func (g *cacheGroup) register(reg prometheus.Registerer) {
	reg.MustRegister(g.requests, g.latency, g.served, g.bytes)
}

// validCacheGroupKey accepts only data block keys ("chunks/..._<index>_<size>"), so the endpoint cannot be used to
// read arbitrary objects.
func validCacheGroupKey(key string, blockSize int) (int, bool) {
	if !strings.HasPrefix(key, "chunks/") || strings.Contains(key, "..") {
		return 0, false
	}
	size := parseObjOrigSize(key)
	return size, size > 0 && size <= blockSize
}

// serveCacheGroupBlock answers another member's request for a block it owns: from the local cache, or by loading it
// from object storage (deduplicated with concurrent local reads of the same block) and caching it.
func (store *cachedStore) serveCacheGroupBlock(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	size, ok := validCacheGroupKey(key, store.conf.BlockSize)
	if !ok {
		http.Error(w, "bad block key", http.StatusBadRequest)
		return
	}
	g := store.cacheGroup
	source := "cache"
	var data []byte
	if buf := make([]byte, size); store.loadCached(key, buf) {
		data = buf
	}
	if data == nil {
		source = "object"
		ctx := context.WithValue(r.Context(), fromPeerKey{}, true)
		p := NewOffPage(size)
		defer p.Release()
		block, err := store.executeShared(ctx, key, func() (*Page, error) {
			p.Acquire()
			err := store.load(ctx, key, p, store.shouldCache(size), true) // cached inside the shared load
			return p, err
		})
		if block != nil {
			defer block.Release()
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		data = block.Data
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Header().Set(cacheGroupChecksumHdr, strconv.FormatUint(uint64(crc32.Checksum(data, crc32c)), 10))
	w.Header().Set("X-Jfs-Source", source)
	_, _ = w.Write(data)
	g.served.WithLabelValues(source).Inc()
	g.bytes.WithLabelValues("served").Add(float64(len(data)))
}
