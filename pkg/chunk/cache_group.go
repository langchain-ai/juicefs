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
	cacheGroupHopsHdr        = "X-Jfs-Hops"
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

// peerHops is how many members a request in ctx has passed through: 0 for this client's own reads, 1 for a request
// another member sent, 2 for one a secondary owner forwarded to the primary.
func peerHops(ctx context.Context) int {
	v, _ := ctx.Value(fromPeerKey{}).(int)
	return v
}

func withPeerHops(ctx context.Context, hops int) context.Context {
	return context.WithValue(ctx, fromPeerKey{}, hops)
}

// fromPeer reports whether ctx belongs to a request another member sent.
func fromPeer(ctx context.Context) bool {
	return peerHops(ctx) > 0
}

type cacheGroup struct {
	self        string
	noLocalCopy bool
	replicas    int    // owners per block
	peers       string // comma-separated host:port list, or dns:<name>:<port>
	members     atomic.Pointer[[]string]
	client      *http.Client
	timeout     time.Duration
	server      *http.Server
	downMu      sync.Mutex
	down        map[string]time.Time
	resolve     func(host string) ([]string, error)
	requests    *prometheus.CounterVec
	latency     *prometheus.HistogramVec
	served      *prometheus.CounterVec
	bytes       *prometheus.CounterVec
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
	replicas := conf.CacheGroupReplicas
	if replicas < 1 {
		replicas = 1
	}
	g := &cacheGroup{
		self:        self,
		noLocalCopy: conf.CacheGroupNoLocalCopy,
		replicas:    replicas,
		peers:       conf.CacheGroupPeers,
		timeout:     timeout,
		down:        make(map[string]time.Time),
		resolve:     net.LookupHost,
		client: &http.Client{Transport: &http.Transport{
			Proxy:               nil,
			DialContext:         (&net.Dialer{Timeout: time.Second, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConnsPerHost: cacheGroupMaxIdlePerPeer,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
		}},
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_group_requests",
			Help: "Blocks asked of a cache group owner, by result: hit (owner had it cached), loaded (owner fetched it from object storage), down (owner skipped as recently failed), canceled (the read was abandoned), or why the request failed and fell back to object storage: timeout, connect, status_4xx, status_5xx, short_read, checksum.",
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

func rendezvousScore(parts ...string) uint64 {
	h := murmur3.New64()
	for i, p := range parts {
		if i > 0 {
			_, _ = h.Write([]byte{0})
		}
		_, _ = h.Write([]byte(p))
	}
	return h.Sum64()
}

// owners returns the members that own key, primary first: the replicas members with the highest hash of key and
// member address. Adding or removing a member moves only the keys it owns.
func (g *cacheGroup) owners(key string) []string {
	members := g.members.Load()
	if members == nil || len(*members) == 0 {
		return []string{g.self}
	}
	ranked := append([]string(nil), *members...)
	scores := make(map[string]uint64, len(ranked))
	for _, m := range ranked {
		scores[m] = rendezvousScore(key, m)
	}
	sort.Slice(ranked, func(i, j int) bool {
		if scores[ranked[i]] != scores[ranked[j]] {
			return scores[ranked[i]] > scores[ranked[j]]
		}
		return ranked[i] < ranked[j]
	})
	return ranked[:min(max(g.replicas, 1), len(ranked))]
}

// isOwner reports whether this member is one of key's owners.
func (g *cacheGroup) isOwner(key string) bool {
	for _, m := range g.owners(key) {
		if m == g.self {
			return true
		}
	}
	return false
}

// askOrder lists the members a load of key in ctx asks, in order, before going to object storage. The primary owner
// asks no one. A secondary owner asks the primary, for its own reads and once for a request another member sent, so the
// group loads each block from object storage about once. Any other member asks the owners, each member preferring a
// different one so the owners share the load; a request that reached a non-owner (membership views can briefly
// differ) is not forwarded again.
func (g *cacheGroup) askOrder(ctx context.Context, key string) []string {
	owners := g.owners(key)
	if owners[0] == g.self {
		return nil
	}
	hops := peerHops(ctx)
	if g.isOwner(key) {
		if hops <= 1 {
			return owners[:1]
		}
		return nil
	}
	if hops > 0 {
		return nil
	}
	sort.SliceStable(owners, func(i, j int) bool {
		return rendezvousScore(key, g.self, owners[i]) > rendezvousScore(key, g.self, owners[j])
	})
	return owners
}

// keepFetched reports whether this member caches a block it fetched from the block's owner.
func (g *cacheGroup) keepFetched() bool {
	return g == nil || !g.noLocalCopy
}

// keepsLocally reports whether this member caches key after loading it: always unless key belongs to another member
// and copies of other members' blocks are off.
func (g *cacheGroup) keepsLocally(key string) bool {
	return g.keepFetched() || g.isOwner(key)
}

// remote reports whether a load of key would ask another member.
func (g *cacheGroup) remote(ctx context.Context, key string) bool {
	return g != nil && len(g.askOrder(ctx, key)) > 0
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

// fetch reads key's whole block into buf, which must be the block's exact size, from the first member in askOrder that
// answers. It returns errCacheGroupSelf when there is no one to ask, and any other error means the caller should go to
// object storage.
func (g *cacheGroup) fetch(ctx context.Context, key string, buf []byte) error {
	order := g.askOrder(ctx, key)
	if len(order) == 0 {
		return errCacheGroupSelf
	}
	var err error
	for _, member := range order {
		if err = g.fetchFrom(ctx, member, key, buf); err == nil || ctx.Err() != nil {
			return err
		}
	}
	return err
}

func (g *cacheGroup) fetchFrom(ctx context.Context, member, key string, buf []byte) error {
	if g.isDown(member) {
		g.requests.WithLabelValues("down").Inc()
		return fmt.Errorf("cache group member %s is down", member)
	}
	start := time.Now()
	result := "connect"
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
	req.Header.Set(cacheGroupHopsHdr, strconv.Itoa(peerHops(ctx)+1))
	resp, err := g.client.Do(req)
	if err != nil {
		switch {
		case ctx.Err() != nil: // the caller gave up; the owner is fine
			result = "canceled"
		case errors.Is(cctx.Err(), context.DeadlineExceeded):
			result = "timeout"
			g.markDown(member)
		default:
			g.markDown(member)
		}
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		result = "status_4xx"
		if resp.StatusCode >= 500 {
			result = "status_5xx"
			g.markDown(member)
		}
		return fmt.Errorf("cache group member %s: %s", member, resp.Status)
	}
	if n, err := io.ReadFull(resp.Body, buf); err != nil {
		result = "short_read"
		if ctx.Err() != nil {
			result = "canceled"
		}
		return fmt.Errorf("cache group member %s: read %d of %d bytes: %w", member, n, len(buf), err)
	}
	want, err := strconv.ParseUint(resp.Header.Get(cacheGroupChecksumHdr), 10, 32)
	if err != nil || uint32(want) != crc32.Checksum(buf, crc32c) {
		result = "checksum"
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
		// Finish the load even if the member that asked hangs up (a reader often abandons readahead within ~10 ms):
		// the block is cached here for the next member that asks, rather than fetched from object storage again.
		// The load is still bounded by the store's GET timeout.
		hops, err := strconv.Atoi(r.Header.Get(cacheGroupHopsHdr))
		if err != nil || hops < 1 {
			hops = 1
		}
		ctx := withPeerHops(context.WithoutCancel(r.Context()), hops)
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
