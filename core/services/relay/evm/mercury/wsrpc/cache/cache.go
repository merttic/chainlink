package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jpillora/backoff"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/smartcontractkit/chainlink-common/pkg/services"
	"github.com/smartcontractkit/chainlink/v2/core/logger"
	mercuryutils "github.com/smartcontractkit/chainlink/v2/core/services/relay/evm/mercury/utils"
	"github.com/smartcontractkit/chainlink/v2/core/services/relay/evm/mercury/wsrpc/pb"
	"github.com/smartcontractkit/chainlink/v2/core/utils"
)

var (
	promFetchFailedCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mercury_cache_fetch_failure_count",
		Help: "Number of times we tried to call LatestReport from the mercury server, but some kind of error occurred",
	},
		[]string{"serverURL", "feedID"},
	)
)

// NOTE: Cache is scoped to one particular mercury server
// Use CacheSet to hold lookups for multiple servers

type Fetcher interface {
	LatestReport(ctx context.Context, req *pb.LatestReportRequest) (resp *pb.LatestReportResponse, err error)
}

type Client interface {
	Fetcher
	ServerURL() string
}

type Cache interface {
	Fetcher
	services.Service
}

type Config struct {
	Logger logger.Logger
	// LatestReportTTL controls how "stale" we will allow a price to be e.g. if
	// set to 1s, a new price will always be fetched if the last result was
	// from more than 1 second ago.
	//
	// Another way of looking at it is such: the cache will _never_ return a
	// price that was queried from before now-LatestReportTTL.
	//
	// Setting to zero disables caching entirely.
	LatestReportTTL time.Duration
	// MaxStaleAge is that maximum amount of time that a value can be stale
	// before it is deleted from the cache (a form of garbage collection).
	//
	// This should generally be set to something much larger than
	// LatestReportTTL. Setting to zero disables garbage collection.
	MaxStaleAge time.Duration
	// LatestReportDeadline controls how long to wait for a response before
	// retrying. Setting this to zero will wait indefinitely.
	LatestReportDeadline time.Duration
}

func NewCache(client Client, cfg Config) Cache {
	return newMemCache(client, cfg)
}

type cacheVal struct {
	sync.RWMutex

	fetching bool
	fetchCh  chan (struct{})

	val *pb.LatestReportResponse

	expiresAt time.Time
}

func (v *cacheVal) read() *pb.LatestReportResponse {
	v.RLock()
	defer v.RUnlock()
	return v.val
}

// caller expected to hold lock
func (v *cacheVal) initiateFetch() <-chan struct{} {
	v.fetching = true
	v.fetchCh = make(chan struct{})
	return v.fetchCh
}

func (v *cacheVal) completeFetch(val *pb.LatestReportResponse, expiresAt time.Time) {
	v.Lock()
	v.val = val
	v.expiresAt = expiresAt
	close(v.fetchCh)
	v.fetchCh = nil
	v.fetching = false
	v.Unlock()
}

func (v *cacheVal) abandonFetch() {
	v.completeFetch(nil, time.Time{})
}

func (v *cacheVal) waitForResult(chResult <-chan struct{}, ctx context.Context, chStop <-chan struct{}) (*pb.LatestReportResponse, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-chStop:
		return nil, ErrStopped{}
	case <-chResult:
		return v.read(), nil
	}
}

// memCache stores values in memory
// it will never return a stale value older than latestPriceTTL, instead
// waiting for a successful fetch or caller context cancels, whichever comes
// first
type memCache struct {
	services.StateMachine
	lggr logger.Logger

	client Client

	latestPriceTTL       time.Duration
	maxStaleAge          time.Duration
	latestReportDeadline time.Duration

	cache sync.Map

	wg     sync.WaitGroup
	chStop utils.StopChan
}

func newMemCache(client Client, cfg Config) *memCache {
	return &memCache{
		services.StateMachine{},
		cfg.Logger.Named("MercuryMemCache"),
		client,
		cfg.LatestReportTTL,
		cfg.MaxStaleAge,
		cfg.LatestReportDeadline,
		sync.Map{},
		sync.WaitGroup{},
		make(chan (struct{})),
	}
}

type ErrStopped struct{}

func (e ErrStopped) Error() string {
	return "memCache was stopped"
}

// LatestReport
// FIXME: This will actually block on all types of errors, even non timeouts. Context should be set carefully
// TODO: Can we return a cause e.g. context timed out wrapping latest error on the cacheVal?
func (m *memCache) LatestReport(ctx context.Context, req *pb.LatestReportRequest) (resp *pb.LatestReportResponse, err error) {
	if req == nil {
		return nil, errors.New("req must not be nil")
	}
	if m.latestPriceTTL <= 0 {
		return m.client.LatestReport(ctx, req)
	}
	vi, _ := m.cache.LoadOrStore(req, &cacheVal{
		sync.RWMutex{},
		false,
		nil,
		nil,
		time.Now(), // first result is always "expired" and requires fetch
	})
	v := vi.(*cacheVal)

	// HOT PATH
	v.RLock()
	if time.Now().Before(v.expiresAt) {
		// CACHE HIT
		defer v.RUnlock()
		return v.val, nil
	} else if v.fetching {
		// CACHE WAIT
		// if someone else is fetching then wait for the fetch to complete
		ch := v.fetchCh
		v.RUnlock()
		return v.waitForResult(ch, ctx, m.chStop)
	} else {
		// CACHE MISS
		// fallthrough to cold path and fetch
		v.RUnlock()
	}

	// COLD PATH
	v.Lock()
	if time.Now().Before(v.expiresAt) {
		// CACHE HIT
		defer v.RUnlock()
		return v.val, nil
	} else if v.fetching {
		// CACHE WAIT
		// if someone else is fetching then wait for the fetch to complete
		ch := v.fetchCh
		v.Unlock()
		return v.waitForResult(ch, ctx, m.chStop)
	} else {
		// CACHE MISS
		// initiate the fetch and wait for result
		ch := v.initiateFetch()
		v.Unlock()

		ok := m.IfStarted(func() {
			m.wg.Add(1)
			go m.fetch(req, v)
		})
		if !ok {
			v.abandonFetch()
			return nil, fmt.Errorf("memCache must be started, but is: %v", m.State())
		}
		return v.waitForResult(ch, ctx, m.chStop)
	}
}

const minBackoffRetryInterval = 50 * time.Millisecond

// newBackoff creates a backoff for retrying
func (m *memCache) newBackoff() backoff.Backoff {
	min := minBackoffRetryInterval
	if min > m.latestPriceTTL/2 {
		min = m.latestPriceTTL / 2
	}
	return backoff.Backoff{
		Min:    min,
		Max:    m.latestPriceTTL / 2,
		Factor: 2,
		Jitter: true,
	}
}

// fetch continually tries to call FetchLatestReport and write the result to v
func (m *memCache) fetch(req *pb.LatestReportRequest, v *cacheVal) {
	defer m.wg.Done()
	b := m.newBackoff()
	memcacheCtx, cancel := m.chStop.NewCtx()
	defer cancel()
	var t time.Time
	var val *pb.LatestReportResponse
	defer func() {
		// TODO: save the error as well?
		v.completeFetch(val, t.Add(m.latestPriceTTL))
	}()

	for {
		var err error
		var ctx context.Context
		var cancel context.CancelFunc
		t = time.Now()

		if m.latestReportDeadline > 0 {
			ctx, cancel = context.WithTimeoutCause(memcacheCtx, m.latestReportDeadline, errors.New("latest report fetch deadline exceeded"))
		} else {
			ctx = memcacheCtx
			cancel = func() {}
		}
		val, err = m.client.LatestReport(ctx, req)
		cancel()
		if memcacheCtx.Err() != nil {
			// stopped
			return
		} else if err != nil {
			m.lggr.Warnw("FetchLatestReport failed", "err", err)
			promFetchFailedCount.WithLabelValues(m.client.ServerURL(), mercuryutils.BytesToFeedID(req.FeedId).String()).Inc()
			select {
			case <-m.chStop:
				return
			case <-time.After(b.Duration()):
				continue
			}
		}
		return
	}
}

func (m *memCache) Start(context.Context) error {
	return m.StartOnce(m.Name(), func() error {
		m.wg.Add(1)
		go m.runloop()
		return nil
	})
}

func (m *memCache) runloop() {
	defer m.wg.Done()

	if m.maxStaleAge == 0 {
		return
	}
	t := time.NewTicker(utils.WithJitter(m.maxStaleAge))

	for {
		select {
		case <-t.C:
			m.cleanup()
			t.Reset(utils.WithJitter(m.maxStaleAge))
		case <-m.chStop:
			return
		}
	}
}

// remove anything that has been stale for longer than maxStaleAge so that cache doesn't grow forever and cause memory leaks
func (m *memCache) cleanup() {
	m.cache.Range(func(k, vi any) bool {
		v := vi.(*cacheVal)
		v.RLock()
		defer v.RUnlock()
		if v.fetching {
			// skip cleanup if fetching
			return true
		}
		if time.Now().After(v.expiresAt.Add(m.maxStaleAge)) {
			// garbage collection
			m.cache.Delete(k)
		}
		return true
	})
}

func (m *memCache) Close() error {
	return m.StopOnce(m.Name(), func() error {
		close(m.chStop)
		m.wg.Wait()
		return nil
	})
}
func (m *memCache) HealthReport() map[string]error {
	return map[string]error{
		m.Name(): m.Ready(),
	}
}
func (m *memCache) Name() string { return m.lggr.Name() }
