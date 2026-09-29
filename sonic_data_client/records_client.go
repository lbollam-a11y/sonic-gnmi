package client

// RECORDS client (WS1 bootstrap): the gNMI STREAM target that streams orchagent
// and sairedis recorder lines. It wires WS2 (tailer) -> WS3 (parser, matcher) ->
// gNMI priority queue, with history replay first (when from= is given) then a
// live tail. STREAM only; every other RPC is Unimplemented, like EVENTS.

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Workiva/go-datastructures/queue"
	log "github.com/golang/glog"
	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
	spb "github.com/sonic-net/sonic-gnmi/proto"
	sdcfg "github.com/sonic-net/sonic-gnmi/sonic_db_config"
)

const recPQDefaultSize = 10240

type RecordsClient struct {
	prefix  *gnmipb.Path
	subs    []*recSub
	matcher *recordsMatcher
	loc     *time.Location
	dir     string

	q       *queue.PriorityQueue
	pqMax   int
	channel chan struct{}
	wg      *sync.WaitGroup
	cancel  context.CancelFunc

	stopped   int32
	liveCount int32

	// counters
	sent    uint64
	sendErr uint64
	stalls  uint64
	matched uint64
}

// NewRecordsClient parses and validates the RECORDS subscription paths.
func NewRecordsClient(paths []*gnmipb.Path, prefix *gnmipb.Path, logLevel int) (Client, error) {
	loc := time.Local
	if tz := recordsTzEnv(); tz != "" {
		if l, err := time.LoadLocation(tz); err == nil {
			loc = l
		} else {
			log.V(1).Infof("records: bad RECORDS_TZ %q: %v; using local", tz, err)
		}
	}

	valid := validNamespaces()

	c := &RecordsClient{
		prefix: prefix,
		loc:    loc,
		dir:    recordsDirEnv(),
		pqMax:  recPQDefaultSize,
	}

	for _, p := range paths {
		s, err := parseRecordsPath(p, valid, loc)
		if err != nil {
			return nil, err
		}
		c.subs = append(c.subs, s)
	}
	if len(c.subs) == 0 {
		return nil, fmt.Errorf("RECORDS: no subscription paths")
	}
	c.matcher = newRecordsMatcher(c.subs)
	log.V(2).Infof("records: new client dir=%s tz=%s subs=%d", c.dir, loc, len(c.subs))
	return c, nil
}

// validNamespaces builds the set the operator may name. The default namespace is
// spelled "localhost"; non-default ones are asic0, asic1, ...
func validNamespaces() map[string]bool {
	valid := map[string]bool{"localhost": true}
	if nsList, err := sdcfg.GetDbAllNamespaces(); err == nil {
		for _, ns := range nsList {
			if ns == sdcfg.SONIC_DEFAULT_NAMESPACE {
				continue
			}
			valid[ns] = true
		}
	}
	return valid
}

// parseRecordsPath turns /RECORDS/<ns>/<DB>/<TABLE>[/<key...>][from=..][ops=..]
// into a recSub.
func parseRecordsPath(p *gnmipb.Path, valid map[string]bool, loc *time.Location) (*recSub, error) {
	elems := p.GetElem()
	names := make([]string, 0, len(elems))
	from, ops := "", ""
	for _, e := range elems {
		names = append(names, e.GetName())
		for k, v := range e.GetKey() {
			switch k {
			case "from":
				from = v
			case "ops":
				ops = v
			}
		}
	}
	// Tolerate a leading "RECORDS" element (path may echo the target).
	if len(names) > 0 && strings.EqualFold(names[0], "RECORDS") {
		names = names[1:]
	}
	if len(names) < 3 {
		return nil, fmt.Errorf("RECORDS path too short, want /<namespace>/<DB>/<TABLE>[...]: %v", names)
	}

	ns := names[0]
	if !valid[ns] {
		return nil, fmt.Errorf("RECORDS: unknown namespace %q (have %s)", ns, keysOf(valid))
	}

	db := names[1]
	if db != dbAPPL && db != dbASIC {
		return nil, fmt.Errorf("RECORDS: unknown DB %q, want APPL_DB or ASIC_DB", db)
	}

	s := &recSub{namespace: ns, db: db, path: p, ops: map[string]bool{}}

	if db == dbASIC {
		rest := names[2:]
		if len(rest) > 0 && rest[0] == asicStateTable {
			rest = rest[1:]
		}
		joined := strings.Join(rest, "/")
		s.table, s.key = splitSaiKeyToken(joined) // SAI_OBJECT_TYPE_X[:entry]
	} else {
		s.table = names[2]
		s.key = strings.Join(names[3:], "/") // stitch route prefixes back together
	}

	fromT, err := parseFrom(from, time.Now(), loc)
	if err != nil {
		return nil, fmt.Errorf("RECORDS: %v", err)
	}
	s.from = fromT

	for _, o := range strings.Split(ops, ",") {
		if o = strings.TrimSpace(o); o != "" {
			s.ops[o] = true
		}
	}
	log.V(2).Infof("records: sub ns=%s db=%s table=%s key=%q from=%v ops=%v", s.namespace, s.db, s.table, s.key, s.from, s.ops)
	return s, nil
}

func parseFrom(s string, now time.Time, loc *time.Location) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil // live only
	}
	if strings.HasPrefix(s, "-") {
		body := s[1:]
		if n, ok := trimUnit(body, "d"); ok {
			return now.Add(-time.Duration(n) * 24 * time.Hour), nil
		}
		if n, ok := trimUnit(body, "w"); ok {
			return now.Add(-time.Duration(n) * 7 * 24 * time.Hour), nil
		}
		d, err := time.ParseDuration(body)
		if err != nil {
			return time.Time{}, fmt.Errorf("bad relative from=%q", s)
		}
		return now.Add(-d), nil
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Unix(n, 0), nil
	}
	if ts, err := time.Parse(time.RFC3339, s); err == nil {
		return ts, nil
	}
	if ts, err := time.ParseInLocation("2006-01-02T15:04:05", s, loc); err == nil {
		return ts, nil
	}
	return time.Time{}, fmt.Errorf("bad from=%q (want RFC3339, epoch, or -30m/-2h/-1d)", s)
}

func trimUnit(body, unit string) (int, bool) {
	if strings.HasSuffix(body, unit) {
		if n, err := strconv.Atoi(strings.TrimSuffix(body, unit)); err == nil {
			return n, true
		}
	}
	return 0, false
}

func keysOf(m map[string]bool) string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	return strings.Join(ks, ",")
}

// --- Client interface ---

func (c *RecordsClient) StreamRun(q *queue.PriorityQueue, stop chan struct{}, wg *sync.WaitGroup, subscribe *gnmipb.SubscriptionList) {
	c.wg = wg
	defer wg.Done()
	c.q = q
	c.channel = stop

	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	defer cancel()

	go func() {
		select {
		case <-stop:
			atomic.StoreInt32(&c.stopped, 1)
			cancel()
		case <-ctx.Done():
		}
	}()

	parser := newRecordsParser(c.loc)

	pairs := c.tailerPairs()
	anyReplay := c.anyReplay()
	if anyReplay {
		c.emitMarker("replay_start", time.Now())
	}

	lines := make(chan RawLine, 1024)
	var tw sync.WaitGroup
	total := int32(len(pairs))
	onLive := func() {
		if atomic.AddInt32(&c.liveCount, 1) == total {
			c.emitSync()
			c.emitMarker("live", time.Now())
			log.V(2).Infof("records: replay complete, live tailing (%d tailers)", total)
		}
	}

	for _, p := range pairs {
		ft := newFileTailer(c.dir, p.source, p.namespace, c.loc)
		ft.onLive = onLive
		from := c.fromFor(p.namespace)
		tw.Add(1)
		go func(ft *fileTailer, from time.Time) {
			defer tw.Done()
			if err := ft.Run(ctx, from, lines); err != nil && ctx.Err() == nil {
				log.V(1).Infof("records: tailer %s/%s: %v", ft.base, ft.source, err)
			}
		}(ft, from)
	}
	go func() { tw.Wait(); close(lines) }()

	for {
		select {
		case <-ctx.Done():
			return
		case rl, ok := <-lines:
			if !ok {
				return
			}
			c.handle(parser, rl)
		}
	}
}

func (c *RecordsClient) handle(parser *recordsParser, rl RawLine) {
	rec, ok := parser.Parse(rl)
	if !ok {
		return
	}
	sub, how, matched := c.matcher.MatchSub(rec)
	if !matched {
		return
	}
	// Bound replay: drop records older than the matched subscription's from.
	if !sub.from.IsZero() && rec.TS.Before(sub.from) {
		return
	}
	rec.Matched = how
	atomic.AddUint64(&c.matched, 1)
	c.emitRecord(sub, rec)
}

// tailerPair identifies a (namespace, source) file to tail.
type tailerPair struct {
	namespace string
	source    string
}

// tailerPairs returns the unique (namespace, source) files the subscription set
// needs. A single sairedis+swss pair per namespace covers exact, prefix and
// correlation matching for that namespace.
func (c *RecordsClient) tailerPairs() []tailerPair {
	seen := map[tailerPair]bool{}
	var out []tailerPair
	for _, s := range c.subs {
		for _, src := range []string{srcSwss, srcSairedis} {
			p := tailerPair{s.namespace, src}
			if !seen[p] {
				seen[p] = true
				out = append(out, p)
			}
		}
	}
	return out
}

func (c *RecordsClient) anyReplay() bool {
	for _, s := range c.subs {
		if !s.from.IsZero() {
			return true
		}
	}
	return false
}

// fromFor returns the earliest from across subscriptions in a namespace (so a
// single tailer serves them all).
func (c *RecordsClient) fromFor(ns string) time.Time {
	var earliest time.Time
	for _, s := range c.subs {
		if s.namespace != ns || s.from.IsZero() {
			continue
		}
		if earliest.IsZero() || s.from.Before(earliest) {
			earliest = s.from
		}
	}
	return earliest
}

// emitRecord encodes a Record as JSON_IETF and enqueues it.
func (c *RecordsClient) emitRecord(sub *recSub, r *Record) {
	out := map[string]interface{}{
		"seq":        r.Seq,
		"ts":         r.TS.Format(recordsTSLayout),
		"source":     r.Source,
		"db":         r.DB,
		"table":      r.Table,
		"key":        r.Key,
		"op":         r.Op,
		"fields":     r.Fields,
		"status":     r.Status,
		"matched_by": r.Matched,
	}
	c.enqueue(sub.path, out, r.TS)
}

func (c *RecordsClient) emitMarker(event string, ts time.Time) {
	out := map[string]interface{}{"event": event}
	var path *gnmipb.Path
	if len(c.subs) > 0 {
		path = c.subs[0].path
	}
	c.enqueue(path, out, ts)
}

func (c *RecordsClient) enqueue(path *gnmipb.Path, payload map[string]interface{}, ts time.Time) {
	jv, err := json.Marshal(payload)
	if err != nil {
		log.V(1).Infof("records: marshal: %v", err)
		return
	}
	// Backpressure: never drop; block until the queue drains. Disk holds the
	// backlog, so a slow subscriber only delays live records, never loses them.
	for c.q.Len() >= c.pqMax {
		if atomic.LoadInt32(&c.stopped) == 1 {
			return
		}
		atomic.AddUint64(&c.stalls, 1)
		time.Sleep(10 * time.Millisecond)
	}
	spbv := &spb.Value{
		Prefix:    c.prefix,
		Path:      path,
		Timestamp: ts.UnixNano(),
		Val: &gnmipb.TypedValue{
			Value: &gnmipb.TypedValue_JsonIetfVal{JsonIetfVal: jv},
		},
	}
	if err := c.q.Put(Value{spbv}); err != nil {
		log.V(3).Infof("records: queue put: %v", err)
		return
	}
	atomic.AddUint64(&c.sent, 1)
}

func (c *RecordsClient) emitSync() {
	if err := c.q.Put(Value{&spb.Value{SyncResponse: true}}); err != nil {
		log.V(3).Infof("records: queue put sync: %v", err)
	}
}

func (c *RecordsClient) String() string {
	return fmt.Sprintf("RecordsClient prefix=%v subs=%d", c.prefix.GetTarget(), len(c.subs))
}

func (c *RecordsClient) Get(wg *sync.WaitGroup) ([]*spb.Value, error) {
	return nil, fmt.Errorf("RECORDS: Get not supported, use Subscribe STREAM")
}

func (c *RecordsClient) OnceRun(q *queue.PriorityQueue, once chan struct{}, wg *sync.WaitGroup, subscribe *gnmipb.SubscriptionList) {
	log.V(1).Infof("records: ONCE not supported")
}

func (c *RecordsClient) PollRun(q *queue.PriorityQueue, poll chan struct{}, wg *sync.WaitGroup, subscribe *gnmipb.SubscriptionList) {
	log.V(1).Infof("records: POLL not supported")
}

// AppDBPollRun exists on the upscale-ai-network sonic-gnmi Client interface;
// RECORDS is STREAM-only so it is a no-op. Harmless as an extra method on the
// upstream tree.
func (c *RecordsClient) AppDBPollRun(q *queue.PriorityQueue, poll chan struct{}, wg *sync.WaitGroup, subscribe *gnmipb.SubscriptionList) {
	log.V(1).Infof("records: AppDBPollRun not supported")
}

func (c *RecordsClient) Set(delete []*gnmipb.Path, replace []*gnmipb.Update, update []*gnmipb.Update) error {
	return fmt.Errorf("RECORDS: Set not supported")
}

func (c *RecordsClient) Capabilities() []gnmipb.ModelData {
	return nil
}

func (c *RecordsClient) Close() error {
	if c.cancel != nil {
		c.cancel()
	}
	return nil
}

func (c *RecordsClient) SentOne(val *Value) {}

func (c *RecordsClient) FailedSend() {
	atomic.AddUint64(&c.sendErr, 1)
}
