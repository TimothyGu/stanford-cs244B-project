package lookup

import (
	"context"
	"flag"
	"math"
	"math/rand"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/miekg/dns"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.timothygu.me/stanford-cs244b-project/internal/pkg/cache"
	"go.timothygu.me/stanford-cs244b-project/internal/pkg/chmembership"
	"go.timothygu.me/stanford-cs244b-project/internal/pkg/internapi"
	"go.timothygu.me/stanford-cs244b-project/internal/pkg/types"
)

var ExtraNodes = flag.Int("extranodes", 0, "number of extra nodes to forward responses to")

var supportedResponses = map[uint16]bool{
	dns.TypeA:     true,
	dns.TypeCNAME: true,
}

var supportedQueries = map[uint16]bool{
	dns.TypeA:     true,
	dns.TypeCNAME: true,
}

var L1Cache = cache.NewL1Cache()
var L2Cache = cache.NewL2Cache()

const defaultExternalServer = "1.1.1.1:53"

var externalClient = dns.Client{Net: "udp", Timeout: 30 * time.Second}

func externalLookup(ctx context.Context, query dns.Question) []cache.Value {
	externalServer := defaultExternalServer

	var m dns.Msg
	m.SetQuestion(query.Name, query.Qtype)
	m.RecursionDesired = true

	log.Infof("looking up %s at %s", query.Name, externalServer)
	now := time.Now()
	r, _, err := externalClient.ExchangeContext(ctx, &m, externalServer)
	if err != nil {
		log.Error(err)
		return nil
	}
	log.Infof("received response for %s", query.Name)

	var cacheValues []cache.Value
	for _, rec := range r.Answer {
		if !supportedResponses[rec.Header().Rrtype] {
			log.Infof("unknown answer type: %u (%T)", rec.Header().Rrtype, rec)
			continue
		}
		cacheValues = append(cacheValues, cache.Value{
			Type:   types.ResourceAnswer,
			Record: rec,
			Expiry: now.Add(time.Duration(rec.Header().Ttl) * time.Second),
		})
	}
	for _, rec := range r.Ns {
		if !supportedResponses[rec.Header().Rrtype] {
			log.Infof("unknown authority type: %u (%T)", rec.Header().Rrtype, rec)
			continue
		}
		cacheValues = append(cacheValues, cache.Value{
			Type:   types.ResourceAuthority,
			Record: rec,
			Expiry: now.Add(time.Duration(rec.Header().Ttl) * time.Second),
		})
	}
	for _, rec := range r.Extra {
		if !supportedResponses[rec.Header().Rrtype] {
			log.Infof("unknown additional type: %u (%T)", rec.Header().Rrtype, rec)
			continue
		}
		cacheValues = append(cacheValues, cache.Value{
			Type:   types.ResourceAdditional,
			Record: rec,
			Expiry: now.Add(time.Duration(rec.Header().Ttl) * time.Second),
		})
	}

	return cacheValues
}

var grpcConns, _ = lru.New(100) // addr -> *grpc.ClientConn

func getGRPCConn(addr string) (*grpc.ClientConn, error) {
	grpcConn, ok := grpcConns.Get(addr)
	if ok {
		return grpcConn.(*grpc.ClientConn), nil
	}
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Errorf("lookup: failed to dial gRPC: %v", conn)
		return nil, err
	}
	grpcConns.Add(addr, conn)
	return conn, err
}

func gossipToL2(ctx context.Context, query dns.Question, values []cache.Value, addr string) {
	grpcConn, err := getGRPCConn(addr)
	if err != nil {
		return
	}
	c := internapi.NewInternAPIClient(grpcConn)
	now := time.Now()

	qa := internapi.DNSQuestionResponse{
		Question: &internapi.DNSQuestion{
			Name:  query.Name,
			Qtype: uint32(query.Qtype),
		},
	}
	hasAnswer := false
	for _, v := range values {
		if v.Expiry.Sub(now) < 2*time.Second {
			// Not worth gossiping if it's expiring soon.
			continue
		}
		rrBuf := make([]byte, dns.Len(v.Record))
		_, err = dns.PackRR(v.Record, rrBuf, 0, nil, false)
		if err != nil {
			log.Warnf("lookup: unable to pack RR: %v", err)
			continue
		}
		if v.Type == types.ResourceAnswer {
			hasAnswer = true
		}
		qa.Response = append(qa.Response, &internapi.DNSResponse{
			Rr:     rrBuf,
			Expiry: timestamppb.New(v.Expiry),
			Type:   internapi.FromResourceRecordType(v.Type),
		})
	}
	if !hasAnswer {
		// No sense in gossiping if the response has no answer.
		return
	}

	req := internapi.NewContentRequest{Content: []*internapi.DNSQuestionResponse{&qa}}
	_, err = c.NewContent(ctx, &req)
	if err != nil {
		log.Warnf("lookup: unable to gossip to %v: %v", addr, err)
	}
}

func l2Lookup(ctx context.Context, query dns.Question, addr string) []cache.Value {
	grpcConn, err := getGRPCConn(addr)
	if err != nil {
		return nil
	}
	c := internapi.NewInternAPIClient(grpcConn)

	req := &internapi.QueryRequest{
		Question: &internapi.DNSQuestion{
			Name:  query.Name,
			Qtype: uint32(query.Qtype),
		},
	}
	log.Infof("lookup: requesting %v from %v", query.Name, addr)
	res, err := c.Query(ctx, req)
	if err != nil {
		log.Errorf("lookup: %v", err)
		return nil
	}

	var cacheValues []cache.Value
	for _, rec := range res.GetResponse() {
		rr, _, err := dns.UnpackRR(rec.GetRr(), 0)
		if err != nil {
			log.Warnf("lookup: got invalid RR from %v: %v", addr, err)
			continue
		}
		cacheValues = append(cacheValues, cache.Value{
			Type:   rec.Type.As(),
			Record: rr,
			Expiry: rec.GetExpiry().AsTime(),
		})
	}
	return cacheValues
}

type LookupOption uint8

const (
	LookupExternal LookupOption = 1 << iota
	LookupRemoteL2
	LookupSaveToL1
	LookupSaveToL2
	LookupGossipToOtherL2

	ExternalProfile = LookupExternal | LookupRemoteL2 | LookupSaveToL1 | LookupSaveToL2
	InternalProfile = LookupExternal | LookupSaveToL2 | LookupGossipToOtherL2
)

func (o LookupOption) Has(option LookupOption) bool {
	return o&option != 0
}

// Lookup does whatever is necessary to lookup a query.
func Lookup(ctx context.Context, m *chmembership.Membership, query dns.Question, options LookupOption) (output []types.TypedResourceRecord) {
	if !supportedQueries[query.Qtype] || query.Qclass != dns.ClassINET {
		return nil
	}
	query.Name = dns.Fqdn(query.Name)

	key := cache.Key{
		DomainName: query.Name,
		Type:       query.Qtype,
	}
	keyBytes, _ := key.MarshalBinary()

	l2Nodes := m.GetClosestN(keyBytes, 1+*ExtraNodes)
	if len(l2Nodes) == 0 {
		log.Warnf("lookup: query for %s has no L2 nodes", query.Name)
	}
	inSelfRange := contains(l2Nodes, m.Self())

	now := time.Now()

	// Look up local L2 cache if the key is within range.
	var records []cache.Value
	var ok bool
	records, ok = L1Cache.Get(key)
	if inSelfRange {
		records, ok = L2Cache.Get(key)
	}
	if ok {
		log.Infof("found local cache for %s", query.Name)

		for _, rec := range records {
			ttl := rec.Expiry.Sub(now).Seconds()
			if ttl > 0 {
				rr := dns.Copy(rec.Record)
				if ttl > math.MaxInt32 {
					rr.Header().Ttl = math.MaxInt32
				} else {
					rr.Header().Ttl = uint32(ttl)
				}
				output = append(output, types.TypedResourceRecord{
					Type:   rec.Type,
					Record: rr,
				})
			}
		}
		if len(output) > 0 {
			return output
		}
		// Don't bother deleting the entry from cache: we'll be looking it up
		// externally anyway, which should overwrite the L2 cache.
		log.Infof("local cache for %s is stale", query.Name)
	}

	var cacheValues []cache.Value

	// If no L2 nodes are available (e.g., consistent hashing is broken),
	// fall back to doing external lookup.
	if inSelfRange || len(l2Nodes) == 0 {
		cacheValues = externalLookup(ctx, query)
		if inSelfRange && options.Has(LookupSaveToL2) {
			L2Cache.Add(key, cacheValues)
		}
	} else if options.Has(LookupRemoteL2) {
		luckyNode := l2Nodes[rand.Intn(len(l2Nodes))]
		cacheValues = l2Lookup(ctx, query, luckyNode.Addr)
	}

	if !inSelfRange && options.Has(LookupSaveToL1) {
		L1Cache.Add(key, cacheValues)
	}

	if options.Has(LookupGossipToOtherL2) {
		go func() {
			gossipCtx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
			defer cancel()
			var wg sync.WaitGroup
			for _, l2Node := range l2Nodes {
				if l2Node.Name == m.Self().Name {
					continue
				}
				wg.Add(1)
				go func(addr string) {
					defer wg.Done()
					gossipToL2(gossipCtx, query, cacheValues, addr)
				}(l2Node.Addr)
			}
		}()
	}

	for _, rec := range cacheValues {
		output = append(output, types.TypedResourceRecord{
			Type:   rec.Type,
			Record: rec.Record,
		})
	}
	return output
}

func contains(servers []*chmembership.ServerNode, s chmembership.ServerNode) bool {
	for _, os := range servers {
		if os.Name == s.Name {
			return true
		}
	}
	return false
}
