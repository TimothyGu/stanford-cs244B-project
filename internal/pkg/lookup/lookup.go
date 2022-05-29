package lookup

import (
	"go.timothygu.me/stanford-cs244b-project/internal/pkg/types"
	"math"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/miekg/dns"
	log "github.com/sirupsen/logrus"
)

var supportedResponses = map[uint16]bool{
	dns.TypeA:     true,
	dns.TypeCNAME: true,
}

var supportedQueries = map[uint16]bool{
	dns.TypeA:     true,
	dns.TypeCNAME: true,
}

type CacheKey struct {
	DomainName string
	Type       uint16
}

type CacheValue struct {
	Type   types.ResourceRecordType
	Record dns.RR // TimeToLive field is indeterminate
	Expiry time.Time
}

var cache *lru.Cache // CacheKey -> []CacheValue

const defaultExternalServer = "1.1.1.1:53"

func externalLookup(query dns.Question, externalServer string) []CacheValue {
	if externalServer == "" {
		externalServer = defaultExternalServer
	}

	var m dns.Msg
	m.SetQuestion(query.Name, query.Qtype)
	m.RecursionDesired = true

	log.Infof("looking up %s at %s", query.Name, externalServer)
	now := time.Now()
	r, err := dns.Exchange(&m, externalServer)
	if err != nil {
		log.Error(err)
		return nil
	}
	log.Infof("received response for %s", query.Name)

	var cacheValues []CacheValue
	for _, rec := range r.Answer {
		if !supportedResponses[rec.Header().Rrtype] {
			log.Infof("unknown answer type: %u (%T)", rec.Header().Rrtype, rec)
			continue
		}
		cacheValues = append(cacheValues, CacheValue{
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
		cacheValues = append(cacheValues, CacheValue{
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
		cacheValues = append(cacheValues, CacheValue{
			Type:   types.ResourceAdditional,
			Record: rec,
			Expiry: now.Add(time.Duration(rec.Header().Ttl) * time.Second),
		})
	}

	return cacheValues
}

// Lookup does whatever is necessary to lookup a query.
func Lookup(wg *sync.WaitGroup, query dns.Question, output chan<- types.TypedResourceRecord, externalServer string) {
	defer wg.Done()

	if !supportedQueries[query.Qtype] || query.Qclass != dns.ClassINET {
		return
	}
	query.Name = dns.Fqdn(query.Name)

	key := CacheKey{
		DomainName: query.Name,
		Type:       query.Qtype,
	}
	now := time.Now()
	v, ok := cache.Get(key)
	if ok {
		log.Infof("found local cache for %s", query.Name)

		records := v.([]CacheValue)
		foundUpToDate := false
		for _, rec := range records {
			ttl := rec.Expiry.Sub(now).Seconds()
			if ttl > 0 {
				foundUpToDate = true
				rr := dns.Copy(rec.Record)
				if ttl > math.MaxInt32 {
					rr.Header().Ttl = math.MaxInt32
				} else {
					rr.Header().Ttl = uint32(ttl)
				}
				output <- types.TypedResourceRecord{
					Type:   rec.Type,
					Record: rr,
				}
			}
		}
		if foundUpToDate {
			return
		}
		// Don't bother deleting the entry from cache: we'll be looking it up
		// externally anyway.
		log.Infof("local cache for %s is stale", query.Name)
	}

	cacheValues := externalLookup(query, externalServer)
	cache.Add(key, cacheValues)

	for _, rec := range cacheValues {
		output <- types.TypedResourceRecord{
			Type:   rec.Type,
			Record: rec.Record,
		}
	}
}

// Initialize preloaded JSON database.
func InitDB() {
	names, err := GetNames()
	if err != nil {
		return
	}

	m := map[CacheKey][]CacheValue{}

	for _, name := range names {
		log.Infof("adding %s local entry to %v", name.Name, name.Address)

		key := CacheKey{Type: dns.TypeA, DomainName: dns.Fqdn(name.Name)}
		value := CacheValue{
			Type: types.ResourceAnswer,
			Record: &dns.A{
				Hdr: dns.RR_Header{
					Name:     dns.Fqdn(name.Name),
					Rrtype:   dns.TypeA,
					Class:    dns.ClassINET,
					Ttl:      0, // will be filled in at query time
					Rdlength: 0, // will be calculated at pack time
				},
				A: name.Address,
			},
			Expiry: time.Now().Add(2400 * time.Hour), // 100 days
		}
		m[key] = append(m[key], value)
	}
	cache, _ = lru.New(1000)
	for k, v := range m {
		cache.Add(k, v)
	}
}
