// Package cache implements a DNS cache.
package cache

import (
	"github.com/miekg/dns"
	"sync"
	"time"
)

var SupportedResponses = map[uint16]bool{
	dns.TypeA:     true,
	dns.TypeCNAME: true,
}

var SupportedQueries = map[uint16]bool{
	dns.TypeA:     true,
	dns.TypeCNAME: true,
}

type ResourceRecordType uint8

const (
	ResourceAnswer ResourceRecordType = iota
	ResourceAuthority
	ResourceAdditional
)

type TypedResourceRecord struct {
	Type   ResourceRecordType
	Record dns.RR
}

type CacheKey struct {
	DomainName string
	Type       uint16
}

type CacheValue struct {
	Type   ResourceRecordType
	Record dns.RR // TimeToLive field is indeterminate
	Expiry time.Time
}

type Cache = sync.Map // CacheKey -> CacheValue
