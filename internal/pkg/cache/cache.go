package cache

import (
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/miekg/dns"
	"go.timothygu.me/stanford-cs244b-project/internal/pkg/types"
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

const Size = 1000

type Cache lru.Cache

func New() *Cache {
	cache, _ := lru.New(Size)
	return cache
}

func (c *Cache) Add(k Key, v []Value) {
	(*lru.Cache)(c).Add(k, v)
}

func (c *Cache) Get(k Key) (value []Value, ok bool) {
	vv, ok := (*lru.Cache)(c).Get(k)
	for _, v := range vv {
		value = append(value, v)
	}
	return value, ok
}

type Key struct {
	DomainName string
	Type       uint16
}

type Value struct {
	Type   types.ResourceRecordType
	Record dns.RR // TimeToLive field is indeterminate
	Expiry time.Time
}
