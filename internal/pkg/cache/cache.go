package cache

import (
	lru "github.com/hashicorp/golang-lru"
	"github.com/miekg/dns"
	"go.timothygu.me/stanford-cs244b-project/internal/pkg/types"
	"time"
)

const Size = 1000

type Cache lru.Cache

func New() *Cache {
	cache, _ := lru.New(Size)
	return (*Cache)(cache)
}

func (c *Cache) Add(k Key, v []Value) {
	(*lru.Cache)(c).Add(k, v)
}

func (c *Cache) Get(k Key) (value interface{}, ok bool) {
	return (*lru.Cache)(c).Get(k)
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
