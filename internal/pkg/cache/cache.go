package cache

import (
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/miekg/dns"

	"go.timothygu.me/stanford-cs244b-project/internal/pkg/types"
)

type Key struct {
	DomainName string
	Type       uint16
}

type Value struct {
	Type   types.ResourceRecordType
	Record dns.RR // TimeToLive field is indeterminate
	Expiry time.Time
}

const Size = 1000

type Cache lru.Cache

func New() *Cache {
	cache, _ := lru.New(Size)
	return (*Cache)(cache)
}

// Purge is used to completely clear the cache.
func (c *Cache) Purge() {
	(*lru.Cache)(c).Purge()
}

// Add adds a value to the cache.  Returns true if an eviction occurred.
func (c *Cache) Add(k Key, v []Value) bool {
	return (*lru.Cache)(c).Add(k, v)
}

// Get looks up a key's value from the cache.
func (c *Cache) Get(k Key) (value []Value, ok bool) {
	vv, ok := (*lru.Cache)(c).Get(k)
	return vv.([]Value), ok
}

// Contains checks if a key is in the cache, without updating the
// recent-ness or deleting it for being stale.
func (c *Cache) Contains(k Key) bool {
	return (*lru.Cache)(c).Contains(k)
}

// Peek returns the key value (or undefined if not found) without updating
// the "recently used"-ness of the key.
func (c *Cache) Peek(k Key) (value []Value, ok bool) {
	vv, ok := (*lru.Cache)(c).Peek(k)
	return vv.([]Value), ok
}

// Remove removes the provided key from the cache.
func (c *Cache) Remove(k Key) {
	(*lru.Cache)(c).Remove(k)
}

// RemoveOldest removes the oldest item from the cache.
func (c *Cache) RemoveOldest() {
	(*lru.Cache)(c).RemoveOldest()
}

// Keys returns a slice of the keys in the cache, from oldest to newest.
func (c *Cache) Keys() (keys []Key) {
	kk := (*lru.Cache)(c).Keys()
	for _, k := range kk {
		keys = append(keys, k.(Key))
	}
	return keys
}

// Len returns the number of items in the cache.
func (c *Cache) Len() int {
	return (*lru.Cache)(c).Len()
}
