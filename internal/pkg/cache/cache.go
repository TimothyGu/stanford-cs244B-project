package cache

import (
	"errors"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/miekg/dns"

	"go.timothygu.me/stanford-cs244b-project/internal/pkg/types"
)

type Key struct {
	DomainName string
	Type       uint16
}

func (k Key) MarshalBinary() (data []byte, err error) {
	buf := make([]byte, 2+len(k.DomainName))
	buf[0] = byte(k.Type >> 8)
	buf[1] = byte(k.Type)
	copy(buf[2:], k.DomainName)
	return buf, nil
}

func (k *Key) UnmarshalBinary(data []byte) error {
	if len(data) < 2 {
		return errors.New("cache: too short to be a Key")
	}
	k.Type = uint16(data[0])<<8 | uint16(data[1])
	k.DomainName = string(data[2:])
	return nil
}

type Value struct {
	Type   types.ResourceRecordType
	Record dns.RR // TimeToLive field is indeterminate
	Expiry time.Time
}

const L1CacheSize = 1000
const L1CacheExpiration = time.Hour * 2
const L2CacheSize = 10000

type L1Cache gcache.LFUCache
type L2Cache lru.Cache

func NewL1Cache() *L1Cache {
	gc := gcache.New(L1CacheSize).LFU().Build().(*gcache.LFUCache)
	return (*L1Cache)(gc)
}

// Purge is used to completely clear the cache.
func (c *L1Cache) Purge() {
	(*gcache.LFUCache)(c).Purge()
}

// Add adds a value to the cache.
func (c *L1Cache) Add(k Key, v []Value) {
	_ = (*gcache.LFUCache)(c).SetWithExpire(k, v, L1CacheExpiration)
}

// Get looks up a key's value from the cache.
func (c *L1Cache) Get(k Key) (value []Value, ok bool) {
	vv, err := (*gcache.LFUCache)(c).Get(k)
	if err != nil {
		return nil, ok
	}
	return vv.([]Value), ok
}

// Contains checks if a key is in the cache, without updating the
// recent-ness or deleting it for being stale.
func (c *L1Cache) Contains(k Key) bool {
	return (*gcache.LFUCache)(c).Has(k)
}

// Remove removes the provided key from the cache.
func (c *L1Cache) Remove(k Key) {
	(*gcache.LFUCache)(c).Remove(k)
}

// Keys returns a slice of the keys in the cache including the expired keys.
func (c *L1Cache) Keys() (keys []Key) {
	kk := (*gcache.LFUCache)(c).Keys(false)
	for _, k := range kk {
		keys = append(keys, k.(Key))
	}
	return keys
}

// Len returns the number of items in the cache including the expired ones.
func (c *L1Cache) Len() int {
	return (*gcache.LFUCache)(c).Len(false)
}

func NewL2Cache() *L2Cache {
	cache, _ := lru.New(L2CacheSize)
	return (*L2Cache)(cache)
}

// Purge is used to completely clear the cache.
func (c *L2Cache) Purge() {
	(*lru.Cache)(c).Purge()
}

// Add adds a value to the cache.  Returns true if an eviction occurred.
func (c *L2Cache) Add(k Key, v []Value) bool {
	return (*lru.Cache)(c).Add(k, v)
}

// Get looks up a key's value from the cache.
func (c *L2Cache) Get(k Key) (value []Value, ok bool) {
	vv, ok := (*lru.Cache)(c).Get(k)
	if !ok {
		return nil, ok
	}
	return vv.([]Value), ok
}

// Contains checks if a key is in the cache, without updating the
// recent-ness or deleting it for being stale.
func (c *L2Cache) Contains(k Key) bool {
	return (*lru.Cache)(c).Contains(k)
}

// Peek returns the key value (or undefined if not found) without updating
// the "recently used"-ness of the key.
func (c *L2Cache) Peek(k Key) (value []Value, ok bool) {
	vv, ok := (*lru.Cache)(c).Peek(k)
	return vv.([]Value), ok
}

// Remove removes the provided key from the cache.
func (c *L2Cache) Remove(k Key) {
	(*lru.Cache)(c).Remove(k)
}

// RemoveOldest removes the oldest item from the cache.
func (c *L2Cache) RemoveOldest() {
	(*lru.Cache)(c).RemoveOldest()
}

// Keys returns a slice of the keys in the cache, from oldest to newest.
func (c *L2Cache) Keys() (keys []Key) {
	kk := (*lru.Cache)(c).Keys()
	for _, k := range kk {
		keys = append(keys, k.(Key))
	}
	return keys
}

// Len returns the number of items in the cache.
func (c *L2Cache) Len() int {
	return (*lru.Cache)(c).Len()
}

