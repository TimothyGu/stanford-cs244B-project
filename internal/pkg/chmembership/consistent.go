package chmembership

import (
	"github.com/buraksezer/consistent"
	"github.com/dchest/siphash"
)

// DNSRequestHash is the hash function used to distribute keys/members uniformly.
type DNSRequestHash struct{}

// Keys to SipHash, generated at random.
// Ideally this should be decided at runtime and stored in Zookeeper.
const (
	siphash_k0 uint64 = 16038536194969526240
	siphash_k1 uint64 = 11178038365157218530
)

func (h DNSRequestHash) Sum64(data []byte) uint64 {
	return siphash.Hash(siphash_k0, siphash_k1, data)
}

// NewConsistentHashing creates a new consistent.Consistant instance.
func NewConsistentHashing() *consistent.Consistent {
	cfg := consistent.Config{
		PartitionCount:    65521, // prime number
		ReplicationFactor: 3,
		Load:              1.50,
		Hasher:            DNSRequestHash{},
	}
	return consistent.New(nil, cfg)
}
