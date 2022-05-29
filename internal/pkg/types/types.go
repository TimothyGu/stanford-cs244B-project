package types

import "github.com/miekg/dns"

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
