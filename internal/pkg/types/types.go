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

type ServerNode struct {
	Name string
	Addr string
}

func (sa *ServerNode) String() string {
	return sa.Name
}
