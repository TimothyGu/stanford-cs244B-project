package internapi

import "go.timothygu.me/stanford-cs244b-project/internal/pkg/cache"

var (
	_DNSResponse_Type_conv = [...]cache.ResourceRecordType{
		DNSResponse_ANSWER:     cache.ResourceAnswer,
		DNSResponse_AUTHORITY:  cache.ResourceAuthority,
		DNSResponse_ADDITIONAL: cache.ResourceAdditional,
	}
)

func (t DNSResponse_Type) As() cache.ResourceRecordType {
	if t < 0 || int(t) >= len(_DNSResponse_Type_conv) {
		return 0
	}
	return _DNSResponse_Type_conv[t]
}
