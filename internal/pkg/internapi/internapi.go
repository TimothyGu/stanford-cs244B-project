package internapi

import "go.timothygu.me/stanford-cs244b-project/internal/pkg/types"

var (
	_DNSResponse_Type_conv = [...]types.ResourceRecordType{
		DNSResponse_ANSWER:     types.ResourceAnswer,
		DNSResponse_AUTHORITY:  types.ResourceAuthority,
		DNSResponse_ADDITIONAL: types.ResourceAdditional,
	}
)

func (t DNSResponse_Type) As() types.ResourceRecordType {
	if t < 0 || int(t) >= len(_DNSResponse_Type_conv) {
		return 0
	}
	return _DNSResponse_Type_conv[t]
}
