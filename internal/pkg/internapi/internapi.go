package internapi

import "go.timothygu.me/stanford-cs244b-project/internal/pkg/types"

var (
	_DNSResponse_Type_conv = [...]types.ResourceRecordType{
		DNSResponse_ANSWER:     types.ResourceAnswer,
		DNSResponse_AUTHORITY:  types.ResourceAuthority,
		DNSResponse_ADDITIONAL: types.ResourceAdditional,
	}
	_DNSResponse_Type_conv2 = [...]DNSResponse_Type{
		types.ResourceAnswer:     DNSResponse_ANSWER,
		types.ResourceAuthority:  DNSResponse_AUTHORITY,
		types.ResourceAdditional: DNSResponse_ADDITIONAL,
	}
)

func (t DNSResponse_Type) As() types.ResourceRecordType {
	if t < 0 || int(t) >= len(_DNSResponse_Type_conv) {
		return 0
	}
	return _DNSResponse_Type_conv[t]
}

func FromResourceRecordType(t types.ResourceRecordType) DNSResponse_Type {
	if t < 0 || int(t) >= len(_DNSResponse_Type_conv2) {
		return 0
	}
	return _DNSResponse_Type_conv2[t]
}
