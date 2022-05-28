// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.20.1
// source: internal/pkg/raftcache/raftcache.proto

package raftcache

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// RaftCacheClient is the client API for RaftCache service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type RaftCacheClient interface {
	Cache(ctx context.Context, in *RaftCacheRequest, opts ...grpc.CallOption) (*RaftCacheReply, error)
	ConfChange(ctx context.Context, in *ConfChangeRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
}

type raftCacheClient struct {
	cc grpc.ClientConnInterface
}

func NewRaftCacheClient(cc grpc.ClientConnInterface) RaftCacheClient {
	return &raftCacheClient{cc}
}

func (c *raftCacheClient) Cache(ctx context.Context, in *RaftCacheRequest, opts ...grpc.CallOption) (*RaftCacheReply, error) {
	out := new(RaftCacheReply)
	err := c.cc.Invoke(ctx, "/raftcache.RaftCache/Cache", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *raftCacheClient) ConfChange(ctx context.Context, in *ConfChangeRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, "/raftcache.RaftCache/ConfChange", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RaftCacheServer is the server API for RaftCache service.
// All implementations must embed UnimplementedRaftCacheServer
// for forward compatibility
type RaftCacheServer interface {
	Cache(context.Context, *RaftCacheRequest) (*RaftCacheReply, error)
	ConfChange(context.Context, *ConfChangeRequest) (*emptypb.Empty, error)
	mustEmbedUnimplementedRaftCacheServer()
}

// UnimplementedRaftCacheServer must be embedded to have forward compatible implementations.
type UnimplementedRaftCacheServer struct {
}

func (UnimplementedRaftCacheServer) Cache(context.Context, *RaftCacheRequest) (*RaftCacheReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Cache not implemented")
}
func (UnimplementedRaftCacheServer) ConfChange(context.Context, *ConfChangeRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ConfChange not implemented")
}
func (UnimplementedRaftCacheServer) mustEmbedUnimplementedRaftCacheServer() {}

// UnsafeRaftCacheServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to RaftCacheServer will
// result in compilation errors.
type UnsafeRaftCacheServer interface {
	mustEmbedUnimplementedRaftCacheServer()
}

func RegisterRaftCacheServer(s grpc.ServiceRegistrar, srv RaftCacheServer) {
	s.RegisterService(&RaftCache_ServiceDesc, srv)
}

func _RaftCache_Cache_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RaftCacheRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftCacheServer).Cache(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/raftcache.RaftCache/Cache",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RaftCacheServer).Cache(ctx, req.(*RaftCacheRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RaftCache_ConfChange_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ConfChangeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftCacheServer).ConfChange(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/raftcache.RaftCache/ConfChange",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RaftCacheServer).ConfChange(ctx, req.(*ConfChangeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// RaftCache_ServiceDesc is the grpc.ServiceDesc for RaftCache service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var RaftCache_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "raftcache.RaftCache",
	HandlerType: (*RaftCacheServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Cache",
			Handler:    _RaftCache_Cache_Handler,
		},
		{
			MethodName: "ConfChange",
			Handler:    _RaftCache_ConfChange_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "internal/pkg/raftcache/raftcache.proto",
}