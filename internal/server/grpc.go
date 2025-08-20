package server

import (
	"datahub/api/datalayer/v1"
	"datahub/internal/conf"
	"datahub/internal/service"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware/metadata"
	"github.com/go-kratos/kratos/v2/middleware/recovery"
	"github.com/go-kratos/kratos/v2/transport/grpc"
)

func NewGRPCServer(c *conf.Server, datalayer *service.DatalayerService, logger log.Logger) *grpc.Server {
	var opts = []grpc.ServerOption{
		grpc.Middleware(
			recovery.Recovery(),
			metadata.Server(),
		),
	}
	if c.Grpc.Addr != "" {
		opts = append(opts, grpc.Address(c.Grpc.Addr))
	}
	if c.Grpc.Timeout != nil {
		opts = append(opts, grpc.Timeout(c.Grpc.Timeout.AsDuration()))
	}
	srv := grpc.NewServer(opts...)
	v1.RegisterDataCRUDServer(srv, datalayer)
	v1.RegisterMetadataServer(srv, datalayer)
	v1.RegisterRawSqlServer(srv, datalayer)
	return srv
}
