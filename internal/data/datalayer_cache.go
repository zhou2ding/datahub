package data

import (
	"context"
	v1 "datahub/api/datalayer/v1"
	"datahub/internal/biz"
	"google.golang.org/protobuf/types/known/emptypb"
	"time"

	"github.com/go-kratos/kratos/v2/log"
)

const (
	defaultCacheTTL = 4 * time.Hour
)

type CachingDatalayerRepo struct {
	wrapped biz.DatalayerRepo
	cache   *RedisClient
	log     *log.Helper
}

func NewCachingDatalayerRepo(wrapped *DatalayerRepo, cache *RedisClient, logger log.Logger) *CachingDatalayerRepo {
	return &CachingDatalayerRepo{
		wrapped: wrapped,
		cache:   cache,
		log:     log.NewHelper(logger),
	}
}

func (r *CachingDatalayerRepo) BeginTransaction(ctx context.Context, req *v1.BeginTransactionRequest) (*v1.BeginTransactionResponse, error) {
	return r.wrapped.BeginTransaction(ctx, req)
}

func (r *CachingDatalayerRepo) CommitTransaction(ctx context.Context, req *v1.TransactionRequest) (*emptypb.Empty, error) {
	return r.wrapped.CommitTransaction(ctx, req)
}

func (r *CachingDatalayerRepo) RollbackTransaction(ctx context.Context, req *v1.TransactionRequest) (*emptypb.Empty, error) {
	return r.wrapped.RollbackTransaction(ctx, req)
}

func (r *CachingDatalayerRepo) ListTables(ctx context.Context, req *v1.ListTablesRequest) (*v1.ListTablesResponse, error) {
	return r.wrapped.ListTables(ctx, req)
}

func (r *CachingDatalayerRepo) DescribeTable(ctx context.Context, req *v1.DescribeTableRequest) (*v1.DescribeTableResponse, error) {
	return r.wrapped.DescribeTable(ctx, req)
}

func (r *CachingDatalayerRepo) ExecRawSQL(ctx context.Context, req *v1.ExecRawSQLRequest) (*v1.ExecRawSQLResponse, error) {
	return r.wrapped.ExecRawSQL(ctx, req)
}
