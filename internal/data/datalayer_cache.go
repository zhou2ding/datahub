package data

import (
	"context"
	v1 "datahub/api/datalayer/v1"
	"datahub/internal/biz"
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

func (r *CachingDatalayerRepo) ExecRawSQL(ctx context.Context, req *v1.ExecRawSQLRequest) (*v1.ExecRawSQLResponse, error) {
	return r.wrapped.ExecRawSQL(ctx, req)
}
