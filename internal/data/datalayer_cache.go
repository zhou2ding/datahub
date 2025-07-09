package data

import (
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
