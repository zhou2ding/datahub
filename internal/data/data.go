package data

import (
	"datahub/api/datalayer/v1"
	"datahub/internal/conf"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-redis/redis"
	"github.com/google/wire"
)

var ProviderSet = wire.NewSet(
	NewRedisClients,
)

type RedisClient struct {
	clients map[int32]*redis.Client
}

func NewRedisClients(c *conf.Data, logger log.Logger) (*RedisClient, error) {
	rdb := &RedisClient{
		clients: make(map[int32]*redis.Client),
	}

	// 初始化所有db的客户端
	for dbNum := range v1.RedisDB_name {
		if dbNum != int32(v1.RedisDB_UNSPECIFIED) {
			client := redis.NewFailoverClient(&redis.FailoverOptions{
				MasterName:    c.Redis.Master,
				SentinelAddrs: c.Redis.SentinelAddrs,
				Password:      c.Redis.Password,
				DB:            int(dbNum - 1), //redis的db从0开始
			})

			if _, err := client.Ping().Result(); err != nil {
				log.NewHelper(logger).Errorf("connect to redis %d error: %v", dbNum, err)
				return nil, err
			}

			rdb.clients[dbNum] = client
		}
	}

	return rdb, nil
}
