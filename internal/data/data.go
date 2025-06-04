package data

import (
	"datahub/api/datalayer/v1"
	"datahub/internal/conf"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-redis/redis"
	"github.com/google/uuid"
	"github.com/google/wire"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormLogger "gorm.io/gorm/logger"
	"sync"
	"time"
)

var ProviderSet = wire.NewSet(
	NewRedisClients,
)

type Data struct {
	db           map[string]*gorm.DB
	cache        *RedisClient
	transactions map[string]*gorm.DB // 存储活跃的事务，键是事务ID，值是事务对象
	txMu         sync.RWMutex        // 用于保护 transactions map 的读写锁
}

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

func (r *RedisClient) GetRedis(num int32) *redis.Client {
	return r.clients[num]
}

type ormLogger struct {
	*log.Helper
}

func (o *ormLogger) Printf(format string, args ...interface{}) {
	o.Infof(format, args...)
}

func NewDatabase(c *conf.Data, l *conf.Log, logger log.Logger) (map[string]*gorm.DB, error) {
	dbs := make(map[string]*gorm.DB)
	for _, source := range c.Databases {
		db, err := gorm.Open(mysql.Open(source.Dsn), &gorm.Config{})
		if err != nil {
			log.NewHelper(logger).Errorf("connect to dib error: %v", err)
			return nil, err
		}
		if l.Stdout {
			db.Logger = gormLogger.New(&ormLogger{log.NewHelper(logger)}, gormLogger.Config{
				SlowThreshold:             time.Second,
				LogLevel:                  gormLogger.Info,
				IgnoreRecordNotFoundError: false,
				Colorful:                  false,
			})
		}
		dbs[source.Name] = db
	}

	return dbs, nil
}

func (d *Data) BeginTransaction(dbName string) (string, *gorm.DB, error) {
	tx := d.db[dbName].Begin()
	if tx.Error != nil {
		return "", nil, tx.Error
	}

	txID := uuid.NewString()

	d.txMu.Lock()
	d.transactions[txID] = tx
	d.txMu.Unlock()

	return txID, tx, nil
}
