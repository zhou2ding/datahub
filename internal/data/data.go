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
	"io"
	"sync"
	"time"

	gormLogger "gorm.io/gorm/logger"
	stdLog "log"
)

var ProviderSet = wire.NewSet(
	NewData,
	NewDatabase,
	NewRedisClients,
	NewDatalayerRepo,
	NewCachingDatalayerRepo,
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

type ormLogger struct {
	*log.Helper
}

func (o *ormLogger) Printf(format string, args ...interface{}) {
	o.Debugf(format, args...)
}

func NewData(c *conf.Data, logger log.Logger, dbs map[string]*gorm.DB, cache *RedisClient) (*Data, func(), error) {
	d := &Data{db: dbs, cache: cache, transactions: make(map[string]*gorm.DB)}
	cleanup := func() {
		log.NewHelper(logger).Info("closing the data resources")

		//关闭数据库连接
		for _, db := range d.db {
			sql, _ := db.DB()
			_ = sql.Close()
		}

		//关闭redis连接
		for _, client := range cache.clients {
			_ = client.Close()
		}

		// 清理未完成的事务
		d.txMu.Lock()
		if len(d.transactions) > 0 {
			log.NewHelper(logger).Warnf("found %d unfinished transactions during cleanup. Attempting rollback.", len(d.transactions))
			for id, tx := range d.transactions {
				log.NewHelper(logger).Infof("rolling back transaction %s", id)
				_ = tx.Rollback()
			}
			d.transactions = make(map[string]*gorm.DB)
		}
		d.txMu.Unlock()
	}
	return d, cleanup, nil
}

func NewDatabase(c *conf.Data, l *conf.Log, logger log.Logger) (map[string]*gorm.DB, error) {
	dbs := make(map[string]*gorm.DB)
	for _, source := range c.Databases {
		db, err := gorm.Open(mysql.Open(source.Dsn), &gorm.Config{})
		if err != nil {
			log.NewHelper(logger).Errorf("connect to dib error: %v", err)
			return nil, err
		}
		if l.Level == "debug" {
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

func NewRedisClients(c *conf.Data, l *conf.Log, logger log.Logger) (*RedisClient, error) {
	if l.Level != "debug" {
		redis.SetLogger(stdLog.New(io.Discard, "", 0))
	}
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

func (d *Data) GetTransaction(transactionId string) (*gorm.DB, bool) {
	if transactionId == "" {
		return nil, false
	}
	d.txMu.RLock()
	defer d.txMu.RUnlock()

	tx, ok := d.transactions[transactionId]
	return tx, ok
}

func (d *Data) RemoveTransaction(transactionId string) {
	if transactionId == "" {
		return
	}
	d.txMu.Lock()
	defer d.txMu.Unlock()
	delete(d.transactions, transactionId)
}
