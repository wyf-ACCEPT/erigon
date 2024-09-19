package parallel_tests

import (
	originContext "context"
	"encoding/hex"

	"github.com/go-redis/redis/v8"
)

type RedisDB struct {
	rdb *redis.Client
	ctx originContext.Context
}

func (redisTx *RedisDB) Put(table string, k, v []byte) error {
	return redisTx.rdb.Set(redisTx.ctx, hex.EncodeToString(k), v, 0).Err()
}

func (redisTx *RedisDB) Delete(table string, k []byte) error {
	return redisTx.rdb.Del(redisTx.ctx, hex.EncodeToString(k)).Err()
}

func (redisTx *RedisDB) IncrementSequence(bucket string, amount uint64) (uint64, error) {
	return 0, nil // Unimplemented
}

func NewRedisDB() *RedisDB {
	return &RedisDB{
		rdb: redis.NewClient(&redis.Options{
			Addr:     "localhost:6379",
			DB:       0,
			Password: "",
		}),
		ctx: originContext.Background(),
	}
}
