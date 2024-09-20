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

/* For State Writer */
func (redisTx *RedisDB) Put(table string, k, v []byte) error {
	return redisTx.rdb.Set(redisTx.ctx, hex.EncodeToString(k), v, 0).Err()
}

func (redisTx *RedisDB) Delete(table string, k []byte) error {
	return redisTx.rdb.Del(redisTx.ctx, hex.EncodeToString(k)).Err()
}

func (redisTx *RedisDB) IncrementSequence(bucket string, amount uint64) (uint64, error) {
	return 0, nil // Unimplemented
}

/* For State Reader */
func (redisTx *RedisDB) Has(table string, key []byte) (bool, error) {
	return redisTx.rdb.Exists(redisTx.ctx, hex.EncodeToString(key)).Val() == 1, nil
}

func (redisTx *RedisDB) GetOne(table string, key []byte) (val []byte, err error) {
	return redisTx.rdb.Get(redisTx.ctx, hex.EncodeToString(key)).Bytes()
}

func (redisTx *RedisDB) ForEach(table string, fromPrefix []byte, walker func(k, v []byte) error) error {
	return nil // Unimplemented
}

func (redisTx *RedisDB) ForPrefix(table string, prefix []byte, walker func(k, v []byte) error) error {
	return nil // Unimplemented
}

func (redisTx *RedisDB) ForAmount(table string, prefix []byte, amount uint32, walker func(k, v []byte) error) error {
	return nil // Unimplemented
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
