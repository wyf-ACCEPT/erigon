package parallel_tests

import (
	originContext "context"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

var ctx = originContext.Background()
var rdb = redis.NewClient(&redis.Options{
	Addr:     "localhost:6379",
	DB:       0,
	Password: "",
})

func TestRedisGet(t *testing.T) {
	cursor := uint64(0)
	keys := int64(10000)
	res, _, err := rdb.Scan(ctx, cursor, "*", keys).Result()
	if err != nil {
		panic(err)
	}

	fmt.Println(len(res))

	var values1 []interface{}
	var values2 []interface{}

	timeStart1 := time.Now()
	for _, key := range res {
		value, err := rdb.Get(ctx, key).Result()
		if err != nil {
			panic(err)
		} else {
			values1 = append(values1, value)
		}
	}
	fmt.Printf("Time taken to get %d keys (separately): %v\n", keys, time.Since(timeStart1))

	timeStart2 := time.Now()
	values2, err = rdb.MGet(ctx, res...).Result()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Time taken to get %d keys (together): %v\n", keys, time.Since(timeStart2))

	if len(values1) != len(values2) {
		panic("Lengths don't match")
	} else {
		for i := range values1 {
			if values1[i] != values2[i] {
				panic(fmt.Sprintf("Values don't match at %d", i))
			}
		}
	}
}
