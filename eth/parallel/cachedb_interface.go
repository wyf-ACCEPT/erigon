package parallel_tests

import (
	"encoding/gob"
	"encoding/hex"
	"errors"
	"os"
	"sync"
)

/* Thread-safe map structure */
type CacheDB struct {
	data map[string][]byte
	mu   sync.RWMutex
}

/* For State Writer */
func (cacheTx *CacheDB) Put(table string, k, v []byte) error {
	cacheTx.mu.Lock()
	defer cacheTx.mu.Unlock()
	cacheTx.data[hex.EncodeToString(k)] = v
	return nil
}

func (cacheTx *CacheDB) Delete(table string, k []byte) error {
	cacheTx.mu.Lock()
	defer cacheTx.mu.Unlock()
	delete(cacheTx.data, hex.EncodeToString(k))
	return nil
}

func (cacheTx *CacheDB) IncrementSequence(bucket string, amount uint64) (uint64, error) {
	return 0, nil // Unimplemented
}

/* For State Reader */
func (cacheTx *CacheDB) Has(table string, key []byte) (bool, error) {
	cacheTx.mu.RLock()
	defer cacheTx.mu.RUnlock()
	_, exists := cacheTx.data[hex.EncodeToString(key)]
	return exists, nil
}

func (cacheTx *CacheDB) GetOne(table string, key []byte) (val []byte, err error) {
	cacheTx.mu.RLock()
	defer cacheTx.mu.RUnlock()
	value, exists := cacheTx.data[hex.EncodeToString(key)]
	if !exists {
		return nil, errors.New("key not found")
	}
	return value, nil
}

func (cacheTx *CacheDB) ForEach(table string, fromPrefix []byte, walker func(k, v []byte) error) error {
	return nil // Unimplemented
}

func (cacheTx *CacheDB) ForPrefix(table string, prefix []byte, walker func(k, v []byte) error) error {
	return nil // Unimplemented
}

func (cacheTx *CacheDB) ForAmount(table string, prefix []byte, amount uint32, walker func(k, v []byte) error) error {
	return nil // Unimplemented
}

func NewCacheDB() *CacheDB {
	return &CacheDB{
		data: make(map[string][]byte),
	}
}

func (cachedb *CacheDB) SaveMapToFile(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	err = encoder.Encode(cachedb.data)
	if err != nil {
		return err
	}
	return nil
}

func LoadMapFromFile(filename string) (*CacheDB, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var data map[string][]byte
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&data)
	if err != nil {
		return nil, err
	}
	return &CacheDB{data: data}, nil
}
