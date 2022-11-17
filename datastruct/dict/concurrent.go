package dict

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// ConcurrentDict is thread safe map using sharding lock
//用分片锁实现线程安全的map
type ConcurrentDict struct {
	table      []*shard
	count      int32
	shardCount int
}

type shard struct {
	m     map[string]interface{}
	mutex sync.RWMutex
}

//计算容量（看不懂这里边在干嘛）
func computeCapacity(param int) (size int) {
	if param <= 16 {
		return 16
	}
	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n < 0 {
		return math.MaxInt32
	}
	return n + 1
}

// MakeConcurrent creates ConcurrentDict with the given shard count
//用给定的分片数初始化新的ConcurrentDict，主要是把其中table里的所有shard给make出来
func MakeConcurrent(shardCount int) *ConcurrentDict {
	shardCount = computeCapacity(shardCount)
	table := make([]*shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &shard{
			m: make(map[string]interface{}),
		}
	}
	d := &ConcurrentDict{
		count:      0,
		table:      table,
		shardCount: shardCount,
	}
	return d
}

const prime32 = uint32(16777619)

//fnv32 哈希算法，复杂度为KEY的长度
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

//通过哈希值定位分片。
//本算法保证定位到的分片一定不超过table容量，而且尽可能均匀分布在table中。
func (dict *ConcurrentDict) spread(hashCode uint32) uint32 {
	if dict == nil {
		panic("dict is nil")
	}
	tableSize := uint32(len(dict.table))
	return (tableSize - 1) & hashCode
}

//通过索引得到分片，其实就是直接返回table[index]的value
func (dict *ConcurrentDict) getShard(index uint32) *shard {
	if dict == nil {
		panic("dict is nil")
	}
	return dict.table[index]
}

// Get returns the binding value and whether the key is exist
//通过Key来获取对应的val值，并返回存在状态。
//执行过程：首先求出key的hash值，求出index后从table中取出这个分片，
//然后在分片中再次取出map中对应的value（加锁时，只对这个分片进行加锁，不加整个dict的锁）
func (dict *ConcurrentDict) Get(key string) (val interface{}, exists bool) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	val, exists = s.m[key]
	return
}

// Len returns the number of dict
//返回dict中数据的数量（也就是count的值）
func (dict *ConcurrentDict) Len() int {
	if dict == nil {
		panic("dict is nil")
	}
	return int(atomic.LoadInt32(&dict.count))
}

// Put puts key value into dict and returns the number of new inserted key-value
//向ConcurrentDict中插入K-V对，返回多插入的对数（原来就有的话，覆盖原来的value并返回1。
//注意，不管原来是有还是没有，都会让count+1（为什么要这么设计？）好像addCount的调用时间有问题！
func (dict *ConcurrentDict) Put(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)
	dict.addCount()
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 0
	}
	s.m[key] = val
	return 1
}

// PutIfAbsent puts value if the key is not exists and returns the number of updated key-value
//只有当KEY不存在才会成功Put的方法，如果不存在则返回0，并切不会调用 addCount 方法。
//存在则返回1，并调用 addCount 方法
func (dict *ConcurrentDict) PutIfAbsent(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.m[key]; ok {
		return 0
	}
	s.m[key] = val
	dict.addCount()
	return 1
}

// PutIfExists puts value if the key is exist and returns the number of inserted key-value
//只有当KEY已经存在才会调用的 Put 方法。
//如果修改成功，则会返回1，修改失败会直接返回0。
//全程都不会调用 addCount 方法
func (dict *ConcurrentDict) PutIfExists(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 1
	}
	return 0
}

// Remove removes the key and return the number of deleted key-value
//删除K-V对，并返回成功执行的条数。本方法会调用 decreaseCount 方法
func (dict *ConcurrentDict) Remove(key string) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.m[key]; ok {
		delete(s.m, key)
		dict.decreaseCount()
		return 1
	}
	return 0
}

//原子的给dict中的数据数（count）+1
func (dict *ConcurrentDict) addCount() int32 {
	return atomic.AddInt32(&dict.count, 1)
}

//原子的给dict中的数据数（count）-1
func (dict *ConcurrentDict) decreaseCount() int32 {
	return atomic.AddInt32(&dict.count, -1)
}

// ForEach traversal the dict
// it may not visits new entry inserted during traversal
//对ConcurrentDict中的每一个KV进行操作，具体的操作是consumer函数，需要自行传入。
//一旦对某一对KV的consumer执行返回了false，会立刻返回。
//对每一个KV操作时，会上读锁，也就是说操作不能修改这个KV对。
func (dict *ConcurrentDict) ForEach(consumer Consumer) {
	if dict == nil {
		panic("dict is nil")
	}

	for _, s := range dict.table {
		s.mutex.RLock()
		func() {
			defer s.mutex.RUnlock()
			for key, value := range s.m {
				continues := consumer(key, value)
				if !continues {
					return
				}
			}
		}()
	}
}

// Keys returns all keys in dict
//遍历ConcurrentDict，将其中的key全部取出。
//注意：实际存在KV数量大于count（为什么会存在这种情况？）时，仍会继续添加进结果集中。
func (dict *ConcurrentDict) Keys() []string {
	keys := make([]string, dict.Len())
	i := 0
	dict.ForEach(func(key string, val interface{}) bool {
		if i < len(keys) {
			keys[i] = key
			i++
		} else {
			keys = append(keys, key)
		}
		return true
	})
	return keys
}

// RandomKey returns a key randomly
//随机返回分片中的一个KEY。其实就是直接遍历这个分片，然后返回遇上的key
func (shard *shard) RandomKey() string {
	if shard == nil {
		panic("shard is nil")
	}
	shard.mutex.RLock()
	defer shard.mutex.RUnlock()

	for key := range shard.m {
		return key
	}
	return ""
}

// RandomKeys randomly returns keys of the given number, may contain duplicated key
//随机取limit个KEY，要求的数字大于最大KV数，则直接调用Keys并返回。
//可能会包含重复的KEY，事实上可能会包括很多重复的KEY······
func (dict *ConcurrentDict) RandomKeys(limit int) []string {
	size := dict.Len()
	//要求的数字大于最大KV数，则直接调用Keys并返回
	if limit >= size {
		return dict.Keys()
	}
	shardCount := len(dict.table)

	result := make([]string, limit)
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < limit; {
		s := dict.getShard(uint32(nR.Intn(shardCount)))
		if s == nil {
			continue
		}
		key := s.RandomKey()
		if key != "" {
			result[i] = key
			i++
		}
	}
	return result
}

// RandomDistinctKeys randomly returns keys of the given number, won't contain duplicated key
//随机返回不同的limit个key，要求的数字大于最大KV数，则直接调用Keys并返回。
//感觉其实完成不了这项功能！如果有多个在同一个分片的数据存在，可能永远都无法取出他们后面的数据。
func (dict *ConcurrentDict) RandomDistinctKeys(limit int) []string {
	size := dict.Len()
	if limit >= size {
		return dict.Keys()
	}

	shardCount := len(dict.table)
	result := make(map[string]struct{})
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	for len(result) < limit {
		shardIndex := uint32(nR.Intn(shardCount))
		s := dict.getShard(shardIndex)
		if s == nil {
			continue
		}
		key := s.RandomKey()
		if key != "" {
			if _, exists := result[key]; !exists {
				result[key] = struct{}{}
			}
		}
	}
	arr := make([]string, limit)
	i := 0
	for k := range result {
		arr[i] = k
		i++
	}
	return arr
}

// Clear removes all keys in dict
func (dict *ConcurrentDict) Clear() {
	*dict = *MakeConcurrent(dict.shardCount)
}
