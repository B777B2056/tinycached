package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

type HashFunc func([]byte) uint32

type Map struct {
	hash     HashFunc
	replicas int            //虚拟节点倍数
	keys     []int          // 哈希环
	hashmap  map[int]string //虚拟节点与真实节点的映射表：key为虚拟节点哈希值，value为对应的真实节点名称
}

func NewConsistentHash(replicas int, hash HashFunc) (c *Map) {
	c = &Map{
		hash:     hash,
		replicas: replicas,
		hashmap:  make(map[int]string),
	}
	if hash == nil {
		c.hash = crc32.ChecksumIEEE
	}
	return c
}

// 添加节点到一致性哈希上
func (c *Map) AddNode(key string) {
	if key == "" {
		return
	}

	for i := 0; i < c.replicas; i++ {
		// 对每一个真实节点 key，对应创建 replicas个虚拟节点
		// 虚拟节点的名称是：strconv.Itoa(i) + key，通过添加编号的方式区分不同虚拟节点
		hashVal := int(c.hash([]byte(strconv.Itoa(i) + key)))
		c.keys = append(c.keys, hashVal)
		c.hashmap[hashVal] = key
	}
	// 环上哈希值排序
	sort.Ints(c.keys)
}

// 获取节点
func (c *Map) FindNode(key string) string {
	if key == "" {
		return ""
	}

	hashVal := int(c.hash([]byte(key)))
	idx := sort.Search(len(c.keys), func(i int) bool {
		return c.keys[i] >= hashVal
	})
	return c.hashmap[c.keys[idx%len(c.keys)]]
}

// 移除一致性哈希上的节点
func (c *Map) RemoveNode(key string) {
	if key == "" {
		return
	}

	for i := 0; i < c.replicas; i++ {
		hashVal := int(c.hash([]byte(strconv.Itoa(i) + key)))
		idx := sort.SearchInts(c.keys, hashVal)
		c.keys = append(c.keys[:idx], c.keys[idx+1:]...)
		delete(c.hashmap, hashVal)
	}
}
