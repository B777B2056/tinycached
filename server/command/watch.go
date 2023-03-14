package command

import (
	"container/list"
	"sync"
)

var watchMap sync.Map

func AddWatchKey(key string, clt *CacheClientInfo) {
	if val, ok := watchMap.Load(&key); !ok {
		cltList := list.New()
		cltList.PushBack(clt)
		watchMap.Store(&key, &cltList)
	} else {
		list := val.(*list.List)
		list.PushBack(clt)
	}
}

func NotifyModifyed(key string) {
	if val, ok := watchMap.Load(&key); ok {
		list := val.(*list.List)
		for i := list.Front(); i != nil; i = i.Next() {
			i.Value.(*CacheClientInfo).isCAS = true
		}
	}
}

func DelWatchKey(key string, clt *CacheClientInfo) {
	if val, ok := watchMap.Load(&key); ok {
		list := val.(*list.List)
		for i := list.Front(); i != nil; i = i.Next() {
			if clt == i.Value.(*CacheClientInfo) {
				list.Remove(i)
				break
			}
		}
	}
}
