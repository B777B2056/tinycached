package command

import (
	"errors"
	"strconv"
	"strings"
	"tinycached/server/cache"
	"tinycached/server/persistence"
	"tinycached/utils"
)

type CacheClientInfo struct {
	cache     *cache.Cache
	isCAS     bool          // 客户端监视的key中，是否至少有一个已经被其他客户端修改
	isInMulti bool          // 是否位于事务状态
	queue     *CommandQueue // 事务命令队列
}

func NewCacheClient() (clt *CacheClientInfo) {
	clt = &CacheClientInfo{
		cache:     cache.New(512),
		isCAS:     false,
		isInMulti: false,
		queue:     NewCmdQueue(),
	}
	return clt
}

func (clt *CacheClientInfo) ExecCmd(cmd utils.CmdType, body string) (ret []byte, err error) {
	aof := persistence.AofInstance()
	elem := strings.Split(string(body), ":")
	switch cmd {
	case utils.GET:
		if len(elem) < 1 {
			return nil, errors.New("wrong command")
		}

		if clt.isInMulti {
			clt.queue.PushCmd(cmd, body)
			return []byte("QUEUED"), nil
		} else {
			val, ok := clt.cache.Get(elem[0])
			if !ok {
				return nil, errors.New("key is not exits")
			}
			return val, nil
		}

	case utils.SET:
		if len(elem) < 2 {
			return nil, errors.New("wrong command")
		}

		aof.Append([]byte(cmd.String()), []byte(body)) // 记录SET命令

		if clt.isInMulti {
			clt.queue.PushCmd(cmd, body)
			NotifyModifyed(string(elem[0]))
			return []byte("QUEUED"), nil
		} else {
			clt.cache.Add(elem[0], []byte(elem[1]))
			NotifyModifyed(string(elem[0]))
			return []byte("DONE"), nil
		}

	case utils.DEL:
		if len(elem) < 1 {
			return nil, errors.New("wrong command")
		}

		aof.Append([]byte(cmd.String()), []byte(body)) // 记录DEL命令
		if clt.isInMulti {
			clt.queue.PushCmd(cmd, body)
			NotifyModifyed(string(elem[0]))
			return []byte("QUEUED"), nil
		} else {
			clt.cache.Del(elem[0])
			NotifyModifyed(string(elem[0]))
			return []byte("DONE"), nil
		}

	case utils.EXPR:
		if len(elem) < 2 {
			return nil, errors.New("wrong command")
		}

		aof.Append([]byte(cmd.String()), []byte(body)) // 记录EXPR命令
		if clt.isInMulti {
			clt.queue.PushCmd(cmd, body)
			NotifyModifyed(string(elem[0]))
			return []byte("QUEUED"), nil
		} else {
			t, err := strconv.ParseInt(elem[1], 10, 64)
			if err != nil {
				return nil, err
			}
			clt.cache.SetExpireTimeMs(elem[0], t)
			NotifyModifyed(string(elem[0]))
			return []byte("DONE"), nil
		}

	case utils.MULTI:
		clt.isInMulti = true
		aof.Append([]byte(cmd.String()), []byte(body))
		return []byte("DONE"), nil

	case utils.EXEC:
		aof.Append([]byte(cmd.String()), []byte(body))
		DelWatchKey(string(body), clt)
		if !clt.isCAS {
			return nil, errors.New("NIL")
		}
		ret, err := clt.queue.ExecCmds(clt)
		clt.isInMulti = false
		return ret, err

	case utils.DISCARD:
		clt.isInMulti = false
		aof.Append([]byte(cmd.String()), []byte(body))
		return []byte("DONE"), nil

	case utils.WATCH:
		if clt.isInMulti {
			return nil, errors.New("NIL")
		} else {
			aof.Append([]byte(cmd.String()), []byte(body))
			AddWatchKey(string(body), clt)
			return []byte("DONE"), nil
		}

	case utils.UNWATCH:
		if clt.isInMulti {
			return nil, errors.New("NIL")
		} else {
			aof.Append([]byte(cmd.String()), []byte(body))
			DelWatchKey(string(body), clt)
			return []byte("DONE"), nil
		}

	default:
		// 请求的格式出错
		return nil, errors.New("wrong command")
	}
}

// func (clt *CacheClientInfo) execSelectCmd(arg []byte) []byte {

// }

// func (clt *CacheClientInfo) execGetCmd(arg []byte) []byte {

// }

// func (clt *CacheClientInfo) execSetCmd(arg []byte) []byte {

// }

// func (clt *CacheClientInfo) execDelCmd(arg []byte) []byte {

// }

// func (clt *CacheClientInfo) execExprCmd(arg []byte) []byte {

// }

// func (clt *CacheClientInfo) execMultiCmd(arg []byte) []byte {

// }

// func (clt *CacheClientInfo) execExecCmd(arg []byte) []byte {

// }

// func (clt *CacheClientInfo) execDiscardCmd(arg []byte) []byte {

// }

// func (clt *CacheClientInfo) execWatchCmd(arg []byte) []byte {

// }

// func (clt *CacheClientInfo) execUnwatchCmd(arg []byte) []byte {

// }
