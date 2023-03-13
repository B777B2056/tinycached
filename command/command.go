package command

import (
	"errors"
	"strconv"
	"strings"
	"tinycached/cache"
	"tinycached/persistence"
	"tinycached/utils"
)

type CacheClientInfo struct {
	group     *cache.CacheGroup
	isCAS     bool          // 客户端监视的key中，是否至少有一个已经被其他客户端修改
	isInMulti bool          // 是否位于事务状态
	queue     *CommandQueue // 事务命令队列
}

func NewCacheClient() (clt *CacheClientInfo) {
	clt.group = nil
	clt.isCAS = false
	clt.isInMulti = false
	clt.queue = NewCmdQueue()
	return clt
}

func (clt *CacheClientInfo) ExecCmd(cmd []byte, body []byte) (ret []byte, err error) {
	aof := persistence.AofInstance()
	group := clt.group
	elem := strings.Split(string(body), " ")
	switch string(cmd) {
	case utils.SELECT.String():
		if len(elem) < 1 {
			return nil, errors.New("wrong command")
		}

		aof.Append(cmd, body)
		if clt.isInMulti {
			clt.queue.PushCmd(cmd, body)
			return []byte("QUEUED"), nil
		} else {
			clt.group = cache.GetCacheGroup(elem[0])
			return []byte("DONE"), nil
		}

	case utils.GET.String():
		if len(elem) < 1 {
			return nil, errors.New("wrong command")
		}

		if clt.isInMulti {
			clt.queue.PushCmd(cmd, body)
			return []byte("QUEUED"), nil
		} else {
			val, err := group.Get(elem[0])
			if err != nil {
				return nil, err
			}
			return val, nil
		}

	case utils.SET.String():
		if len(elem) < 2 {
			return nil, errors.New("wrong command")
		}

		aof.Append(cmd, body) // 记录SET命令

		if clt.isInMulti {
			clt.queue.PushCmd(cmd, body)
			NotifyModifyed(string(elem[0]))
			return []byte("QUEUED"), nil
		} else {
			group.Add(elem[0], []byte(elem[1]))
			NotifyModifyed(string(elem[0]))
			return []byte("DONE"), nil
		}

	case utils.DEL.String():
		if len(elem) < 1 {
			return nil, errors.New("wrong command")
		}

		aof.Append(cmd, body) // 记录DEL命令
		if clt.isInMulti {
			clt.queue.PushCmd(cmd, body)
			NotifyModifyed(string(elem[0]))
			return []byte("QUEUED"), nil
		} else {
			group.Del(elem[0])
			NotifyModifyed(string(elem[0]))
			return []byte("DONE"), nil
		}

	case utils.EXPR.String():
		if len(elem) < 2 {
			return nil, errors.New("wrong command")
		}

		aof.Append(cmd, body) // 记录EXPR命令
		if clt.isInMulti {
			clt.queue.PushCmd(cmd, body)
			NotifyModifyed(string(elem[0]))
			return []byte("QUEUED"), nil
		} else {
			t, err := strconv.ParseInt(elem[1], 10, 64)
			if err != nil {
				return nil, err
			}
			group.SetExpireTimeMs(elem[0], t)
			NotifyModifyed(string(elem[0]))
			return []byte("DONE"), nil
		}

	case utils.MULTI.String():
		clt.isInMulti = true
		aof.Append(cmd, body)
		return []byte("DONE"), nil

	case utils.EXEC.String():
		aof.Append(cmd, body)
		DelWatchKey(string(body), clt)
		if !clt.isCAS {
			return nil, errors.New("NIL")
		}
		ret, err := clt.queue.ExecCmds(clt)
		clt.isInMulti = false
		return ret, err

	case utils.DISCARD.String():
		clt.isInMulti = false
		aof.Append(cmd, body)
		return []byte("DONE"), nil

	case utils.WATCH.String():
		if clt.isInMulti {
			return nil, errors.New("NIL")
		} else {
			aof.Append(cmd, body)
			AddWatchKey(string(body), clt)
			return []byte("DONE"), nil
		}

	case utils.UNWATCH.String():
		if clt.isInMulti {
			return nil, errors.New("NIL")
		} else {
			aof.Append(cmd, body)
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
