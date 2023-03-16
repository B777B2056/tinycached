package command

import (
	"container/list"
	"tinycached/utils"
)

type element struct {
	cmd  utils.CmdType
	body string
}

type CommandQueue struct {
	list *list.List
}

func NewCmdQueue() (q *CommandQueue) {
	q = &CommandQueue{list: list.New()}
	return q
}

func (q *CommandQueue) PushCmd(cmd utils.CmdType, body string) {
	q.list.PushBack(&element{cmd, body})
}

func (q *CommandQueue) ExecCmds(clt *CacheClientInfo) (ret []byte, err error) {
	for q.list.Len() > 0 {
		elem := q.list.Front()

		cmd := elem.Value.(*element).cmd
		body := elem.Value.(*element).body
		ret, err = clt.ExecCmd(cmd, body)
		if err != nil {
			q.discardAllCmds()
			return nil, err
		}

		q.list.Remove(elem)
	}
	return ret, err
}

func (q *CommandQueue) discardAllCmds() {
	for q.list.Len() > 0 {
		q.list.Remove(q.list.Front())
	}
}
