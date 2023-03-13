package command

import (
	"container/list"
)

type element struct {
	cmd  []byte
	body []byte
}

type CommandQueue struct {
	list *list.List
}

func NewCmdQueue() (q *CommandQueue) {
	q.list = list.New()
	return q
}

func (q *CommandQueue) PushCmd(cmd []byte, body []byte) {
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
