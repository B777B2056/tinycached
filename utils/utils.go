package utils

type PolicyType int16

const (
	PolicyLRU PolicyType = iota
	PolicyLFU
)

type CmdParseFSMState int8

const (
	CmdSTATE CmdParseFSMState = iota
	BodySTATE
	EndSTATE
)

type CmdType int16

const (
	SELECT CmdType = iota
	GET
	SET
	DEL
	EXPR
	MULTI
	EXEC
	DISCARD
	WATCH
	UNWATCH
)

func (cmd CmdType) String() string {
	switch cmd {
	case SELECT:
		return "SELECT"
	case GET:
		return "GET"
	case SET:
		return "SET"
	case DEL:
		return "DEL"
	case EXPR:
		return "EXPR"
	case MULTI:
		return "MULTI"
	case EXEC:
		return "EXEC"
	case DISCARD:
		return "DISCARD"
	case WATCH:
		return "WATCH"
	case UNWATCH:
		return "UNWATCH"
	default:
		return ""
	}
}

func CopyBytes(src []byte) (dest []byte) {
	dest = make([]byte, len(src))
	copy(dest, src)
	return dest
}


