package utils

import "strings"

var maxCmdSize = 8
var maxArgSize = 256

type fsmState interface {
	doAction(ctx *fsmContext)
}

type cmdState struct {
	cmd []byte
}

func (s *cmdState) doAction(ctx *fsmContext) {
	if s.cmd == nil {
		s.cmd = make([]byte, 0)
	}
	if len(s.cmd) > maxCmdSize {
		ctx.setState(&errState{})
	} else if ctx.curByte == ' ' {
		ctx.cmd = string(s.cmd)
		ctx.setState(&argState{})
	} else if ctx.curByte == '\n' {
		ctx.cmd = string(s.cmd)
		ctx.setState(&endState{})
	} else {
		s.cmd = append(s.cmd, ctx.curByte)
	}
}

type argState struct {
	arg []byte
}

func (s *argState) doAction(ctx *fsmContext) {
	if s.arg == nil {
		s.arg = make([]byte, 0)
	}

	if len(s.arg) > maxArgSize {
		ctx.setState(&errState{})
	} else if ctx.curByte == '\n' {
		ctx.args = strings.Split(string(s.arg), " ")
		ctx.setState(&endState{})
	} else {
		s.arg = append(s.arg, ctx.curByte)
	}
}

type endState struct {
}

func (s *endState) doAction(ctx *fsmContext) {
	ctx.setState(&cmdState{})
}

type errState struct {
}

func (s *errState) doAction(ctx *fsmContext) {
	ctx.setState(&cmdState{})
}

type fsmContext struct {
	curByte  byte
	curState fsmState
	cmd      string
	args     []string
}

func newContext() (ctx *fsmContext) {
	ctx = &fsmContext{
		curState: &cmdState{},
	}
	return ctx
}

func (ctx *fsmContext) getCmd() CmdType {
	switch strings.ToUpper(ctx.cmd) {
	case "GET":
		return GET
	case "SET":
		return SET
	case "DEL":
		return DEL
	case "EXPR":
		return EXPR
	case "MULTI":
		return MULTI
	case "EXEC":
		return EXEC
	case "DISCARD":
		return DISCARD
	case "WATCH":
		return WATCH
	case "UNWATCH":
		return UNWATCH
	default:
		return ERROR
	}
}

func (ctx *fsmContext) getState() fsmState {
	return ctx.curState
}

func (ctx *fsmContext) setState(state fsmState) {
	ctx.curState = state
}

func ParseFsm(recv func() (byte, bool)) (CmdType, []string) {
	ctx := newContext()
	var ok bool
	for {
		ctx.curByte, ok = recv()
		if !ok {
			break
		}

		state := ctx.getState()
		_, isEndState := state.(*endState)
		_, isErrState := state.(*errState)
		state.doAction(ctx)

		if isEndState || isErrState {
			if isErrState {
				return ERROR, nil
			}
			break
		}
	}
	return ctx.getCmd(), ctx.args
}
