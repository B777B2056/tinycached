package utils

import "time"

type Timer struct {
	ticker  *time.Ticker
	isStart bool
}

func NewTimer(second int) (t *Timer) {
	t = &Timer{ticker: time.NewTicker(time.Duration(second) * time.Second), isStart: false}
	return t
}

func (t *Timer) Start(task func() bool) {
	if !t.isStart {
		t.isStart = true
	} else {
		return
	}

	go func() {
		for range t.ticker.C {
			if !task() {
				break
			}
		}
	}()
}

func (t *Timer) Stop() {
	t.ticker.Stop()
}
