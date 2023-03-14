package timer

import "time"

type Timer struct {
	ticker  *time.Ticker
	isStart bool
}

func NewTimer() (t *Timer) {
	t.ticker = time.NewTicker(1 * time.Second)
	t.isStart = false
	return t
}

func (t *Timer) Start(task func()) {
	if !t.isStart {
		t.isStart = true
	} else {
		return
	}
	go func() {
		for range t.ticker.C {
			task()
		}
	}()
}

func (t *Timer) Stop() {
	t.ticker.Stop()
}
