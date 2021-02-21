package tock

import "time"

type Ticker interface {
	Reset(d time.Duration)
	Stop()
	Chan() <-chan time.Time
}
