package tock

import "time"

type Tick struct {
	ticker *time.Ticker
}

var _ Ticker = (*Tick)(nil)

func NewTick(d time.Duration) *Tick    { return &Tick{ticker: time.NewTicker(d)} }
func (t *Tick) Chan() <-chan time.Time { return t.ticker.C }
func (t *Tick) Reset(d time.Duration)  { t.ticker.Reset(d) }
func (t *Tick) Stop()                  { t.ticker.Stop() }

type TestTick struct {
	c chan time.Time
}

var _ Ticker = (*TestTick)(nil)

func NewTestTick() *TestTick               { return &TestTick{c: make(chan time.Time)} }
func (t *TestTick) Chan() <-chan time.Time { return t.c }
func (t *TestTick) Reset(d time.Duration)  {}
func (t *TestTick) Stop()                  { close(t.c) }
func (t *TestTick) Tick()                  { t.c <- time.Now() }
