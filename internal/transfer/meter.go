package transfer

import (
	"sync/atomic"
	"time"
)

type Meter struct {
	started    time.Time
	uploaded   atomic.Int64
	downloaded atomic.Int64
	confirmed  atomic.Int64
	attempts   atomic.Int64
	failures   atomic.Int64
}

type Snapshot struct {
	Uploaded        int64         `json:"uploaded_bytes"`
	Downloaded      int64         `json:"downloaded_bytes"`
	ConfirmedUpload int64         `json:"confirmed_upload_bytes"`
	Attempts        int64         `json:"attempts"`
	Failures        int64         `json:"failures"`
	Elapsed         time.Duration `json:"elapsed_ns"`
}

func (m *Meter) Add(direction Direction, n int64) {
	if direction == Upload {
		m.uploaded.Add(n)
	} else {
		m.downloaded.Add(n)
	}
}

func (m *Meter) ConfirmUpload(n int64) { m.confirmed.Add(n) }
func (m *Meter) Attempt()              { m.attempts.Add(1) }
func (m *Meter) Failure()              { m.failures.Add(1) }

func (m *Meter) Snapshot() Snapshot {
	return Snapshot{
		Uploaded: m.uploaded.Load(), Downloaded: m.downloaded.Load(),
		ConfirmedUpload: m.confirmed.Load(), Attempts: m.attempts.Load(),
		Failures: m.failures.Load(), Elapsed: time.Since(m.started),
	}
}
