package transfer

import (
	"sync/atomic"
	"time"
)

type Meter struct {
	started         time.Time
	Uploaded        atomic.Int64
	Downloaded      atomic.Int64
	ConfirmedUpload atomic.Int64
	Attempts        atomic.Int64
	Failures        atomic.Int64
}

type Snapshot struct {
	Uploaded        int64         `json:"uploaded_bytes"`
	Downloaded      int64         `json:"downloaded_bytes"`
	ConfirmedUpload int64         `json:"confirmed_upload_bytes"`
	Attempts        int64         `json:"attempts"`
	Failures        int64         `json:"failures"`
	Elapsed         time.Duration `json:"elapsed_ns"`
}

func (m *Meter) Snapshot() Snapshot {
	return Snapshot{
		Uploaded: m.Uploaded.Load(), Downloaded: m.Downloaded.Load(),
		ConfirmedUpload: m.ConfirmedUpload.Load(), Attempts: m.Attempts.Load(),
		Failures: m.Failures.Load(), Elapsed: time.Since(m.started),
	}
}
