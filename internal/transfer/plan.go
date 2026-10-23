package transfer

import (
	"context"
	"errors"
	"time"
)

type Direction string

const (
	Download Direction = "download"
	Upload   Direction = "upload"
)

type Plan struct {
	Download    int64
	Upload      int64
	Loop        bool
	Direction   Direction
	Concurrency int
	Rate        int64
	Duration    time.Duration
	Timeout     time.Duration
}

type Job struct {
	Direction   Direction
	Bytes       int64
	Loop        bool
	Concurrency int
	Rate        int64
	Timeout     time.Duration
}

type Engine interface {
	Init() error
	Transfer(ctx context.Context, job Job, meter *Meter) error
}

func (p Plan) Validate() error {
	switch {
	case p.Download < 0 || p.Upload < 0:
		return errors.New("traffic amounts must not be negative")
	case p.Concurrency < 1 || p.Concurrency > 1024:
		return errors.New("concurrency must be between 1 and 1024")
	case p.Rate < 0 || p.Duration < 0 || p.Timeout <= 0:
		return errors.New("rate and duration must not be negative; timeout must be positive")
	case p.Loop && (p.Download != 0 || p.Upload != 0):
		return errors.New("--loop cannot be combined with traffic amounts")
	case p.Loop && p.Direction != Download && p.Direction != Upload:
		return errors.New("loop direction must be download or upload")
	case !p.Loop && p.Download == 0 && p.Upload == 0:
		return errors.New("specify --download, --upload, or --loop")
	}

	return nil
}
