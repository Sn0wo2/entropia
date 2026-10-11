package transfer

import (
	"context"
	"errors"
	"time"
)

type Report struct {
	Snapshot
	TargetDownload int64   `json:"target_download_bytes"`
	TargetUpload   int64   `json:"target_upload_bytes"`
	State          string  `json:"state"`
	Error          string  `json:"error,omitempty"`
	AverageRate    float64 `json:"average_bytes_per_second"`
}

func Run[E Engine](ctx context.Context, engine E, plan Plan, progress func(Snapshot)) (Report, error) {
	report := Report{TargetDownload: plan.Download, TargetUpload: plan.Upload}
	if err := plan.Validate(); err != nil {
		report.State, report.Error = "failed", err.Error()
		return report, err
	}
	var cancel context.CancelFunc
	if plan.Duration > 0 {
		ctx, cancel = context.WithTimeout(ctx, plan.Duration)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()
	meter := &Meter{started: time.Now()}
	done := make(chan struct{})
	if progress != nil {
		go func() {
			defer close(done)
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					progress(meter.Snapshot())
				}
			}
		}()
	} else {
		close(done)
	}
	var err error
	for _, direction := range []Direction{Download, Upload} {
		bytes := plan.Download
		if direction == Upload {
			bytes = plan.Upload
		}
		if plan.Loop {
			if direction != plan.Direction {
				continue
			}
		} else if bytes == 0 {
			continue
		}
		err = engine.Transfer(ctx, Job{
			Direction: direction, Bytes: bytes, Loop: plan.Loop,
			Concurrency: plan.Concurrency, Rate: plan.Rate, Timeout: plan.Timeout,
		}, meter)
		if err != nil {
			break
		}
	}
	durationReached := plan.Loop && plan.Duration > 0 && errors.Is(err, context.DeadlineExceeded) && errors.Is(ctx.Err(), context.DeadlineExceeded)
	cancel()
	<-done
	report.Snapshot = meter.Snapshot()
	report.State = "completed"
	if durationReached {
		report.State, err = "duration_reached", nil
	} else if err != nil {
		report.State = "failed"
		if errors.Is(err, context.Canceled) {
			report.State = "cancelled"
		} else if errors.Is(err, context.DeadlineExceeded) {
			report.State = "timed_out"
		}
		report.Error = err.Error()
	}
	if seconds := report.Elapsed.Seconds(); seconds > 0 {
		report.AverageRate = (float64(report.Uploaded) + float64(report.Downloaded)) / seconds
	}
	return report, err
}
