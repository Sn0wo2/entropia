package utils

import (
	"context"

	"golang.org/x/time/rate"
)

type Rate struct{}

func (Rate) LimitSize(bucket *rate.Limiter, maximum int) int {
	if bucket == nil {
		return maximum
	}
	return min(maximum, bucket.Burst())
}

func (Rate) WaitBytes(ctx context.Context, bucket *rate.Limiter, bytes int) error {
	if bucket == nil || bytes == 0 {
		return ctx.Err()
	}
	err := bucket.WaitN(ctx, bytes)
	if err != nil && bytes >= 0 && bytes <= bucket.Burst() && ctx.Err() == nil {
		if _, ok := ctx.Deadline(); ok {
			<-ctx.Done()
			return ctx.Err()
		}
	}
	return err
}
