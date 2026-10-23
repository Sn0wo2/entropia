package utils

import (
	"context"

	"golang.org/x/time/rate"
)

func WaitBytes(ctx context.Context, bucket *rate.Limiter, bytes int) error {
	err := bucket.WaitN(ctx, bytes)
	if err != nil && bytes >= 0 && bytes <= bucket.Burst() && ctx.Err() == nil {
		if _, ok := ctx.Deadline(); ok {
			<-ctx.Done()

			return ctx.Err()
		}
	}

	return err
}
