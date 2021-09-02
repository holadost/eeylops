package util

import (
	"context"
	"errors"
	"github.com/golang/glog"
	"time"
)

// RetryFunc are functions that must be retried.
type RetryFunc func(attempt int) (retry bool, err error)

// BackoffFunc must backoff for a certain time interval before returning.
type BackoffFunc func(attempt int)

var (
	ErrExhaustedAllRetryAttempts = errors.New("exhausted all attempts")
	ErrRetryContextExpired       = errors.New("retry context/timeout expired")
)

func Retry(fn RetryFunc, bfn BackoffFunc) error {
	attempt := 0
	for {
		attempt++
		retry, err := fn(attempt)
		if retry {
			bfn(attempt)
			continue
		}
		return err
	}
}

func RetryWithTimeout(fn RetryFunc, bfn BackoffFunc, timeout time.Duration) error {
	if timeout < 0 {
		glog.Fatalf("timeout must be >= 0")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return RetryWithContext(ctx, fn, bfn)
}

func RetryWithContext(ctx context.Context, fn RetryFunc, bfn BackoffFunc) error {
	attempt := 0
	for {
		attempt++
		retry, err := fn(attempt)
		if retry {
			select {
			case <-ctx.Done():
				return ErrRetryContextExpired
			default:
				bfn(attempt)
			}
		}
		return err
	}
}
