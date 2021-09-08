package util

import (
	"context"
	"errors"
	"github.com/golang/glog"
	"time"
)

// RetryFunc are functions that must be retried.
type RetryFunc func(attempt int) (retry bool, err error)

// BackoffFunc must return a timer channel that will fire once the backoff time has expired.
type BackoffFunc func(attempt int) <-chan time.Time

var (
	ErrRetryContextExpired = errors.New("retry context/timeout expired")
)

func Retry(fn RetryFunc, bfn BackoffFunc) error {
	attempt := 0
	for {
		attempt++
		retry, err := fn(attempt)
		backoffChan := bfn(attempt)
		if retry {
			select {
			case <-backoffChan:
				continue
			}
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
			backoffChan := bfn(attempt)
			select {
			case <-ctx.Done():
				return ErrRetryContextExpired
			case <-backoffChan:
				continue
			}
		}
		return err
	}
}
