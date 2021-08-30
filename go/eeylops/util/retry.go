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

func DoRetry(fn RetryFunc, bfn BackoffFunc) error {
	var err error
	attempt := 0
	for {
		attempt++
		retry, err := fn(attempt)
		if retry {
			bfn(attempt)
			continue
		}
		if err == nil {
			break
		}
	}
	return err
}

func DoRetryWithMultiAttempts(fn RetryFunc, bfn BackoffFunc, numAttempts int) error {
	var err error
	var retry bool
	attempt := 0
	for {
		attempt++
		retry, err = fn(attempt)
		if retry {
			bfn(attempt)
			continue
		}
		if err == nil {
			return nil
		}
		if attempt == numAttempts {
			return ErrExhaustedAllRetryAttempts
		}
	}
}

func DoRetryWithTimeout(fn RetryFunc, bfn BackoffFunc, timeout time.Duration) error {
	if timeout < 0 {
		glog.Fatalf("timeout must be >= 0")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return DoRetryWithContext(ctx, fn, bfn)
}

func DoRetryWithContext(ctx context.Context, fn RetryFunc, bfn BackoffFunc) error {
	var err error
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
		if err == nil {
			break
		}
	}
	return err
}
