package util

import (
	"context"
	"errors"
	"github.com/cenkalti/backoff"
	"github.com/golang/glog"
	"time"
)

// RetryFunc are functions that must be retried.
type RetryFunc func(attempt int) (retry bool, err error)

// BackoffFunc must return a timer channel that will fire once the backoff time has expired.
type BackoffFunc func(attempt int) <-chan time.Time

var (
	ErrRetryContextExpired       = errors.New("retry context/timeout expired")
	ErrRetryExhaustedAllAttempts = errors.New("retry exhausted all attempts")
)

// NewExponentialBackoffFunc returns a new backoff function that can used with Retry.
// initialInterval specifies the first backoff duration.
// maxInterval caps the backoff duration.
// multiplier increases the next backoff duration by this factor.
func NewExponentialBackoffFunc(initialInterval time.Duration, maxInterval time.Duration,
	multiplier float64) BackoffFunc {
	b := &backoff.ExponentialBackOff{
		InitialInterval:     initialInterval,
		RandomizationFactor: 0.2,
		Multiplier:          multiplier,
		MaxInterval:         maxInterval,
		MaxElapsedTime:      0, // Never stop the timer.
		Clock:               backoff.SystemClock,
	}
	b.Reset()
	bfn := func(attempt int) <-chan time.Time {
		return time.After(b.NextBackOff())
	}
	return bfn
}

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

func RetryMultiAttempts(numAttempts int, fn RetryFunc, bfn BackoffFunc) error {
	attempt := 0
	for {
		attempt++
		if attempt > numAttempts {
			return ErrRetryExhaustedAllAttempts
		}
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
	doneChan := ctx.Done()
	for {
		attempt++
		retry, err := fn(attempt)
		if retry {
			backoffChan := bfn(attempt)
			select {
			case <-doneChan:
				return ErrRetryContextExpired
			case <-backoffChan:
				continue
			}
		}
		return err
	}
}
