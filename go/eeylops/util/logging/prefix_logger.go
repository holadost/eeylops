package logging

import (
	"context"
	"fmt"
	"github.com/golang/glog"
	"sync"
)

// PrefixLogger is a wrapper around glog that uses the given prefixes in every log statement. Useful for debugging.
type PrefixLogger struct {
	prefixes     []string // List of prefixes.
	logPrefixStr string   // The logPrefixStr string that is attached to every log statement.
	depth        int      // Log depth.
}

// NewPrefixLogger returns a new instance of PrefixLogger which uses the given prefix in every log statement.
func NewPrefixLogger(prefix string) *PrefixLogger {
	return newPrefixLogger([]string{prefix}, 0)
}

// NewMultiPrefixLogger returns a new instance of PrefixLogger which uses the given prefixes in every log statement.
func NewMultiPrefixLogger(prefixes []string) *PrefixLogger {
	return newPrefixLogger(prefixes, 0)
}

// NewPrefixLoggerWithParent returns a new instance of PrefixLogger that uses both the parent prefixes and the given
// prefix.
func NewPrefixLoggerWithParent(prefix string, parentLogger *PrefixLogger) *PrefixLogger {
	if parentLogger != nil {
		return newPrefixLogger(append(parentLogger.GetPrefixes(), prefix), 0)
	}
	return newPrefixLogger([]string{prefix}, 0)
}

// NewPrefixLoggerWithParentAndDepth returns a new instance of the PrefixLogger that uses both the parent prefixes
// and the given prefix. Additionally, the user can also specify the log depth.
func NewPrefixLoggerWithParentAndDepth(prefix string, parentLogger *PrefixLogger, depth int) *PrefixLogger {
	if parentLogger != nil {
		return newPrefixLogger(append(parentLogger.GetPrefixes(), prefix), depth)
	}
	return newPrefixLogger([]string{prefix}, depth)
}

// NewPrefixLoggerWithDepth returns a new instance of PrefixLogger that uses the given prefix in every log statement.
// Additionally, the user can also specify the log depth.
func NewPrefixLoggerWithDepth(prefix string, depth int) *PrefixLogger {
	return newPrefixLogger([]string{prefix}, depth)
}

// NewMultiPrefixLoggerWithDepth returns a new instance of PrefixLogger that uses the given prefixes in every log
// statement. Additionally, the user can also specify the log depth.
func NewMultiPrefixLoggerWithDepth(prefixes []string, depth int) *PrefixLogger {
	return newPrefixLogger(prefixes, depth)
}

func newPrefixLogger(prefixes []string, depth int) *PrefixLogger {
	logger := PrefixLogger{logPrefixStr: createPrefixStr(prefixes), prefixes: prefixes, depth: depth}
	return &logger
}

var defaultLock sync.Once
var defaultLogger *PrefixLogger

func DefaultLogger() *PrefixLogger {
	defaultLock.Do(func() {
		defaultLogger = NewPrefixLogger("")
	})
	return defaultLogger
}

func (logger *PrefixLogger) GetLogPrefix() string {
	return logger.logPrefixStr
}

func (logger *PrefixLogger) GetPrefixes() []string {
	var prefixes []string
	prefixes = append(prefixes, logger.prefixes...)
	return prefixes
}

func (logger *PrefixLogger) Infof(format string, args ...interface{}) {
	logStr := fmt.Sprintf(format, args...)
	glog.InfoDepth(1+logger.depth, fmt.Sprintf("%s %s", logger.logPrefixStr, logStr))
}

func (logger *PrefixLogger) Errorf(format string, args ...interface{}) {
	logStr := fmt.Sprintf(format, args...)
	glog.ErrorDepth(1+logger.depth, fmt.Sprintf("%s %s", logger.logPrefixStr, logStr))
}

func (logger *PrefixLogger) Warningf(format string, args ...interface{}) {
	logStr := fmt.Sprintf(format, args...)
	glog.WarningDepth(1+logger.depth, fmt.Sprintf("%s %s", logger.logPrefixStr, logStr))
}

func (logger *PrefixLogger) Fatalf(format string, args ...interface{}) {
	logStr := fmt.Sprintf(format, args...)
	glog.FatalDepth(1+logger.depth, fmt.Sprintf("%s %s", logger.logPrefixStr, logStr))
}

func (logger *PrefixLogger) InfofWithCtx(ctx context.Context, format string, args ...interface{}) {
	logStr := fmt.Sprintf(format, args...)
	glog.InfoDepth(1+logger.depth, fmt.Sprintf("%s %s", logger.logPrefixStr, logStr))
}

func (logger *PrefixLogger) ErrorfWithCtx(ctx context.Context, format string, args ...interface{}) {
	logStr := fmt.Sprintf(format, args...)
	glog.ErrorDepth(1+logger.depth, fmt.Sprintf("%s %s", logger.logPrefixStr, logStr))
}

func (logger *PrefixLogger) WarningfWithCtx(ctx context.Context, format string, args ...interface{}) {
	logStr := fmt.Sprintf(format, args...)
	glog.WarningDepth(1+logger.depth, fmt.Sprintf("%s %s", logger.logPrefixStr, logStr))
}

func (logger *PrefixLogger) FatalfWithCtx(ctx context.Context, format string, args ...interface{}) {
	logStr := fmt.Sprintf(format, args...)
	glog.FatalDepth(1+logger.depth, fmt.Sprintf("%s %s", logger.logPrefixStr, logStr))
}

func (logger *PrefixLogger) VInfof(v uint, format string, args ...interface{}) {
	if glog.V(glog.Level(v)) {
		logStr := fmt.Sprintf(format, args...)
		glog.InfoDepth(1+logger.depth, fmt.Sprintf("%s %s", logger.logPrefixStr, logStr))
	}
}

func (logger *PrefixLogger) VInfofWithCtx(ctx context.Context, v uint, format string, args ...interface{}) {
	if glog.V(glog.Level(v)) {
		logStr := fmt.Sprintf(format, args...)
		glog.InfoDepth(1+logger.depth, fmt.Sprintf("%s %s", logger.logPrefixStr, logStr))
	}
}

func (logger *PrefixLogger) Debugf(format string, args ...interface{}) {
	if glog.V(glog.Level(1)) {
		logStr := fmt.Sprintf(format, args...)
		glog.InfoDepth(1+logger.depth, fmt.Sprintf("%s %s", logger.logPrefixStr, logStr))
	}
}

func (logger *PrefixLogger) DebugfWithCtx(ctx context.Context, format string, args ...interface{}) {
	if glog.V(glog.Level(1)) {
		logStr := fmt.Sprintf(format, args...)
		glog.InfoDepth(1+logger.depth, fmt.Sprintf("%s %s", logger.logPrefixStr, logStr))
	}
}

func createPrefixStr(prefixes []string) string {
	fullPrefixStr := ""
	for ii, prefix := range prefixes {
		if len(prefix) == 0 {
			continue
		}
		fullPrefixStr += "[" + prefix + "]"
		if ii != len(prefixes)-1 {
			fullPrefixStr += " "
		}
	}
	return fullPrefixStr
}

const kLogContextKey = "log_context"

func GetLogCtx(ctx context.Context) string {
	val := ctx.Value(kLogContextKey)
	actualVal, ok := val.(string)
	if !ok {
		return ""
	}
	return actualVal
}

func WithLogContext(ctx context.Context, logCtxMsg string) context.Context {
	return context.WithValue(ctx, kLogContextKey, logCtxMsg)
}
