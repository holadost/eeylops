package logging

import (
	"context"
	"fmt"
	"github.com/golang/glog"
	"sync"
)

type PrefixLogger struct {
	prefixes     []string
	logPrefixStr string // The logPrefixStr string that is attached to every log statement.
	depth        int
}

// NewPrefixLogger returns a new instance of the logPrefixStr logger.
func NewPrefixLogger(prefix string) *PrefixLogger {
	return newPrefixLogger([]string{prefix}, 0)
}

// NewMultiPrefixLogger returns a new instance of the logPrefixStr logger.
func NewMultiPrefixLogger(prefixes []string) *PrefixLogger {
	return newPrefixLogger(prefixes, 0)
}

// NewPrefixLoggerWithParent returns a new instance of the logPrefixStr logger. It uses the logPrefixStr of the parent as well
// as the given logPrefixStr in every log statement.
func NewPrefixLoggerWithParent(prefix string, parentLogger *PrefixLogger) *PrefixLogger {
	if parentLogger != nil {
		return newPrefixLogger(append(parentLogger.GetPrefixes(), prefix), 0)
	}
	return newPrefixLogger([]string{prefix}, 0)
}

func NewPrefixLoggerWithParentAndDepth(prefix string, parentLogger *PrefixLogger, depth int) *PrefixLogger {
	if parentLogger != nil {
		return newPrefixLogger(append(parentLogger.GetPrefixes(), prefix), depth)
	}
	return newPrefixLogger([]string{prefix}, depth)
}

// NewPrefixLoggerWithDepth returns a new instance of the logPrefixStr logger.
func NewPrefixLoggerWithDepth(prefix string, depth int) *PrefixLogger {
	return newPrefixLogger([]string{prefix}, depth)
}

// NewMultiPrefixLoggerWithDepth returns a new instance of the logPrefixStr logger.
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
