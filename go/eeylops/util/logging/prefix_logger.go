package logging

import (
	"fmt"
	"github.com/golang/glog"
	"sync"
)

type PrefixLogger struct {
	prefix string // The prefix string that is attached to every log statement.
	depth  int
}

// NewPrefixLogger returns a new instance of the prefix logger.
func NewPrefixLogger(prefix string) *PrefixLogger {
	logger := PrefixLogger{prefix: createPrefixStr(prefix)}
	return &logger
}

// NewMultiPrefixLogger returns a new instance of the prefix logger.
func NewMultiPrefixLogger(prefixes []string) *PrefixLogger {
	fullPrefix := ""
	for _, prefix := range prefixes {
		fullPrefix += createPrefixStr(prefix)
	}
	logger := PrefixLogger{prefix: fullPrefix}
	return &logger
}

// NewPrefixLoggerWithParent returns a new instance of the prefix logger. It uses the prefix of the parent as well
// as the given prefix in every log statement.
func NewPrefixLoggerWithParent(prefix string, parentLogger *PrefixLogger) *PrefixLogger {
	actualPrefix := prefix
	if parentLogger != nil {
		actualPrefix = parentLogger.GetPrefix() + " " + createPrefixStr(prefix)
	}
	logger := PrefixLogger{prefix: actualPrefix}
	return &logger
}

// NewPrefixLoggerWithDepth returns a new instance of the prefix logger.
func NewPrefixLoggerWithDepth(prefix string, depth int) *PrefixLogger {
	logger := PrefixLogger{prefix: createPrefixStr(prefix), depth: depth}
	return &logger
}

// NewMultiPrefixLoggerWithDepth returns a new instance of the prefix logger.
func NewMultiPrefixLoggerWithDepth(prefixes []string, depth int) *PrefixLogger {
	fullPrefix := ""
	for _, prefix := range prefixes {
		fullPrefix += createPrefixStr(prefix) + " "
	}
	logger := PrefixLogger{prefix: fullPrefix, depth: depth}
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

func (logger *PrefixLogger) GetPrefix() string {
	return logger.prefix
}

func (logger *PrefixLogger) Infof(format string, args ...interface{}) {
	logStr := fmt.Sprintf(format, args...)
	glog.InfoDepth(1+logger.depth, fmt.Sprintf("%s %s", logger.prefix, logStr))
}

func (logger *PrefixLogger) Errorf(format string, args ...interface{}) {
	logStr := fmt.Sprintf(format, args...)
	glog.ErrorDepth(1+logger.depth, fmt.Sprintf("%s %s", logger.prefix, logStr))
}

func (logger *PrefixLogger) Warningf(format string, args ...interface{}) {
	logStr := fmt.Sprintf(format, args...)
	glog.WarningDepth(1+logger.depth, fmt.Sprintf("%s %s", logger.prefix, logStr))
}

func (logger *PrefixLogger) Fatalf(format string, args ...interface{}) {
	logStr := fmt.Sprintf(format, args...)
	glog.FatalDepth(1+logger.depth, fmt.Sprintf("%s %s", logger.prefix, logStr))
}

func (logger *PrefixLogger) VInfof(v uint, format string, args ...interface{}) {
	if glog.V(glog.Level(v)) {
		logStr := fmt.Sprintf(format, args...)
		glog.InfoDepth(1+logger.depth, fmt.Sprintf("%s %s", logger.prefix, logStr))
	}
}

func (logger *PrefixLogger) Debugf(format string, args ...interface{}) {
	if glog.V(glog.Level(1)) {
		logStr := fmt.Sprintf(format, args...)
		glog.InfoDepth(1+logger.depth, fmt.Sprintf("%s %s", logger.prefix, logStr))
	}
}

func createPrefixStr(prefix string) string {
	if len(prefix) == 0 {
		return ""
	}
	return "{" + prefix + "}"
}
