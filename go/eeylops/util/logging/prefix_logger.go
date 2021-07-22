package logging

import (
	"fmt"
	"github.com/golang/glog"
)

type PrefixLogger struct {
	prefix string // The prefix string that is attached to every log statement.
}

// NewPrefixLogger returns a new instance of the prefix logger.
func NewPrefixLogger(prefix string) *PrefixLogger {
	logger := PrefixLogger{prefix: createPrefixStr(prefix)}
	return &logger
}

// NewPrefixLoggerWithParent returns a new instance of the prefix logger. It uses the prefix of the parent as well
// as the given prefix in every log statement. Makes traceability from logs a whole lot easier.
func NewPrefixLoggerWithParent(prefix string, parentLogger *PrefixLogger) *PrefixLogger {
	actualPrefix := prefix
	if parentLogger != nil {
		actualPrefix = parentLogger.GetPrefix() + " " + createPrefixStr(prefix)
	}
	logger := PrefixLogger{prefix: actualPrefix}
	return &logger
}

func (logger *PrefixLogger) GetPrefix() string {
	return logger.prefix
}

func (logger *PrefixLogger) Infof(format string, args ...interface{}) {
	logStr := fmt.Sprintf(format, args...)
	glog.InfoDepth(1, fmt.Sprintf("%s %s", logger.prefix, logStr))
}

func (logger *PrefixLogger) Errorf(format string, args ...interface{}) {
	logStr := fmt.Sprintf(format, args...)
	glog.ErrorDepth(1, fmt.Sprintf("%s %s", logger.prefix, logStr))
}

func (logger *PrefixLogger) Warningf(format string, args ...interface{}) {
	logStr := fmt.Sprintf(format, args...)
	glog.WarningDepth(1, fmt.Sprintf("%s %s", logger.prefix, logStr))
}

func (logger *PrefixLogger) Fatalf(format string, args ...interface{}) {
	logStr := fmt.Sprintf(format, args...)
	glog.FatalDepth(1, fmt.Sprintf("%s %s", logger.prefix, logStr))
}

func (logger *PrefixLogger) VInfof(v uint, format string, args ...interface{}) {
	if glog.V(glog.Level(v)) {
		logStr := fmt.Sprintf(format, args...)
		glog.InfoDepth(1, fmt.Sprintf("%s %s", logger.prefix, logStr))
	}
}

func createPrefixStr(prefix string) string {
	return "{" + prefix + "}"
}
