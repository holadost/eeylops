package logging

import (
	"eeylops/util/logging/glog"
	"fmt"
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
	newLogger := PrefixLogger{
		prefix: parentLogger.GetPrefix() + " " + createPrefixStr(prefix),
	}
	return &newLogger
}

func (logger *PrefixLogger) GetPrefix() string {
	return logger.prefix
}

func (logger *PrefixLogger) Infof(format string, args ...interface{}) {
	newFormat := fmt.Sprintf("%s %s", logger.prefix, format)
	glog.Infof(newFormat, args...)
}

func (logger *PrefixLogger) Errorf(format string, args ...interface{}) {
	newFormat := fmt.Sprintf("%s %s", logger.prefix, format)
	glog.Errorf(newFormat, args...)
}

func (logger *PrefixLogger) Warningf(format string, args ...interface{}) {
	newFormat := fmt.Sprintf("%s %s", logger.prefix, format)
	glog.Warningf(newFormat, args...)
}

func (logger *PrefixLogger) Fatalf(format string, args ...interface{}) {
	newFormat := fmt.Sprintf("%s %s", logger.prefix, format)
	glog.Fatalf(newFormat, args...)
}

func (logger *PrefixLogger) VInfof(v uint, format string, args ...interface{}) {
	newFormat := fmt.Sprintf("%s %s", logger.prefix, format)
	glog.V(glog.Level(v)).Infof(newFormat, args...)
}

func createPrefixStr(prefix string) string {
	return "[" + prefix + "]"
}
