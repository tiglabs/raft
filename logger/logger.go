package logger

import (
	"fmt"
	"log"
	"os"
	"util"
)

// Logger encapsulation the log interface.
type Logger interface {
	IsEnableDebug() bool
	IsEnableInfo() bool
	IsEnableWarn() bool

	Debug(format string, v ...interface{})
	Info(format string, v ...interface{})
	Warn(format string, v ...interface{})
	Error(format string, v ...interface{})
}

var (
	stdLogger  = NewDefaultLogger(log.New(os.Stderr, "raft", log.LstdFlags|log.Lshortfile), 0)
	raftLogger = Logger(stdLogger)
)

func SetLogger(l Logger) {
	raftLogger = l
}

func IsEnableDebug() bool {
	return raftLogger.IsEnableDebug()
}

func IsEnableInfo() bool {
	return raftLogger.IsEnableInfo()
}

func IsEnableWarn() bool {
	return raftLogger.IsEnableWarn()
}

func Debug(format string, v ...interface{}) {
	raftLogger.Debug(format, v)
}

func Info(format string, v ...interface{}) {
	raftLogger.Info(format, v)
}

func Warn(format string, v ...interface{}) {
	raftLogger.Warn(format, v)
}

func Error(format string, v ...interface{}) {
	raftLogger.Error(format, v)
}

// DefaultLogger is a default implementation of the Logger interface.
type DefaultLogger struct {
	*log.Logger
	debugEnable bool
	infoEnable  bool
	warnEnable  bool
}

func NewDefaultLogger(log *log.Logger, level int) *DefaultLogger {
	return &DefaultLogger{
		Logger:      log,
		debugEnable: level <= util.DebugLevel,
		infoEnable:  level <= util.InfoLevel,
		warnEnable:  level <= util.WarnLevel,
	}
}

func (l *DefaultLogger) header(lvl, msg string) string {
	return fmt.Sprintf("%s: %s", lvl, msg)
}

func (l *DefaultLogger) IsEnableDebug() bool {
	return l.debugEnable
}

func (l *DefaultLogger) Debug(format string, v ...interface{}) {
	l.Output(2, l.header("DEBUG", fmt.Sprintf(format, v...)))
}

func (l *DefaultLogger) IsEnableInfo() bool {
	return l.infoEnable
}

func (l *DefaultLogger) Info(format string, v ...interface{}) {
	l.Output(2, l.header("INFO", fmt.Sprintf(format, v...)))
}

func (l *DefaultLogger) IsEnableWarn() bool {
	return l.warnEnable
}

func (l *DefaultLogger) Warn(format string, v ...interface{}) {
	l.Output(2, l.header("WARN", fmt.Sprintf(format, v...)))
}

func (l *DefaultLogger) Error(format string, v ...interface{}) {
	l.Output(2, l.header("ERROR", fmt.Sprintf(format, v...)))
}

type FileLogger struct {
	*util.Log
	debugEnable bool
	infoEnable  bool
	warnEnable  bool
}

func NewFileLogger(log *util.Log, level int) *FileLogger {
	return &FileLogger{
		Log:         log,
		debugEnable: level <= util.DebugLevel,
		infoEnable:  level <= util.InfoLevel,
		warnEnable:  level <= util.WarnLevel,
	}
}

func (fl *FileLogger) IsEnableDebug() bool {
	return fl.debugEnable
}

func (fl *FileLogger) Debug(format string, v ...interface{}) {
	fl.LogDebug(fmt.Sprintf(format, v...))
}

func (fl *FileLogger) IsEnableInfo() bool {
	return fl.infoEnable
}

func (fl *FileLogger) Info(format string, v ...interface{}) {
	fl.LogInfo(fmt.Sprintf(format, v...))
}

func (fl *FileLogger) IsEnableWarn() bool {
	return fl.warnEnable
}

func (fl *FileLogger) Warn(format string, v ...interface{}) {
	fl.LogWarn(fmt.Sprintf(format, v...))
}

func (fl *FileLogger) Error(format string, v ...interface{}) {
	fl.LogError(fmt.Sprintf(format, v...))
}
