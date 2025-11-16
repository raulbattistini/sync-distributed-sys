package util

import (
	"log"
	"os"
	"strings"
)

type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	FATAL
	ERROR
)

var stringToLevel = map[string]LogLevel{
	"DEBUG": DEBUG,
	"INFO":  INFO,
	"WARN":  WARN,
	"FATAL": FATAL,
	"ERROR": ERROR,
}

var levelToString = map[LogLevel]string{
	DEBUG: "DEBUG",
	INFO:  "INFO",
	WARN:  "WARN",
	FATAL: "FATAL",
	ERROR: "ERROR",
}

func ParseLevel(levelStr string) LogLevel {
	if level, ok := stringToLevel[strings.ToUpper(levelStr)]; ok {
		return level
	}
	return INFO
}

func (l LogLevel) String() string {
	if s, ok := levelToString[l]; ok {
		return s
	}
	return "UNKNOWN"
}

type Logger struct {
	standardLogger *log.Logger
	level          LogLevel
}

func NewLogger(minLevel LogLevel) *Logger {
	stdLog := log.New(os.Stdout, "[RPC-SERVER] ", log.LstdFlags)
	return &Logger{
		standardLogger: stdLog,
		level:          minLevel,
	}
}

func (l *Logger) print(messageLevel LogLevel, format string, v ...interface{}) {
	if l.level <= messageLevel {
		prefixFormat := "[" + messageLevel.String() + "] " + format
		l.standardLogger.Printf(prefixFormat, v...)
	}
}

func (l *Logger) Info(format string, v ...interface{}) {
	l.print(INFO, format, v...)
}

func (l *Logger) Fatal(format string, v ...interface{}) {
	l.print(FATAL, format, v...)
	os.Exit(1)
}

func (l *Logger) Error(format string, v ...interface{}) {
	l.print(ERROR, format, v...)
}

func (l *Logger) Warn(format string, v ...interface{}) {
	l.print(WARN, format, v...)
}

func (l *Logger) Debug(format string, v ...interface{}) {
	l.print(DEBUG, format, v...)
}

var DefaultLogger *Logger

func init() {
	DefaultLogger = NewLogger(INFO)
}

func Log() *Logger {
	return DefaultLogger
}
