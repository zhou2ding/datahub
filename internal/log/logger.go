package log

import (
	"github.com/go-kratos/kratos/v2/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
)

type Config struct {
	Level      string `json:"level"`      // 日志级别
	Filename   string `json:"filename"`   // 日志文件路径
	MaxSize    int    `json:"maxSize"`    // 日志文件最大大小(MB)
	MaxBackups int    `json:"maxBackups"` // 最多保留多少个日志文件
	MaxAge     int    `json:"maxAge"`     // 日志文件最多保存多少天
	Compress   bool   `json:"compress"`   // 是否压缩
	Stdout     bool   `json:"stdout"`     // 是否同时输出到控制台
}

func NewLogger(conf *Config) log.Logger {
	// 设置日志级别
	level := getZapLevel(conf.Level)

	// 初始化 lumberjack
	hook := lumberjack.Logger{
		Filename:   conf.Filename,   // 日志文件路径
		MaxSize:    conf.MaxSize,    // 每个日志文件保存的最大尺寸 单位：M
		MaxBackups: conf.MaxBackups, // 日志文件最多保存多少个备份
		MaxAge:     conf.MaxAge,     // 文件最多保存多少天
		Compress:   conf.Compress,   // 是否压缩
	}

	// 设置日志输出
	var ws zapcore.WriteSyncer
	if conf.Stdout {
		ws = zapcore.NewMultiWriteSyncer(zapcore.AddSync(os.Stdout), zapcore.AddSync(&hook))
	} else {
		ws = zapcore.AddSync(&hook)
	}

	// 设置日志编码
	encoder := zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig())

	return NewZapLogger(encoder, ws, level)
}

func getZapLevel(level string) zapcore.Level {
	switch level {
	case "debug":
		return zapcore.DebugLevel
	case "info":
		return zapcore.InfoLevel
	case "warn":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	case "fatal":
		return zapcore.FatalLevel
	default:
		return zapcore.InfoLevel
	}
}
