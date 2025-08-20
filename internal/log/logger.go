package log

import (
	"os"

	"github.com/go-kratos/kratos/v2/log"
	"go.elastic.co/ecszap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
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
	logger := initLogger(conf, 2)
	return newZapLogger(logger)
}

func initLogger(conf *Config, skip int) *zap.Logger {
	// 1. 设置日志输出
	var ws zapcore.WriteSyncer
	if conf.Stdout {
		ws = zapcore.AddSync(os.Stdout)
	} else {
		// 初始化 lumberjack
		hook := lumberjack.Logger{
			Filename:   conf.Filename,
			MaxSize:    conf.MaxSize,
			MaxBackups: conf.MaxBackups,
			MaxAge:     conf.MaxAge,
			Compress:   conf.Compress,
		}
		ws = zapcore.NewMultiWriteSyncer(zapcore.AddSync(os.Stdout), zapcore.AddSync(&hook))
	}

	// 2. 创建 ecszap 的 EncoderConfig
	encoderConfig := ecszap.NewDefaultEncoderConfig()

	// 3. 创建 ecszap Core
	core := ecszap.NewCore(
		encoderConfig,
		ws,
		getZapLevel(conf.Level),
	)

	// 4. 创建 zap logger
	logger := zap.New(
		core,
		zap.AddCaller(),
		zap.AddCallerSkip(skip),
		zap.AddStacktrace(zapcore.DPanicLevel), // DPanicLevel 及以上级别日志会自动带上堆栈信息
	)

	return logger
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
