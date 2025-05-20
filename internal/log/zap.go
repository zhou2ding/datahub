package log

import (
	"github.com/go-kratos/kratos/v2/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var _ log.Logger = (*ZapLogger)(nil)

type ZapLogger struct {
	*zap.Logger
	Sync func() error
}

func NewZapLogger(encoder zapcore.Encoder, ws zapcore.WriteSyncer, level zapcore.Level) *ZapLogger {
	if ws == nil {
		panic("the writer sync can't be nil")
	}

	core := zapcore.NewCore(encoder, ws, level)
	zapLogger := zap.New(
		core,
		zap.AddCaller(),
		zap.AddCallerSkip(2),
		zap.AddStacktrace(zapcore.DPanicLevel),
	)

	return &ZapLogger{Logger: zapLogger, Sync: zapLogger.Sync}
}

func (l *ZapLogger) Log(level log.Level, keyvals ...interface{}) error {
	if len(keyvals) == 0 || len(keyvals)%2 != 0 {
		l.Warn("Keyvalues must appear in pairs")
		return nil
	}

	var data []zap.Field
	for i := 0; i < len(keyvals); i += 2 {
		data = append(data, zap.Any(keyvals[i].(string), keyvals[i+1]))
	}

	switch level {
	case log.LevelDebug:
		l.Debug("", data...)
	case log.LevelInfo:
		l.Info("", data...)
	case log.LevelWarn:
		l.Warn("", data...)
	case log.LevelError:
		l.Error("", data...)
	case log.LevelFatal:
		l.Fatal("", data...)
	}
	return nil
}

func (l *ZapLogger) Close() error {
	return l.Sync()
}
