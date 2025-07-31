package log

import (
	"github.com/go-kratos/kratos/v2/log"
	"go.uber.org/zap"
)

var _ log.Logger = (*zapLogger)(nil)

type zapLogger struct {
	*zap.Logger
	fields []zap.Field
	Sync   func() error
}

func newZapLogger(logger *zap.Logger) *zapLogger {
	return &zapLogger{
		Logger: logger,
		Sync:   logger.Sync,
	}
}

func (z *zapLogger) Log(level log.Level, keyvals ...interface{}) error {
	if len(keyvals) == 0 || len(keyvals)%2 != 0 {
		z.Warn("Keyvalues must appear in pairs")
		return nil
	}

	var data []zap.Field
	for i := 0; i < len(keyvals); i += 2 {
		data = append(data, zap.Any(keyvals[i].(string), keyvals[i+1]))
	}

	switch level {
	case log.LevelDebug:
		z.Debug("", data...)
	case log.LevelInfo:
		z.Info("", data...)
	case log.LevelWarn:
		z.Warn("", data...)
	case log.LevelError:
		z.Error("", data...)
	case log.LevelFatal:
		z.Fatal("", data...)
	default:
		z.Info("", data...)
	}
	return nil
}
func (z *zapLogger) Close() error {
	return z.Sync()
}
