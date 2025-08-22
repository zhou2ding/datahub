package data

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strings"
	"syscall"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	mysqlDriver "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
)

type RetryOptions struct {
	Timeout        time.Duration
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Logger         *log.Helper
}

var DefaultRetryOptions = RetryOptions{
	Timeout:        15 * time.Second,
	InitialBackoff: 200 * time.Millisecond,
	MaxBackoff:     2 * time.Second,
}

func withRetry(ctx context.Context, db *gorm.DB, op func(tx *gorm.DB) error, opt *RetryOptions) error {
	if opt == nil {
		tmp := DefaultRetryOptions
		opt = &tmp
	}
	if opt.Timeout <= 0 {
		opt.Timeout = DefaultRetryOptions.Timeout
	}
	if opt.InitialBackoff <= 0 {
		opt.InitialBackoff = DefaultRetryOptions.InitialBackoff
	}
	if opt.MaxBackoff <= 0 {
		opt.MaxBackoff = DefaultRetryOptions.MaxBackoff
	}

	deadline := time.Now().Add(opt.Timeout) // 计算超时截止
	attempt := 0                            // 第几次尝试
	var lastErr error

	for {
		attempt++
		tx := db
		if ctx != nil {
			tx = tx.WithContext(ctx)
		}

		err := op(tx)
		if err == nil {
			return nil
		}
		lastErr = err

		// 非连接类错误：直接返回
		if !IsConnError(err) {
			return err
		}

		now := time.Now()
		if now.After(deadline) {
			return fmt.Errorf("db operation failed after %d attempts within %s: %w", attempt, opt.Timeout, lastErr)
		}

		// 计算退避（指数退避 + 全抖动）
		exp := min(attempt-1, 10)
		backoff := opt.InitialBackoff * time.Duration(1<<uint(exp))
		if backoff > opt.MaxBackoff {
			backoff = opt.MaxBackoff
		}
		// 全抖动：在 [0, backoff] 内随机等待
		sleep := time.Duration(rand.Int63n(int64(backoff) + 1))

		if opt.Logger != nil {
			opt.Logger.Warnf("DB connection error (attempt %d), retrying in %v: %v", attempt, sleep, err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}
	}
}

func IsConnError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, io.EOF) || errors.Is(err, driver.ErrBadConn) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var nerr net.Error
	if errors.As(err, &nerr) { // 超时/临时错误可重试
		if nerr.Timeout() || nerr.Temporary() {
			return true
		}
	}

	var operr *net.OpError
	if errors.As(err, &operr) {
		switch strings.ToLower(operr.Op) { // 常见网络操作
		case "dial", "read", "write":
			return true
		}
	}

	// 常见 syscall 错误：broken pipe、connection reset 等
	if errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}

	// MySQL 断链/网络相关错误码
	var my *mysqlDriver.MySQLError
	if errors.As(err, &my) {
		switch my.Number {
		case 2006: // server has gone away
			return true
		case 2013: // lost connection during query
			return true
		case 2055: // lost connection at '%s'
			return true
		// 额外网络相关错误码（增强鲁棒性）
		case 1042: // can't get hostname for address
			return true
		case 1047: // unknown command
			return true
		}
	}

	// 兜底：字符串匹配（覆盖 broken pipe / connection reset 等）
	msg := strings.ToLower(err.Error())
	bits := []string{
		"broken pipe",
		"connection reset by peer",
		"read: connection reset",
		"write: connection reset",
		"write: broken pipe",
		"no such host",
		"i/o timeout",
		"server has gone away",
		"lost connection to mysql server",
		"bad connection",
	}
	for _, s := range bits {
		if strings.Contains(msg, s) {
			return true
		}
	}

	return false
}
