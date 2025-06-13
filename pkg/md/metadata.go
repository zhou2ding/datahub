package md

import (
	"context"
	"github.com/go-kratos/kratos/v2/metadata"
)

func GetMetadata(ctx context.Context, key string) string {
	if md, ok := metadata.FromServerContext(ctx); ok {
		value := md.Get(key)
		return value
	}
	return ""
}
