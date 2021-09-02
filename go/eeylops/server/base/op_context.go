package base

import (
	"context"
	"eeylops/util/logging"
	"fmt"
)

const kOpIdKey = "eeylops_op_id"

func WithOpContext(ctx context.Context, opId uint64) context.Context {
	return logging.WithLogContext(context.WithValue(ctx, kOpIdKey, opId), fmt.Sprintf("op_id:%d", opId))
}
