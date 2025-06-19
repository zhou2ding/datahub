package data

import (
	"context"
	v1 "datahub/api/datalayer/v1"
	"datahub/internal/biz"
	"datahub/pkg/global"
	"datahub/pkg/md"
	"fmt"
	"github.com/go-kratos/kratos/v2/log"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	"gorm.io/gorm/clause"
	"time"
)

type DatalayerRepo struct {
	data *Data
	log  *log.Helper
}

func NewDatalayerRepo(data *Data, logger log.Logger) *DatalayerRepo {
	return &DatalayerRepo{
		data: data,
		log:  log.NewHelper(logger),
	}
}

var _ biz.DatalayerRepo = (*DatalayerRepo)(nil)

func (r *DatalayerRepo) Query(ctx context.Context, req *v1.QueryRequest) (*v1.QueryResponse, error) {
	return &v1.QueryResponse{}, nil
}

func mapToProtoRow(ctx context.Context, record map[string]any) *v1.Row {
	fields := make(map[string]*structpb.Value)
	for key, val := range record {
		var (
			err      error
			protoVal *structpb.Value
		)
		switch v := val.(type) {
		case time.Time:
			protoVal = structpb.NewStringValue(v.Format(time.DateTime))
		default:
			// 对于其他标准类型，使用通用的转换
			protoVal, err = structpb.NewValue(v)
			if err != nil {
				log.Errorf("traceId: %s failed to convert value for key '%s' (Go type: %T, value: %v) to Protobuf Value: %v", md.GetMetadata(ctx, global.RequestIdMd), key, val, val, err)
			}
		}
		fields[key] = protoVal
	}
	return &v1.Row{Fields: fields}
}

// 将 Protobuf Value 转换为适合 GORM 的 Go 类型
func protobufValueToAny(pv *structpb.Value) (any, error) {
	if pv == nil {
		return nil, nil
	}
	switch pv.Kind.(type) {
	case *structpb.Value_NullValue:
		return nil, nil
	case *structpb.Value_NumberValue:
		return pv.GetNumberValue(), nil
	case *structpb.Value_StringValue:
		return pv.GetStringValue(), nil
	case *structpb.Value_BoolValue:
		return pv.GetBoolValue(), nil
	case *structpb.Value_StructValue:
		return nil, fmt.Errorf("struct type conversion not implemented for GORM conditions")
	case *structpb.Value_ListValue:
		list := pv.GetListValue()
		goList := make([]any, len(list.Values))
		for i, v := range list.Values {
			goVal, err := protobufValueToAny(v)
			if err != nil {
				return nil, fmt.Errorf("error converting list element %d: %w", i, err)
			}
			goList[i] = goVal
		}
		return goList, nil
	default:
		return nil, fmt.Errorf("unsupported protobuf value kind: %T", pv.Kind)
	}
}

// 将 proto Operator 枚举转换为 GORM 字符串操作符和占位符信息
func getGormOperator(op v1.Operator) (gormOp string, placeholder string, requiresValue bool) {
	switch op {
	case v1.Operator_EQ:
		return "=", "?", true
	case v1.Operator_NEQ:
		return "!=", "?", true
	case v1.Operator_GT:
		return ">", "?", true
	case v1.Operator_GTE:
		return ">=", "?", true
	case v1.Operator_LT:
		return "<", "?", true
	case v1.Operator_LTE:
		return "<=", "?", true
	case v1.Operator_IN:
		return "IN", "", true
	case v1.Operator_NOT_IN:
		return "NOT IN", "", true
	case v1.Operator_LIKE:
		return "LIKE", "?", true
	case v1.Operator_NOT_LIKE:
		return "NOT LIKE", "?", true
	case v1.Operator_IS_NULL:
		return "IS NULL", "", false
	case v1.Operator_IS_NOT_NULL:
		return "IS NOT NULL", "", false
	case v1.Operator_EXISTS:
		return "EXISTS", "", true
	case v1.Operator_NOT_EXISTS:
		return "NOT EXISTS", "", true
	default:
		return "", "", false
	}
}

// 构建聚合函数的 SQL 字符串
func buildAggregationClause(agg *v1.Aggregation) (string, error) {
	if agg.Alias == "" {
		return "", fmt.Errorf("aggregation alias is required")
	}
	field := agg.Field
	if field == "" && agg.Function != v1.Aggregation_COUNT {
		return "", fmt.Errorf("field is required for aggregation function %s", agg.Function)
	}
	if field == "" && agg.Function == v1.Aggregation_COUNT {
		field = "*" // 默认 COUNT 字段
	}

	safeAlias := clause.Column{Name: agg.Alias}.Name
	safeField := field
	if field != "*" {
		safeField = clause.Column{Name: field}.Name
	}

	funcName := ""
	switch agg.Function {
	case v1.Aggregation_COUNT:
		funcName = "COUNT"
	case v1.Aggregation_SUM:
		funcName = "SUM"
	case v1.Aggregation_AVG:
		funcName = "AVG"
	case v1.Aggregation_MIN:
		funcName = "MIN"
	case v1.Aggregation_MAX:
		funcName = "MAX"
	default:
		return "", fmt.Errorf("unsupported aggregation function: %s", agg.Function)
	}

	// 格式: FUNCTION(field) AS alias
	return fmt.Sprintf("%s(%s) AS %s", funcName, safeField, safeAlias), nil
}

func (r *DatalayerRepo) Insert(ctx context.Context, req *v1.InsertRequest) (*v1.MutationResponse, error) {
	return &v1.MutationResponse{}, nil
}

func (r *DatalayerRepo) Update(ctx context.Context, req *v1.UpdateRequest) (*v1.MutationResponse, error) {
	return &v1.MutationResponse{}, nil
}

func (r *DatalayerRepo) Delete(ctx context.Context, req *v1.DeleteRequest) (*v1.MutationResponse, error) {
	return &v1.MutationResponse{}, nil
}

func (r *DatalayerRepo) BeginTransaction(ctx context.Context, req *v1.BeginTransactionRequest) (*v1.BeginTransactionResponse, error) {
	return &v1.BeginTransactionResponse{}, nil
}

func (r *DatalayerRepo) CommitTransaction(ctx context.Context, req *v1.TransactionRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (r *DatalayerRepo) RollbackTransaction(ctx context.Context, req *v1.TransactionRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (r *DatalayerRepo) ListTables(ctx context.Context, req *v1.ListTablesRequest) (*v1.ListTablesResponse, error) {
	return &v1.ListTablesResponse{}, nil
}

func (r *DatalayerRepo) DescribeTable(ctx context.Context, req *v1.DescribeTableRequest) (*v1.DescribeTableResponse, error) {
	return &v1.DescribeTableResponse{}, nil
}

func (r *DatalayerRepo) ExecRawSQL(ctx context.Context, req *v1.ExecRawSQLRequest) (*v1.ExecRawSQLResponse, error) {
	return &v1.ExecRawSQLResponse{}, nil
}
