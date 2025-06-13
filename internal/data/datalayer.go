package data

import (
	"context"
	v1 "datahub/api/datalayer/v1"
	"datahub/internal/biz"
	"datahub/pkg/global"
	"datahub/pkg/md"
	"github.com/go-kratos/kratos/v2/log"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
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
