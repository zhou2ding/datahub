package data

import (
	"context"
	v1 "datahub/api/datalayer/v1"
	"datahub/internal/biz"
	"github.com/go-kratos/kratos/v2/log"
	"google.golang.org/protobuf/types/known/emptypb"
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
