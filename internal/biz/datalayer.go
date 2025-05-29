package biz

import (
	"context"
	"datahub/api/datalayer/v1"
	"github.com/go-kratos/kratos/v2/log"
	"google.golang.org/protobuf/types/known/emptypb"
)

type DatalayerRepo interface {
	Query(ctx context.Context, req *v1.QueryRequest) (*v1.QueryResponse, error)
	Insert(ctx context.Context, req *v1.InsertRequest) (*v1.MutationResponse, error)
	Update(ctx context.Context, req *v1.UpdateRequest) (*v1.MutationResponse, error)
	Delete(ctx context.Context, req *v1.DeleteRequest) (*v1.MutationResponse, error)
	BeginTransaction(ctx context.Context, req *v1.BeginTransactionRequest) (*v1.BeginTransactionResponse, error)
	CommitTransaction(ctx context.Context, req *v1.TransactionRequest) (*emptypb.Empty, error)
	RollbackTransaction(ctx context.Context, req *v1.TransactionRequest) (*emptypb.Empty, error)
	ListTables(ctx context.Context, req *v1.ListTablesRequest) (*v1.ListTablesResponse, error)
	DescribeTable(ctx context.Context, req *v1.DescribeTableRequest) (*v1.DescribeTableResponse, error)
	ExecRawSQL(ctx context.Context, req *v1.ExecRawSQLRequest) (*v1.ExecRawSQLResponse, error)
}

type DatalayerUseCase struct {
	repo DatalayerRepo
	log  *log.Helper
}

func NewDatalayerUseCase(repo DatalayerRepo, logger log.Logger) *DatalayerUseCase {
	return &DatalayerUseCase{repo: repo, log: log.NewHelper(logger)}
}

func (uc *DatalayerUseCase) Query(ctx context.Context, req *v1.QueryRequest) (*v1.QueryResponse, error) {
	return uc.repo.Query(ctx, req)
}

func (uc *DatalayerUseCase) Insert(ctx context.Context, req *v1.InsertRequest) (*v1.MutationResponse, error) {
	return uc.repo.Insert(ctx, req)
}

func (uc *DatalayerUseCase) Update(ctx context.Context, req *v1.UpdateRequest) (*v1.MutationResponse, error) {
	return uc.repo.Update(ctx, req)
}

func (uc *DatalayerUseCase) Delete(ctx context.Context, req *v1.DeleteRequest) (*v1.MutationResponse, error) {
	return uc.repo.Delete(ctx, req)
}

func (uc *DatalayerUseCase) BeginTransaction(ctx context.Context, req *v1.BeginTransactionRequest) (*v1.BeginTransactionResponse, error) {
	return uc.repo.BeginTransaction(ctx, req)
}

func (uc *DatalayerUseCase) CommitTransaction(ctx context.Context, req *v1.TransactionRequest) (*emptypb.Empty, error) {
	return uc.repo.CommitTransaction(ctx, req)
}

func (uc *DatalayerUseCase) RollbackTransaction(ctx context.Context, req *v1.TransactionRequest) (*emptypb.Empty, error) {
	return uc.repo.RollbackTransaction(ctx, req)
}

func (uc *DatalayerUseCase) ListTables(ctx context.Context, req *v1.ListTablesRequest) (*v1.ListTablesResponse, error) {
	return uc.repo.ListTables(ctx, req)
}

func (uc *DatalayerUseCase) DescribeTable(ctx context.Context, req *v1.DescribeTableRequest) (*v1.DescribeTableResponse, error) {
	return uc.repo.DescribeTable(ctx, req)
}

func (uc *DatalayerUseCase) ExecRawSQL(ctx context.Context, req *v1.ExecRawSQLRequest) (*v1.ExecRawSQLResponse, error) {
	return uc.repo.ExecRawSQL(ctx, req)
}
