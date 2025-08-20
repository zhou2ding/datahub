package service

import (
	"context"
	"datahub/api/datalayer/v1"
	"datahub/internal/biz"

	"google.golang.org/protobuf/types/known/emptypb"
)

type DatalayerService struct {
	v1.UnimplementedDataCRUDServer
	v1.UnimplementedMetadataServer
	v1.UnimplementedRawSqlServer
	uc *biz.DatalayerUseCase
}

func NewDatalayerService(uc *biz.DatalayerUseCase) *DatalayerService {
	return &DatalayerService{uc: uc}
}

func (s *DatalayerService) Query(ctx context.Context, req *v1.QueryRequest) (*v1.QueryResponse, error) {
	return s.uc.Query(ctx, req)
}

func (s *DatalayerService) Insert(ctx context.Context, req *v1.InsertRequest) (*v1.MutationResponse, error) {
	return s.uc.Insert(ctx, req)
}

func (s *DatalayerService) Update(ctx context.Context, req *v1.UpdateRequest) (*v1.MutationResponse, error) {
	return s.uc.Update(ctx, req)
}

func (s *DatalayerService) Delete(ctx context.Context, req *v1.DeleteRequest) (*v1.MutationResponse, error) {
	return s.uc.Delete(ctx, req)
}

func (s *DatalayerService) BeginTransaction(ctx context.Context, req *v1.BeginTransactionRequest) (*v1.BeginTransactionResponse, error) {
	return s.uc.BeginTransaction(ctx, req)
}

func (s *DatalayerService) CommitTransaction(ctx context.Context, req *v1.TransactionRequest) (*emptypb.Empty, error) {
	return s.uc.CommitTransaction(ctx, req)
}

func (s *DatalayerService) RollbackTransaction(ctx context.Context, req *v1.TransactionRequest) (*emptypb.Empty, error) {
	return s.uc.RollbackTransaction(ctx, req)
}

func (s *DatalayerService) ListTables(ctx context.Context, req *v1.ListTablesRequest) (*v1.ListTablesResponse, error) {
	return s.uc.ListTables(ctx, req)
}

func (s *DatalayerService) DescribeTable(ctx context.Context, req *v1.DescribeTableRequest) (*v1.DescribeTableResponse, error) {
	return s.uc.DescribeTable(ctx, req)
}

func (s *DatalayerService) ExecRawSQL(ctx context.Context, req *v1.ExecRawSQLRequest) (*v1.ExecRawSQLResponse, error) {
	return s.uc.ExecRawSQL(ctx, req)
}
