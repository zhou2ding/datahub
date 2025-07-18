package data

import (
	"context"
	v1 "datahub/api/datalayer/v1"
	"datahub/internal/biz"
	"datahub/pkg/global"
	"datahub/pkg/md"
	"fmt"
	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"strings"
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

// 递归构建 GORM where 表达式和参数
func (r *DatalayerRepo) buildWhereConditions(ctx context.Context, wc *v1.WhereClause) (string, []any, error) {
	if wc == nil {
		return "", nil, nil
	}

	switch clauseType := wc.ClauseType.(type) {
	case *v1.WhereClause_Condition:
		//单条件查询
		cond := clauseType.Condition
		if cond.Field == "" {
			return "", nil, fmt.Errorf("condition field is required")
		}
		field := cond.Field

		op, placeholder, requiresValue := getGormOperator(cond.Operator)
		if op == "" {
			return "", nil, fmt.Errorf("unsupported operator: %s", cond.Operator)
		}

		if !requiresValue {
			// 处理 IS NULL, IS NOT NULL
			return fmt.Sprintf("%s %s", field, op), nil, nil
		}

		var valueArg any

		switch opVal := cond.OperandType.(type) {
		case *v1.Condition_LiteralValue:
			//普通查询
			literalProtoVal := opVal.LiteralValue
			val, err := protobufValueToAny(literalProtoVal)
			if err != nil {
				return "", nil, fmt.Errorf("invalid literal value for field '%s': %w", cond.Field, err)
			}

			// 专门处理 IN, NOT IN
			if cond.Operator == v1.Operator_IN || cond.Operator == v1.Operator_NOT_IN {
				listVal, ok := val.([]any)
				if !ok {
					return "", nil, fmt.Errorf("literal value for IN/NOT IN operator must be a list, got %T for field '%s'", val, cond.Field)
				}
				if len(listVal) == 0 {
					// 处理 IN/NOT IN 的空列表（IN () 总是假，NOT IN () 总是真）
					if cond.Operator == v1.Operator_IN {
						return "1=0", nil, nil
					} else {
						return "1=1", nil, nil
					}
				}
				valueArg = listVal
				return fmt.Sprintf("%s %s (?)", field, op), []any{valueArg}, nil
			}
			valueArg = val
			return fmt.Sprintf("%s %s %s", field, op, placeholder), []any{valueArg}, nil

		default:
			return "", nil, fmt.Errorf("condition for field '%s' requires a value or subquery but received unknown type: %T", cond.Field, cond.OperandType)
		}

	case *v1.WhereClause_NestedClause:
		//多条件查询
		nested := clauseType.NestedClause
		if len(nested.Clauses) == 0 {
			return "", nil, nil
		}

		var subExprs []string
		var allArgs []any
		logicOp := " AND "
		if nested.LogicalOperator == v1.LogicalOperator_OR {
			logicOp = " OR "
		}

		for i, subClause := range nested.Clauses {
			subExpr, subArgs, err := r.buildWhereConditions(ctx, subClause)
			if err != nil {
				return "", nil, fmt.Errorf("error in nested clause element %d: %w", i, err)
			}
			if subExpr != "" {
				// 为确保正确优先级，将子表达式包裹在括号中
				subExprs = append(subExprs, "("+subExpr+")")
				allArgs = append(allArgs, subArgs...)
			}
		}

		if len(subExprs) == 0 {
			return "", nil, nil
		}

		return strings.Join(subExprs, logicOp), allArgs, nil

	default:
		return "", nil, fmt.Errorf("unknown where clause type: %T", wc.ClauseType)
	}
}

// 构建子查询语句
func (r *DatalayerRepo) buildSubQuery(ctx context.Context, req *v1.QueryRequest) (*gorm.DB, error) {
	if req.Table == nil || req.Table.TableName == "" {
		return nil, fmt.Errorf("subquery table and table_name required")
	}
	if req.Table.DbName == "" {
		return nil, fmt.Errorf("subquery DbName required")
	}

	db := r.data.db[req.Table.DbName].WithContext(ctx)
	if req.TransactionId != "" {
		tx, ok := r.data.GetTransaction(req.TransactionId)
		if !ok {
			return nil, fmt.Errorf("transaction %s for subquery not found or expired", req.TransactionId)
		}
		db = tx.WithContext(ctx)
	}

	db = db.Table(req.Table.TableName)

	// 1. 构建 Select 子句
	selectClauses := make([]string, 0, len(req.SelectFields))
	if len(req.SelectFields) > 0 {
		for _, sf := range req.SelectFields {
			selectClauses = append(selectClauses, r.data.db[req.Table.DbName].NamingStrategy.ColumnName("", sf))
		}
	}
	for _, agg := range req.Aggregations {
		aggStr, err := buildAggregationClause(agg)
		if err != nil {
			return nil, fmt.Errorf("subquery aggregation error: %w", err)
		}
		selectClauses = append(selectClauses, aggStr)
	}

	if len(selectClauses) == 0 {
		// Depending on the SQL dialect and usage (e.g. EXISTS), SELECT * might be implied
		// or an error if not selecting specific columns for IN/scalar comparison.
		// For safety, require explicit select for subqueries unless it's an EXISTS.
		// However, GORM might handle `db.Model(&SomeModel{})` without explicit Select for subqueries too.
		// To be safe for IN or scalar, an explicit select is better.
		// If for EXISTS, you could db.Select("1")
		return nil, fmt.Errorf("subquery must have select_fields or aggregations defined")
	}
	db = db.Select(strings.Join(selectClauses, ", "))

	// 2. 构建 Where 子句
	for _, join := range req.Joins {
		joinStr, err := buildJoinClause(req.Table.TableName, join)
		if err != nil {
			return nil, fmt.Errorf("subquery join error: %w", err)
		}
		db = db.Joins(joinStr)
	}

	// 3. 构建 Where 子句 (Recursive call potential here)
	if req.WhereClause != nil {
		whereExpr, args, err := r.buildWhereConditions(ctx, req.WhereClause)
		if err != nil {
			return nil, fmt.Errorf("subquery where clause error: %w", err)
		}
		if whereExpr != "" {
			db = db.Where(whereExpr, args...)
		}
	}

	// 4. 构建 Group By 子句
	if req.GroupBy != nil && len(req.GroupBy.Fields) > 0 {
		quotedGroupByFields := make([]string, len(req.GroupBy.Fields))
		for i, f := range req.GroupBy.Fields {
			quotedGroupByFields[i] = r.data.db[req.Table.DbName].NamingStrategy.ColumnName("", f)
		}
		db = db.Group(strings.Join(quotedGroupByFields, ", "))
	}

	// 5. 构建 Having 子句 (Recursive call potential here)
	if req.HavingClause != nil {
		havingExpr, args, err := r.buildWhereConditions(ctx, req.HavingClause) // Pass ctx and r
		if err != nil {
			return nil, fmt.Errorf("subquery having clause error: %w", err)
		}
		if havingExpr != "" {
			db = db.Having(havingExpr, args...)
		}
	}

	return db, nil
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

// 构建 GORM Joins 字符串
func buildJoinClause(primaryTable string, join *v1.Join) (string, error) {
	if join.TargetTable == "" {
		return "", fmt.Errorf("join target_table is required")
	}
	if len(join.OnConditions) == 0 {
		return "", fmt.Errorf("join on_conditions are required")
	}

	joinTypeStr := ""
	switch join.Type {
	case v1.JoinType_INNER:
		joinTypeStr = "INNER JOIN"
	case v1.JoinType_LEFT:
		joinTypeStr = "LEFT JOIN"
	case v1.JoinType_RIGHT:
		joinTypeStr = "RIGHT JOIN"
	case v1.JoinType_JOIN_TYPE_UNSPECIFIED:
		joinTypeStr = "INNER JOIN" //默认为 INNER JOIN
	default:
		return "", fmt.Errorf("unsupported join type: %s", join.Type)
	}

	var onConditionStrings []string
	for _, cond := range join.OnConditions {
		if cond.FieldFromPrimaryTable == "" || cond.FieldFromJoinedTable == "" {
			return "", fmt.Errorf("join condition fields cannot be empty")
		}
		// 如果未指定或无效，默认为 EQ 操作符用于连接
		opStr := "="
		if cond.Operator != v1.Operator_OPERATOR_UNSPECIFIED && cond.Operator != v1.Operator_EQ {
			// 通常仅允许 EQ 用于连接
			return "", fmt.Errorf("only EQ operator is typically supported in JOIN ON conditions, got: %s", cond.Operator)
		}

		quotedPrimaryTable := "`" + primaryTable + "`"
		quotedTargetTable := "`" + join.TargetTable + "`"

		quotedPrimaryField := "`" + cond.FieldFromPrimaryTable + "`"
		quotedJoinedField := "`" + cond.FieldFromJoinedTable + "`"

		// 拼接成 table.field 格式
		qualifiedPrimaryField := fmt.Sprintf("%s.%s", quotedPrimaryTable, quotedPrimaryField)
		qualifiedJoinedField := fmt.Sprintf("%s.%s", quotedTargetTable, quotedJoinedField)

		onConditionStrings = append(onConditionStrings, fmt.Sprintf("%s %s %s", qualifiedPrimaryField, opStr, qualifiedJoinedField))
	}

	// 格式: JOIN_TYPE target_table ON (condition1 AND condition2 ...)
	return fmt.Sprintf("%s %s ON %s", joinTypeStr, join.TargetTable, strings.Join(onConditionStrings, " AND ")), nil
}

func (r *DatalayerRepo) Insert(ctx context.Context, req *v1.InsertRequest) (*v1.MutationResponse, error) {
	if req.Table == nil {
		return nil, errors.BadRequest(v1.ReasonInvalidArgument, "table required")
	}
	if len(req.Rows) == 0 {
		return nil, errors.BadRequest(v1.ReasonInvalidArgument, "rows cannot be empty")
	}

	traceId := md.GetMetadata(ctx, global.RequestIdMd)
	r.log.Debugf("traceId: %s insert req: %+v", traceId, req)

	db := r.data.db[req.Table.DbName].WithContext(ctx)
	if req.TransactionId != "" {
		tx, ok := r.data.GetTransaction(req.TransactionId)
		if !ok {
			return nil, errors.NotFound(v1.ReasonInvalidTransactionID, fmt.Sprintf("transaction %s not found or expired", req.TransactionId))
		}
		db = tx.WithContext(ctx) // 在事务中执行
		r.log.Debugf("traceId: %s insert is executing within transaction: %s", traceId, req.TransactionId)
	}

	// 1. 转换数据类型
	recordsToInsert := make([]map[string]any, 0, len(req.Rows))
	for i, protoRow := range req.Rows {
		if protoRow == nil || len(protoRow.Fields) == 0 {
			r.log.Warnf("traceId: %s skipping empty row at index %d during insert into table %s", traceId, i, req.Table)
			continue
		}

		record := make(map[string]any)
		for key, protoVal := range protoRow.Fields {
			goVal, err := protobufValueToAny(protoVal)
			if err != nil {
				r.log.Errorf("traceId: %s failed to convert value for key '%s' in row %d: %v", traceId, key, i, err)
				return nil, errors.BadRequest(v1.ReasonInvalidArgument, fmt.Sprintf("invalid value for field '%s': %v", key, err))
			}
			record[key] = goVal
		}
		recordsToInsert = append(recordsToInsert, record)
	}

	if len(recordsToInsert) == 0 {
		r.log.Warnf("traceId: %s no valid rows to insert into table %s after processing input.", traceId, req.Table)
		return &v1.MutationResponse{AffectedRows: 0}, nil
	}

	// 2. 构建 GORM 操作
	tx := db.Table(req.Table.TableName)

	// 处理冲突策略
	switch req.OnConflict {
	case v1.ConflictAction_IGNORE:
		tx = tx.Clauses(clause.OnConflict{DoNothing: true})
	case v1.ConflictAction_UPSERT:
		if len(req.ConflictColumns) == 0 {
			return nil, errors.BadRequest(v1.ReasonInvalidArgument, "conflict_columns field is required for UPSERT operation")
		}
		if len(req.UpdateColumns) == 0 {
			return nil, errors.BadRequest(v1.ReasonInvalidArgument, "update_columns field is required for UPSERT operation")
		}
		cols := make([]clause.Column, len(req.ConflictColumns))
		for i, col := range req.ConflictColumns {
			cols[i] = clause.Column{Name: col}
		}
		// 更新指定的列
		tx = tx.Clauses(clause.OnConflict{
			Columns:   cols,
			DoUpdates: clause.AssignmentColumns(req.UpdateColumns),
		})
	case v1.ConflictAction_FAIL, v1.ConflictAction_CONFLICT_ACTION_UNSPECIFIED:
		// 默认行为，如果冲突则数据库会报错
	default:
		return nil, errors.BadRequest(v1.ReasonInvalidArgument, fmt.Sprintf("unsupported conflict action: %s", req.OnConflict))
	}

	result := tx.Create(&recordsToInsert)
	if result.Error != nil {
		r.log.Errorf("traceId: %s insert failed to table %s: %v", traceId, req.Table, result.Error)
		if strings.Contains(result.Error.Error(), "Duplicate") {
			return nil, errors.Conflict(v1.ReasonDuplicate, result.Error.Error())
		} else {
			return nil, errors.InternalServer(v1.ReasonInsertFailed, result.Error.Error())
		}
	}

	resp := &v1.MutationResponse{
		AffectedRows: result.RowsAffected,
	}

	return resp, nil
}

func (r *DatalayerRepo) Update(ctx context.Context, req *v1.UpdateRequest) (*v1.MutationResponse, error) {
	if req.Table == nil {
		return nil, errors.BadRequest(v1.ReasonInvalidArgument, "table required")
	}
	if req.Data == nil || len(req.Data.Fields) == 0 {
		return nil, errors.BadRequest(v1.ReasonInvalidArgument, "update data cannot be empty")
	}
	if req.WhereClause == nil {
		return nil, errors.BadRequest(v1.ReasonInvalidArgument, "where clause is required for updates")
	}

	traceId := md.GetMetadata(ctx, global.RequestIdMd)

	r.log.Debugf("traceId: %s update req: Table=%s, Data=%v, Where=%v, TxID=%s", traceId, req.Table, req.Data, req.WhereClause, req.TransactionId)

	db := r.data.db[req.Table.DbName].WithContext(ctx)
	if req.TransactionId != "" {
		tx, ok := r.data.GetTransaction(req.TransactionId)
		if !ok {
			return nil, errors.NotFound(v1.ReasonInvalidTransactionID, fmt.Sprintf("transaction %s not found or expired", req.TransactionId))
		}
		db = tx.WithContext(ctx) // 在事务中执行
		r.log.Debugf("traceId: %s update is executing within transaction: %s", traceId, req.TransactionId)
	}

	// 1. 构造update map
	updateData := make(map[string]any)
	for key, protoVal := range req.Data.Fields {
		goVal, err := protobufValueToAny(protoVal)
		if err != nil {
			r.log.Errorf("traceId: %s failed to convert update value for key '%s': %v", traceId, key, err)
			return nil, errors.BadRequest(v1.ReasonInvalidArgument, fmt.Sprintf("invalid value for field '%s': %v", key, err))
		}
		updateData[key] = goVal
	}

	if len(updateData) == 0 {
		r.log.Warnf("traceId: %s no valid update data provided after conversion", traceId)
		return &v1.MutationResponse{AffectedRows: 0}, nil
	}

	db = db.Table(req.Table.TableName)

	// 2. 构建where子句
	whereExpr, args, err := r.buildWhereConditions(ctx, req.WhereClause)
	if err != nil {
		return nil, errors.BadRequest("INVALID_WHERE_CLAUSE", err.Error())
	}
	if whereExpr == "" {
		r.log.Warnf("traceId: %s update on table '%s' resulted in an empty effective WHERE clause!!!", traceId, req.Table)
	} else {
		db = db.Where(whereExpr, args...)
	}

	result := db.Updates(updateData)
	if result.Error != nil {
		r.log.Errorf("traceId: %s update failed for table %s: %v", traceId, req.Table, result.Error)
		return nil, errors.InternalServer(v1.ReasonUpdateFailed, result.Error.Error())
	}

	resp := &v1.MutationResponse{
		AffectedRows: result.RowsAffected,
	}

	return resp, nil
}

func (r *DatalayerRepo) Delete(ctx context.Context, req *v1.DeleteRequest) (*v1.MutationResponse, error) {
	if req.Table == nil {
		return nil, errors.BadRequest(v1.ReasonInvalidArgument, "table required")
	}
	if req.WhereClause == nil {
		return nil, errors.BadRequest(v1.ReasonInvalidArgument, "where clause is required for delete")
	}

	traceId := md.GetMetadata(ctx, global.RequestIdMd)

	r.log.Debugf("traceId: %s delete req: %+v", traceId, req)

	db := r.data.db[req.Table.DbName].WithContext(ctx)
	if req.TransactionId != "" {
		tx, ok := r.data.GetTransaction(req.TransactionId)
		if !ok {
			r.log.Warnf("traceId: %s delete failed: transaction %s not found or expired", traceId, req.TransactionId)
			return nil, errors.NotFound(v1.ReasonInvalidTransactionID, fmt.Sprintf("transaction %s not found or expired", req.TransactionId))
		}
		db = tx.WithContext(ctx) // 在事务中执行
		r.log.Debugf("traceId: %s delete is executing within transaction: %s", traceId, req.TransactionId)
	}

	// 1. 构建 WHERE 子句
	whereExpr, args, err := r.buildWhereConditions(ctx, req.WhereClause)
	if err != nil {
		r.log.Errorf("traceId: %s failed to build where conditions for delete on table %s: %v", traceId, req.Table, err)
		return nil, errors.BadRequest(v1.ReasonInvalidWhereClause, err.Error())
	}

	if whereExpr == "" {
		r.log.Errorf("traceId: %s delete on table '%s' aborted: effective WHERE clause is empty", traceId, req.Table)
		return nil, errors.BadRequest(v1.ReasonInvalidWhereClause, "effective WHERE clause is empty")
	}

	result := db.Table(req.Table.TableName).Where(whereExpr, args...).Delete(nil)

	if result.Error != nil {
		r.log.Errorf("traceId: %s database delete failed for table %s: %v", traceId, req.Table, result.Error)
		return nil, errors.InternalServer(v1.ReasonDeleteFailed, result.Error.Error())
	}

	resp := &v1.MutationResponse{
		AffectedRows: result.RowsAffected,
	}

	return resp, nil
}

func (r *DatalayerRepo) BeginTransaction(ctx context.Context, req *v1.BeginTransactionRequest) (*v1.BeginTransactionResponse, error) {
	traceId := md.GetMetadata(ctx, global.RequestIdMd)
	txID, _, err := r.data.BeginTransaction(req.DbName)
	if err != nil {
		r.log.Errorf("traceId: %s failed to begin transaction: %v", traceId, err)
		return nil, errors.InternalServer(v1.ReasonTransactionError, fmt.Sprintf("failed to begin transaction: %v", err))
	}

	r.log.Infof("traceId: %s successfully started new transaction, id: %s", traceId, txID)
	return &v1.BeginTransactionResponse{
		TransactionId: txID,
	}, nil
}

func (r *DatalayerRepo) CommitTransaction(ctx context.Context, req *v1.TransactionRequest) (*emptypb.Empty, error) {
	traceId := md.GetMetadata(ctx, global.RequestIdMd)
	if req.TransactionId == "" {
		r.log.Warnf("traceId: %s commit transaction failed: transaction_id cannot be empty", traceId)
		return nil, errors.BadRequest(v1.ReasonInvalidArgument, "transaction_id is required")
	}

	r.log.Infof("traceId: %s commit transaction request for id: %s", req.TransactionId, traceId)

	tx, ok := r.data.GetTransaction(req.TransactionId)
	if !ok {
		r.log.Warnf("traceId: %s commit transaction failed: transaction %s not found or expired", traceId, req.TransactionId)
		return nil, errors.NotFound(v1.ReasonInvalidTransactionID, fmt.Sprintf("transaction %s not found or expired", req.TransactionId))
	}

	err := tx.Commit().Error
	// 无论成功或失败，都需要从 map 中移除事务记录
	r.data.RemoveTransaction(req.TransactionId)

	if err != nil {
		r.log.Errorf("traceId: %s failed to commit transaction %s: %v", traceId, req.TransactionId, err)
		return nil, errors.InternalServer(v1.ReasonTransactionCommitFailed, fmt.Sprintf("failed to commit transaction %s: %v", req.TransactionId, err))
	}

	r.log.Infof("traceId: %s transaction %s committed successfully", traceId, req.TransactionId)
	return &emptypb.Empty{}, nil
}

func (r *DatalayerRepo) RollbackTransaction(ctx context.Context, req *v1.TransactionRequest) (*emptypb.Empty, error) {
	traceId := md.GetMetadata(ctx, global.RequestIdMd)
	if req.TransactionId == "" {
		r.log.Warnf("traceId: %s rollback transaction failed: transaction_id cannot be empty", traceId)
		return nil, errors.BadRequest(v1.ReasonInvalidArgument, "transaction_id is required")
	}

	r.log.Infof("traceId: %s rollback transaction request for id: %s", traceId, req.TransactionId)

	tx, ok := r.data.GetTransaction(req.TransactionId)
	if !ok {
		r.log.Warnf("traceId: %s transaction %s not found or expired", traceId, req.TransactionId)
		return nil, nil
	}

	err := tx.Rollback().Error
	// 无论成功或失败，都需要从 map 中移除事务记录
	r.data.RemoveTransaction(req.TransactionId)

	if err != nil {
		r.log.Errorf("traceId: %s failed to rollback transaction %s: %v", req.TransactionId, traceId, err)
		return nil, errors.InternalServer(v1.ReasonTransactionRollbackFailed, fmt.Sprintf("failed to rollback transaction %s: %v", req.TransactionId, err))
	}

	return &emptypb.Empty{}, nil
}

func (r *DatalayerRepo) ListTables(ctx context.Context, req *v1.ListTablesRequest) (*v1.ListTablesResponse, error) {
	tables, err := r.data.db[req.DbName].Migrator().GetTables()
	if err != nil {
		r.log.Warnf("traceId: %s list tables error: %v", md.GetMetadata(ctx, global.RequestIdMd), err)
		return nil, errors.InternalServer(v1.ReasonListTablesFailed, err.Error())
	}
	return &v1.ListTablesResponse{TableNames: tables}, nil
}

func (r *DatalayerRepo) DescribeTable(ctx context.Context, req *v1.DescribeTableRequest) (*v1.DescribeTableResponse, error) {
	if req.Table == nil {
		return nil, errors.BadRequest(v1.ReasonInvalidArgument, "table required")
	}

	traceId := md.GetMetadata(ctx, global.RequestIdMd)

	db := r.data.db[req.Table.DbName]
	migrator := db.Migrator()

	if !migrator.HasTable(req.Table.TableName) {
		r.log.Warnf("traceId: %s table %s not found", traceId, req.Table.TableName)
		return nil, errors.NotFound(v1.ReasonDescribeTablesFailed, fmt.Sprintf("table '%s' not found", req.Table.TableName))
	}

	resp := &v1.DescribeTableResponse{
		TableName: req.Table.TableName,
		Columns:   make([]*v1.ColumnMetadata, 0),
		Indices:   make([]*v1.IndexMetadata, 0),
	}

	// 2. Get Column Information
	columnTypes, err := migrator.ColumnTypes(req.Table.TableName)
	if err != nil {
		r.log.Errorf("traceId: %s failed to get column types for table %s: %v", traceId, req.Table.TableName, err)
		return nil, errors.InternalServer(v1.ReasonDescribeTablesFailed, err.Error())
	}

	for _, colType := range columnTypes {
		colMeta := &v1.ColumnMetadata{
			Name:     colType.Name(),
			DataType: colType.DatabaseTypeName(),
		}

		if nullable, ok := colType.Nullable(); ok {
			colMeta.IsNullable = nullable
		}
		if defaultValue, ok := colType.DefaultValue(); ok {
			colMeta.DefaultValue = defaultValue
		}
		if isPrimary, ok := colType.PrimaryKey(); ok {
			colMeta.IsPrimaryKey = isPrimary
		}
		if length, ok := colType.Length(); ok {
			colMeta.MaxLength = length
		}

		resp.Columns = append(resp.Columns, colMeta)
	}

	indexes, err := migrator.GetIndexes(req.Table.TableName)
	if err != nil {
		r.log.Errorf("traceId: %s failed to get indexes for table %s: %v", traceId, req.Table.TableName, err)
		return nil, errors.InternalServer(v1.ReasonDescribeTablesFailed, err.Error())
	}

	for _, index := range indexes {
		idxMeta := &v1.IndexMetadata{
			Name:      index.Name(),
			Columns:   index.Columns(),
			IndexType: "",
		}
		if isUnique, ok := index.Unique(); ok {
			idxMeta.IsUnique = isUnique
		}

		resp.Indices = append(resp.Indices, idxMeta)
	}

	return resp, nil
}

func (r *DatalayerRepo) ExecRawSQL(ctx context.Context, req *v1.ExecRawSQLRequest) (*v1.ExecRawSQLResponse, error) {
	traceId := md.GetMetadata(ctx, global.RequestIdMd)
	if req.Db == "" {
		return nil, errors.BadRequest(v1.ReasonInvalidArgument, "db required")
	}
	if req.Sql == "" {
		return nil, errors.BadRequest(v1.ReasonInvalidArgument, "sql required")
	}

	db := r.data.db[req.Db].WithContext(ctx)
	if req.TransactionId != "" {
		tx, ok := r.data.GetTransaction(req.TransactionId)
		if !ok {
			return nil, errors.NotFound(v1.ReasonInvalidTransactionID, fmt.Sprintf("transaction %s not found", req.TransactionId))
		}
		db = tx
		r.log.Debugf("traceId:%s execute raw sql within transaction %s", traceId, req.TransactionId)
	}

	result := db.Exec(req.Sql)
	if err := result.Error; err != nil {
		r.log.Errorf("traceId: %s failed to execute raw sql: %v", traceId, err)
		return nil, errors.InternalServer(v1.ReasonExecRawSqlFailed, err.Error())
	}

	return &v1.ExecRawSQLResponse{AffectedRows: result.RowsAffected}, nil
}
