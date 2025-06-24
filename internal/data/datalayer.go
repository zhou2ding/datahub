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
