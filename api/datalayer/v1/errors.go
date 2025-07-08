package v1

const (
	ReasonInvalidArgument     = "INVALID_ARGUMENT"
	ReasonInvalidJoin         = "INVALID_JOIN"
	ReasonInvalidAggregation  = "INVALID_AGGREGATION"
	ReasonInvalidWhereClause  = "INVALID_WHERE_CLAUSE"
	ReasonInvalidHavingClause = "INVALID_HAVING_CLAUSE"

	ReasonQueryFailed  = "QUERY_FAILED"
	ReasonNotFound     = "NOT_FOUND"
	ReasonInsertFailed = "INSERT_FAILED"
	ReasonDuplicate    = "DUPLICATE"
	ReasonUpdateFailed = "UPDATE_FAILED"
	ReasonDeleteFailed = "DELETE_FAILED"

	ReasonTransactionError          = "TRANSACTION_ERROR"
	ReasonTransactionCommitFailed   = "TRANSACTION_COMMIT_FAILED"
	ReasonTransactionRollbackFailed = "TRANSACTION_ROLLBACK_FAILED"
	ReasonInvalidTransactionID      = "INVALID_TRANSACTION_ID"

	ReasonListTablesFailed     = "LIST_TABLES_FAILED"
	ReasonDescribeTablesFailed = "DESCRIBE_TABLE_FAILED"

	ReasonExecRawSqlFailed = "EXEC_Raw_SQL_FAILED"
)
