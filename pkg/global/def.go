package global

// 上下文常量
const (
	RequestIdCtx = "requestid"
	StartTimeCtx = "starttime"
)

// grpc元数据常量，Kratos的全局传递格式：x-md-global-xxx
const (
	RequestIdMd = "x-md-global-requestid"
	RemoteIpMd  = "x-md-global-remoteip"
)
