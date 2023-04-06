package types

// OP
const (
	OpBuy = iota + 1
	OpSell
)

// order status
const (
	Ordered = iota
	Completed
	Revoked
)

const (
	//Count 单次list还回条数
	Count = int32(10)
	//MaxMatchCount 系统最大撮合深度
	MaxMatchCount = 100
)
