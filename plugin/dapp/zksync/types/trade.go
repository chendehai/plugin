package types

// OP
const (
	OpBuy = iota + 1
	OpSell
)

//order status
const (
	Ordered = iota
	Completed
	Revoked
)

//const
const (
	ListDESC = int32(0)
	ListASC  = int32(1)
	ListSeek = int32(2)
)

const (
	//Count 单次list还回条数
	Count = int32(10)
	//MaxMatchCount 系统最大撮合深度
	MaxMatchCount = 100
)
