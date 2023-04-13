package executor

import (
	"fmt"
	"github.com/33cn/chain33/common/db"
	"github.com/33cn/chain33/common/db/table"
	"github.com/33cn/chain33/types"
	zty "github.com/33cn/plugin/plugin/dapp/zksync/types"
)

/*
 * 用户合约存取kv数据时，key值前缀需要满足一定规范
 * 即key = keyPrefix + userKey
 * 需要字段前缀查询时，使用’-‘作为分割符号
 */

// 状态数据库中存储具体挂单信息
func calcOrderKey(orderID int64) []byte {
	key := fmt.Sprintf("%s"+"orderID:%022d", KeyPrefixStateDB, orderID)
	return []byte(key)
}

var opt_exchange_depth = &table.Option{
	Prefix:  KeyPrefixLocalDB,
	Name:    "depth",
	Primary: "price",
	Index:   nil,
}

// 重新设计表，list查询全部在订单信息localdb查询中
var opt_exchange_order = &table.Option{
	Prefix:  KeyPrefixLocalDB,
	Name:    "order",
	Primary: "orderID",
	Index:   []string{"market_order", "addr_status"},
}

var opt_exchange_history = &table.Option{
	Prefix:  KeyPrefixLocalDB,
	Name:    "history",
	Primary: "index",
	Index:   []string{"name", "addr_status"},
}

// NewMarketDepthTable 新建表
func NewMarketDepthTable(kvdb db.KV) *table.Table {
	rowmeta := NewMarketDepthRow()
	table, err := table.NewTable(rowmeta, kvdb, opt_exchange_depth)
	if err != nil {
		panic(err)
	}
	return table
}

// NewMarketOrderTable ...
func NewMarketOrderTable(kvdb db.KV) *table.Table {
	rowmeta := NewOrderRow()
	table, err := table.NewTable(rowmeta, kvdb, opt_exchange_order)
	if err != nil {
		panic(err)
	}
	return table
}

// NewHistoryOrderTable ...
func NewHistoryOrderTable(kvdb db.KV) *table.Table {
	rowmeta := NewHistoryOrderRow()
	table, err := table.NewTable(rowmeta, kvdb, opt_exchange_history)
	if err != nil {
		panic(err)
	}
	return table
}

// OrderRow table meta 结构
type OrderRow struct {
	*zty.Order
}

// NewOrderRow 新建一个meta 结构
func NewOrderRow() *OrderRow {
	return &OrderRow{Order: &zty.Order{}}
}

// CreateRow ...
func (r *OrderRow) CreateRow() *table.Row {
	return &table.Row{Data: &zty.Order{}}
}

// SetPayload 设置数据
func (r *OrderRow) SetPayload(data types.Message) error {
	if txdata, ok := data.(*zty.Order); ok {
		r.Order = txdata
		return nil
	}
	return types.ErrTypeAsset
}

// Get 按照indexName 查询 indexValue
func (r *OrderRow) Get(key string) ([]byte, error) {
	if key == "orderID" {
		return []byte(fmt.Sprintf("%022d", r.OrderID)), nil
	} else if key == "market_order" {
		return []byte(fmt.Sprintf("%d:%d:%d:%016d", r.GetSpotTradeInfo().LeftAssetTokenID, r.GetSpotTradeInfo().RightAssetTokenID, r.GetSpotTradeInfo().Op, r.GetSpotTradeInfo().Price)), nil
	} else if key == "addr_status" {
		return []byte(fmt.Sprintf("%d:%d", r.AccountID, r.Status)), nil
	}
	return nil, types.ErrNotFound
}

// HistoryOrderRow table meta 结构
type HistoryOrderRow struct {
	*zty.Order
}

// NewHistoryOrderRow ...
func NewHistoryOrderRow() *HistoryOrderRow {
	return &HistoryOrderRow{Order: &zty.Order{Value: &zty.Order_SpotTradeInfo{SpotTradeInfo: &zty.SpotTradeInfo{}}}}
}

// CreateRow ...
func (m *HistoryOrderRow) CreateRow() *table.Row {
	return &table.Row{Data: &zty.Order{Value: &zty.Order_SpotTradeInfo{SpotTradeInfo: &zty.SpotTradeInfo{}}}}
}

// SetPayload 设置数据
func (m *HistoryOrderRow) SetPayload(data types.Message) error {
	if txdata, ok := data.(*zty.Order); ok {
		m.Order = txdata
		return nil
	}
	return types.ErrTypeAsset
}

// Get 按照indexName 查询 indexValue
func (m *HistoryOrderRow) Get(key string) ([]byte, error) {
	if key == "index" {
		return []byte(fmt.Sprintf("%022d", m.Index)), nil
	} else if key == "name" {
		return []byte(fmt.Sprintf("%d:%d", m.GetSpotTradeInfo().LeftAssetTokenID, m.GetSpotTradeInfo().RightAssetTokenID)), nil
	} else if key == "addr_status" {
		return []byte(fmt.Sprintf("%d:%d", m.AccountID, m.Status)), nil
	}
	return nil, types.ErrNotFound
}

// MarketDepthRow table meta 结构
type MarketDepthRow struct {
	*zty.MarketDepth
}

// NewMarketDepthRow 新建一个meta 结构
func NewMarketDepthRow() *MarketDepthRow {
	return &MarketDepthRow{MarketDepth: &zty.MarketDepth{}}
}

// CreateRow 新建数据行(注意index 数据一定也要保存到数据中,不能就保存eventid)
func (m *MarketDepthRow) CreateRow() *table.Row {
	return &table.Row{Data: &zty.MarketDepth{}}
}

// SetPayload 设置数据
func (m *MarketDepthRow) SetPayload(data types.Message) error {
	if txdata, ok := data.(*zty.MarketDepth); ok {
		m.MarketDepth = txdata
		return nil
	}
	return types.ErrTypeAsset
}

// Get 按照indexName 查询 indexValue
func (m *MarketDepthRow) Get(key string) ([]byte, error) {
	if key == "price" {
		return []byte(fmt.Sprintf("%d:%d:%d:%016d", m.LeftAssetTokenID, m.RightAssetTokenID, m.Op, m.Price)), nil
	}
	return nil, types.ErrNotFound
}
