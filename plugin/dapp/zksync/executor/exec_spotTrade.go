package executor

import (
	"encoding/hex"
	"fmt"
	dbm "github.com/33cn/chain33/common/db"
	tab "github.com/33cn/chain33/common/db/table"
	"github.com/33cn/chain33/types"
	et "github.com/33cn/plugin/plugin/dapp/exchange/types"
	zt "github.com/33cn/plugin/plugin/dapp/zksync/types"
	"github.com/pkg/errors"
	"math/big"
)

func (a *Action) ZkSpotTrade(payload *zt.ZkSpotTrade) (*types.Receipt, error) {
	tradeInfo := payload.SpotTradeInfo

	fromLeaf, err := GetLeafByAccountId(a.statedb, tradeInfo.GetFromAccountID())
	if err != nil {
		return nil, errors.Wrapf(err, "db.GetLeafByAccountId")
	}
	if fromLeaf == nil {
		return nil, errors.New("account not exist")
	}
	err = authVerification(payload.Signature.PubKey, fromLeaf.PubKey)
	if err != nil {
		return nil, errors.Wrapf(err, "authVerification")
	}

	leftTokenID := tradeInfo.GetLeftAssetTokenID()
	rightTokenID := tradeInfo.GetRightAssetTokenID()
	cfg := a.api.GetConfig()
	if leftTokenID == rightTokenID {
		return nil, errors.Wrapf(types.ErrNotAllow, "asset with the same token ID =%d", leftTokenID)
	}
	if !checkTradeAmount(tradeInfo.GetAmount(), cfg.GetCoinPrecision()) {
		return nil, zt.ErrAssetAmount
	}
	if !checkPrice(tradeInfo.GetPrice()) {
		return nil, zt.ErrAssetPrice
	}
	if !checkOp(tradeInfo.GetOp()) {
		return nil, zt.ErrAssetOp
	}

	//leftAssetDB, err := account.NewAccountDB(cfg, leftTokenID.GetExecer(), leftTokenID.GetSymbol(), a.statedb)
	//if err != nil {
	//	return nil, err
	//}
	//rightAssetDB, err := account.NewAccountDB(cfg, rightTokenID.GetExecer(), rightTokenID.GetSymbol(), a.statedb)
	//if err != nil {
	//	return nil, err
	//}

	//Check your account balance first
	if tradeInfo.GetOp() == et.OpBuy {
		amount := safeMul(tradeInfo.GetAmount(), tradeInfo.GetPrice(), cfg.GetCoinPrecision())
		rightAccount := rightAssetDB.LoadExecAccount(a.fromaddr, a.execaddr)
		if rightAccount.Balance < amount {
			zlog.Error("limit check right balance", "addr", a.fromaddr, "avail", rightAccount.Balance, "need", amount)
			return nil, et.ErrAssetBalance
		}
		return a.matchLimitOrder(tradeInfo)

	}
	if tradeInfo.GetOp() == et.OpSell {
		amount := tradeInfo.GetAmount()
		leftAccount := leftAssetDB.LoadExecAccount(a.fromaddr, a.execaddr)
		if leftAccount.Balance < amount {
			zlog.Error("limit check left balance", "addr", a.fromaddr, "avail", leftAccount.Balance, "need", amount)
			return nil, et.ErrAssetBalance
		}
		return a.matchLimitOrder(tradeInfo, leftAssetDB, rightAssetDB, entrustAddr)
	}
	return nil, fmt.Errorf("unknow op")
}

func (a *Action) ZkRevokeTrade(payload *zt.ZkRevokeTrade) (*types.Receipt, error) {

	//此处的decimal无用
	return a.l2TransferProc(payload, actionTy, 18)
}

func (a *Action) ZkTransfer2Ex(payload *zt.ZkTransfer2Ex) (*types.Receipt, error) {

	//此处的decimal无用
	return a.l2TransferProc(payload, actionTy, 18)
}

// set the transaction logic method
// rules:
// 1. The purchase price is higher than the market price, and the price is matched from low to high.
// 2. Sell orders are matched at prices lower than market prices.
// 3. Match the same prices on a first-in, first-out basis
func (a *Action) matchLimitOrder(tradeInfo *zt.SpotTradeInfo) (*types.Receipt, error) {
	var (
		logs         []*types.ReceiptLog
		kvs          []*types.KeyValue
		priceKey     string
		count        int
		taker        int32
		maker        int32
		minFee       int64
		leftTokenID  uint32
		rightTokenID uint32
	)

	leftTokenID = tradeInfo.LeftAssetTokenID
	rightTokenID = tradeInfo.RightAssetTokenID
	cfg := a.api.GetConfig()
	tCfg, err := parseConfig(a.api.GetConfig(), a.height)
	if err != nil {
		zlog.Error("executor/exchangedb matchLimitOrder.ParseConfig", "err", err)
		return nil, err
	}

	if cfg.IsDappFork(a.height, et.ExchangeX, et.ForkFix1) && tCfg.IsBankAddr(a.fromaddr) {
		return nil, et.ErrAddrIsBank
	}

	if !tCfg.IsFeeFreeAddr(a.fromaddr) {
		trade := tCfg.GetTrade(tradeInfo.GetLeftAsset(), tradeInfo.GetRightAsset())
		taker = trade.GetTaker()
		maker = trade.GetMaker()
		minFee = trade.GetMinFee()
	}

	or := &zt.Order{
		OrderID:    a.GetIndex(),
		Value:      &zt.Order_SpotTradeInfo{tradeInfo},
		Ty:         et.TyLimitOrderAction,
		Executed:   0,
		AVGPrice:   0,
		Balance:    tradeInfo.GetAmount(),
		Status:     et.Ordered,
		Addr:       a.fromaddr,
		UpdateTime: a.blocktime,
		Index:      a.GetIndex(),
		Rate:       maker,
		MinFee:     minFee,
		Hash:       hex.EncodeToString(a.txhash),
		CreateTime: a.blocktime,
	}
	re := &zt.ReceiptExchange{
		Order: or,
		Index: a.GetIndex(),
	}

	// A single transaction can match up to 100 historical orders, the maximum depth can be matched, the system has to protect itself
	// Iteration has listing price
	var done bool
	for {
		if count >= et.MaxMatchCount {
			break
		}
		if done {
			break
		}
		//Obtain price information of existing market listing
		marketDepthList, _ := queryMarketDepth(a.localDB, leftTokenID, rightTokenID, a.OpSwap(tradeInfo.Op), priceKey, zt.Count)
		if marketDepthList == nil || len(marketDepthList.List) == 0 {
			break
		}
		for _, marketDepth := range marketDepthList.List {
			zlog.Info("LimitOrder debug find depth", "height", a.height, "amount", marketDepth.Amount, "price", marketDepth.Price, "order-price", tradeInfo.GetPrice(), "op", a.OpSwap(tradeInfo.Op), "index", a.GetIndex())
			if count >= et.MaxMatchCount {
				done = true
				break
			}
			if tradeInfo.Op == et.OpBuy && marketDepth.Price > tradeInfo.GetPrice() {
				done = true
				break
			}
			if tradeInfo.Op == et.OpSell && marketDepth.Price < tradeInfo.GetPrice() {
				done = true
				break
			}

			var hasOrder = false
			var orderKey string
			for {
				if count >= et.MaxMatchCount {
					done = true
					break
				}
				orderList, err := findOrderIDListByPrice(a.localDB, leftTokenID, rightTokenID, marketDepth.Price, a.OpSwap(tradeInfo.Op), et.ListASC, orderKey)
				if orderList != nil && !hasOrder {
					hasOrder = true
				}
				if err != nil {
					if err == types.ErrNotFound {
						break
					}
					zlog.Error("findOrderIDListByPrice error", "height", a.height, "token ID", leftTokenID, "price", marketDepth.Price, "op", a.OpSwap(tradeInfo.Op), "error", err)
					return nil, err
				}
				for _, matchorder := range orderList.List {
					if count >= et.MaxMatchCount {
						done = true
						break
					}
					// Check the order status
					order, err := findOrderByOrderID(a.statedb, matchorder.GetOrderID())
					if err != nil || order.Status != et.Ordered {
						if len(orderList.List) == 1 {
							hasOrder = true
						}
						continue
					}
					log, kv, err := a.matchModel(leftTokenID, rightTokenID, tradeInfo, order, or, re, tCfg.GetFeeAddr(), taker) // payload, or redundant
					if err != nil {
						if err == types.ErrNoBalance {
							zlog.Warn("matchModel RevokeOrder", "height", a.height, "orderID", order.GetOrderID(), "payloadID", or.GetOrderID(), "error", err)
							continue
						}
						return nil, err
					}
					logs = append(logs, log...)
					kvs = append(kvs, kv...)
					if or.Status == et.Completed {
						receiptlog := &types.ReceiptLog{Ty: et.TyLimitOrderLog, Log: types.Encode(re)}
						logs = append(logs, receiptlog)
						receipts := &types.Receipt{Ty: types.ExecOk, KV: kvs, Logs: logs}
						return receipts, nil
					}
					// match depth count
					count = count + 1
				}
				if orderList.PrimaryKey == "" {
					break
				}
				orderKey = orderList.PrimaryKey
			}
			if !hasOrder {
				var matchorder et.Order
				matchorder.UpdateTime = a.blocktime
				matchorder.Status = et.Completed
				matchorder.Balance = 0
				matchorder.Executed = 0
				matchorder.AVGPrice = marketDepth.Price
				zlog.Info("make empty match to del depth", "height", a.height, "price", marketDepth.Price, "amount", marketDepth.Amount)
				re.MatchOrders = append(re.MatchOrders, &matchorder)
			}
		}

		if marketDepthList.PrimaryKey == "" {
			break
		}
		priceKey = marketDepthList.PrimaryKey
	}

	//Outstanding orders require freezing of the remaining unclosed funds
	if tradeInfo.Op == et.OpBuy {
		amount := calcActualCost(et.OpBuy, or.Balance, tradeInfo.Price, cfg.GetCoinPrecision())
		receipt, err := rightAccountDB.ExecFrozen(a.fromaddr, a.execaddr, amount)
		if err != nil {
			zlog.Error("LimitOrder.ExecFrozen OpBuy", "addr", a.fromaddr, "amount", amount, "err", err.Error())
			return nil, err
		}
		logs = append(logs, receipt.Logs...)
		kvs = append(kvs, receipt.KV...)
	}
	if tradeInfo.Op == et.OpSell {
		amount := calcActualCost(et.OpSell, or.Balance, tradeInfo.Price, cfg.GetCoinPrecision())
		receipt, err := leftAccountDB.ExecFrozen(a.fromaddr, a.execaddr, amount)
		if err != nil {
			zlog.Error("LimitOrder.ExecFrozen OpSell", "addr", a.fromaddr, "amount", amount, "err", err.Error())
			return nil, err
		}
		logs = append(logs, receipt.Logs...)
		kvs = append(kvs, receipt.KV...)
	}
	kvs = append(kvs, a.GetKVSet(or)...)
	re.Order = or
	receiptlog := &types.ReceiptLog{Ty: et.TyLimitOrderLog, Log: types.Encode(re)}
	logs = append(logs, receiptlog)
	receipts := &types.Receipt{Ty: types.ExecOk, KV: kvs, Logs: logs}
	return receipts, nil
}

func (a *Action) matchModel(leftTokenID, rightTokenID uint32, payload *zt.SpotTradeInfo, matchorder *zt.Order, or *zt.Order, re *zt.ReceiptExchange, feeAddr string, taker int32) ([]*types.ReceiptLog, []*types.KeyValue, error) {
	var logs []*types.ReceiptLog
	var kvs []*types.KeyValue
	var matched int64

	if matchorder.GetBalance() >= or.GetBalance() {
		matched = or.GetBalance()
	} else {
		matched = matchorder.GetBalance()
	}

	zlog.Info("try match", "activeId", or.OrderID, "passiveId", matchorder.OrderID, "activeAddr", or.Addr, "passiveAddr",
		matchorder.Addr, "amount", matched, "price", payload.Price)

	cfg := a.api.GetConfig()
	var receipt *types.Receipt
	var err error
	if payload.Op == zt.OpSell {
		//Transfer of frozen assets
		//如果发起交易是出售资产
		//TODO:关于数量的处理，需要统一考虑 by Hezhenghun on 4.4 2023
		amount := calcActualCost(matchorder.GetSpotTradeInfo().Op, matched, matchorder.GetSpotTradeInfo().Price, cfg.GetCoinPrecision())
		if matchorder.Addr != a.fromaddr {
			//如果对手方和发起方为不同的地址，则将冻结的资产（即划入交易账户的资产）转账到出售方（即通过基础货币进行支付）支付Ｕ
			transferPara := &zt.ZkTransfer{
				TokenId:       uint64(rightTokenID),
				Amount:        big.NewInt(amount).String(),
				FromAccountId: matchorder.GetSpotTradeInfo().FromAccountID,
				ToAccountId:   payload.FromAccountID,
			}
			receipt, err = a.l2TransferProc(transferPara, zt.TySpotTradeTakerTransfer, 18, zt.FromFrozen)
			//receipt, err = rightAccountDB.ExecTransferFrozen(matchorder.Addr, a.fromaddr, a.execaddr, amount)
		} else {
			receipt, err = rightAccountDB.ExecActive(a.fromaddr, a.execaddr, amount)
		}
		if err != nil {
			zlog.Error("matchModel.ExecTransferFrozen", "from", matchorder.Addr, "to", a.fromaddr, "amount", amount, "err", err)
			return nil, nil, err
		}
		logs = append(logs, receipt.Logs...)
		kvs = append(kvs, receipt.KV...)

		//Charge fee
		activeFee := calcMtfFee(amount, taker) //Transaction fee of the active party
		if activeFee != 0 {
			receipt, err = rightAccountDB.ExecTransfer(a.fromaddr, feeAddr, a.execaddr, activeFee)
			if err != nil {
				zlog.Error("matchModel.ExecTransfer sell", "from", a.fromaddr, "to", feeAddr,
					"amount", amount, "rate", taker, "activeFee", activeFee, "err", err.Error())
				return nil, nil, err
			}
			or.DigestedFee += activeFee
			logs = append(logs, receipt.Logs...)
			kvs = append(kvs, receipt.KV...)
		}

		//The settlement of the corresponding assets for the transaction to be concluded
		amount = calcActualCost(payload.Op, matched, matchorder.GetSpotTradeInfo().Price, cfg.GetCoinPrecision())
		if a.fromaddr != matchorder.Addr {
			receipt, err = leftAccountDB.ExecTransfer(a.fromaddr, matchorder.Addr, a.execaddr, amount)
			if err != nil {
				zlog.Error("matchModel.ExecTransfer", "from", a.fromaddr, "to", matchorder.Addr, "amount", amount, "err", err.Error())
				return nil, nil, err
			}
			logs = append(logs, receipt.Logs...)
			kvs = append(kvs, receipt.KV...)
		}

		//Charge fee
		passiveFee := calcMtfFee(amount, matchorder.GetRate()) //Passive transaction fees
		if passiveFee != 0 {
			receipt, err = leftAccountDB.ExecTransfer(matchorder.Addr, feeAddr, a.execaddr, passiveFee)
			if err != nil {
				zlog.Error("matchModel.ExecTransfer sell", "from", matchorder.Addr, "to", feeAddr,
					"amount", amount, "rate", matchorder.GetRate(), "passiveFee", passiveFee, "err", err.Error())
				return nil, nil, err
			}
			matchorder.DigestedFee += passiveFee
			logs = append(logs, receipt.Logs...)
			kvs = append(kvs, receipt.KV...)
		}

		or.AVGPrice = caclAVGPrice(or, matchorder.GetSpotTradeInfo().Price, matched)
		//Calculate the average transaction price
		matchorder.AVGPrice = caclAVGPrice(matchorder, matchorder.GetSpotTradeInfo().Price, matched)
	}
	if payload.Op == et.OpBuy {
		amount := calcActualCost(matchorder.GetSpotTradeInfo().Op, matched, matchorder.GetSpotTradeInfo().Price, cfg.GetCoinPrecision())
		if a.fromaddr != matchorder.Addr {
			receipt, err = leftAccountDB.ExecTransferFrozen(matchorder.Addr, a.fromaddr, a.execaddr, amount)
		} else {
			receipt, err = leftAccountDB.ExecActive(a.fromaddr, a.execaddr, amount)
		}
		if err != nil {
			zlog.Error("matchModel.ExecTransferFrozen2", "from", matchorder.Addr, "to", a.fromaddr, "amount", amount, "err", err.Error())
			return nil, nil, err
		}
		logs = append(logs, receipt.Logs...)
		kvs = append(kvs, receipt.KV...)

		activeFee := calcMtfFee(amount, taker)
		if activeFee != 0 {
			receipt, err = leftAccountDB.ExecTransfer(a.fromaddr, feeAddr, a.execaddr, activeFee)
			if err != nil {
				zlog.Error("matchModel.ExecTransfer buy", "from", a.fromaddr, "to", feeAddr,
					"amount", amount, "rate", taker, "activeFee", activeFee, "err", err.Error())
				return nil, nil, err
			}
			or.DigestedFee += activeFee
			logs = append(logs, receipt.Logs...)
			kvs = append(kvs, receipt.KV...)
		}

		amount = calcActualCost(payload.Op, matched, matchorder.GetSpotTradeInfo().Price, cfg.GetCoinPrecision())
		if a.fromaddr != matchorder.Addr {
			receipt, err = rightAccountDB.ExecTransfer(a.fromaddr, matchorder.Addr, a.execaddr, amount)
			if err != nil {
				zlog.Error("matchModel.ExecTransfer2", "from", a.fromaddr, "to", matchorder.Addr, "amount", amount, "err", err.Error())
				return nil, nil, err
			}
			logs = append(logs, receipt.Logs...)
			kvs = append(kvs, receipt.KV...)
		}

		passiveFee := calcMtfFee(amount, matchorder.GetRate())
		if passiveFee != 0 {
			receipt, err = rightAccountDB.ExecTransfer(matchorder.Addr, feeAddr, a.execaddr, passiveFee)
			if err != nil {
				zlog.Error("matchModel.ExecTransfer buy", "from", matchorder.Addr, "to", feeAddr,
					"amount", amount, "rate", matchorder.GetRate(), "passiveFee", passiveFee, "err", err.Error())
				return nil, nil, err
			}
			matchorder.DigestedFee += passiveFee
			logs = append(logs, receipt.Logs...)
			kvs = append(kvs, receipt.KV...)
		}

		or.AVGPrice = caclAVGPrice(or, matchorder.GetSpotTradeInfo().Price, matched)
		matchorder.AVGPrice = caclAVGPrice(matchorder, matchorder.GetSpotTradeInfo().Price, matched)
	}

	matchorder.UpdateTime = a.blocktime

	if matched == matchorder.GetBalance() {
		matchorder.Status = et.Completed
	} else {
		matchorder.Status = et.Ordered
	}

	if matched == or.GetBalance() {
		or.Status = et.Completed
	} else {
		or.Status = et.Ordered
	}

	if matched == or.GetBalance() {
		matchorder.Balance -= matched
		matchorder.Executed = matched
		kvs = append(kvs, a.GetKVSet(matchorder)...)

		or.Executed += matched
		or.Balance = 0
		kvs = append(kvs, a.GetKVSet(or)...) //or complete
	} else {
		or.Balance -= matched
		or.Executed += matched

		matchorder.Executed = matched
		matchorder.Balance = 0
		kvs = append(kvs, a.GetKVSet(matchorder)...) //matchorder complete
	}

	re.Order = or
	re.MatchOrders = append(re.MatchOrders, matchorder)
	return logs, kvs, nil
}

// Query the status database according to the order number
// Localdb deletion sequence: delete the cache in real time first, and modify the DB uniformly during block generation.
// The cache data will be deleted. However, if the cache query fails, the deleted data can still be queried in the DB
func findOrderByOrderID(statedb dbm.KV, orderID int64) (*et.Order, error) {
	data, err := statedb.Get(calcOrderKey(orderID))
	if err != nil {
		zlog.Error("findOrderByOrderID.Get", "orderID", orderID, "err", err.Error())
		return nil, err
	}
	var order et.Order
	err = types.Decode(data, &order)
	if err != nil {
		zlog.Error("findOrderByOrderID.Decode", "orderID", orderID, "err", err.Error())
		return nil, err
	}
	order.Executed = order.GetLimitOrder().Amount - order.Balance
	return &order, nil
}

func findOrderIDListByPrice(localdb dbm.KV, leftTokenID, rightTokenID uint32, price int64, op, direction int32, primaryKey string) (*et.OrderList, error) {
	table := NewMarketOrderTable(localdb)
	prefix := []byte(fmt.Sprintf("%d:%d:%d:%016d", leftTokenID, rightTokenID, op, price))

	var rows []*tab.Row
	var err error
	if primaryKey == "" { // First query, the default display of the latest transaction record
		rows, err = table.ListIndex("market_order", prefix, nil, et.Count, direction)
	} else {
		rows, err = table.ListIndex("market_order", prefix, []byte(primaryKey), et.Count, direction)
	}
	if err != nil {
		if primaryKey == "" {
			zlog.Error("findOrderIDListByPrice.", "leftTokenID", leftTokenID, "rightTokenID", rightTokenID, "price", price, "err", err.Error())
		}
		return nil, err
	}
	var orderList et.OrderList
	for _, row := range rows {
		order := row.Data.(*et.Order)
		// The replacement has been done
		order.Executed = order.GetLimitOrder().Amount - order.Balance
		orderList.List = append(orderList.List, order)
	}
	// Set the primary key index
	if len(rows) == int(et.Count) {
		orderList.PrimaryKey = string(rows[len(rows)-1].Primary)
	}
	return &orderList, nil
}

// queryMarketDepth 这里primaryKey当作主键索引来用，
// The first query does not need to fill in the value, pay according to the price from high to low, selling orders according to the price from low to high query
func queryMarketDepth(localdb dbm.KV, leftTokenID, rightTokenID uint32, op int32, primaryKey string, count int32) (*et.MarketDepthList, error) {
	table := NewMarketDepthTable(localdb)
	prefix := []byte(fmt.Sprintf("%d:%d:%d", leftTokenID, rightTokenID, op))
	if count == 0 {
		count = zt.Count
	}
	var rows []*tab.Row
	var err error
	if primaryKey == "" { // First query, the default display of the latest transaction record
		rows, err = table.ListIndex("price", prefix, nil, count, op)
	} else {
		rows, err = table.ListIndex("price", prefix, []byte(primaryKey), count, op)
	}
	if err != nil {
		return nil, err
	}

	var list et.MarketDepthList
	for _, row := range rows {
		list.List = append(list.List, row.Data.(*et.MarketDepth))
	}
	if len(rows) == int(count) {
		list.PrimaryKey = string(rows[len(rows)-1].Primary)
	}
	return &list, nil
}

func ParseStrings(cfg *types.Chain33Config, tradeKey string, height int64) (ret []string, err error) {
	val, err := cfg.MG(et.MverPrefix+"."+tradeKey, height)
	if err != nil {
		return nil, err
	}

	datas, ok := val.([]interface{})
	if !ok {
		zlog.Error("invalid val", "val", val, "key", tradeKey)
		return nil, et.ErrCfgFmt
	}

	for _, v := range datas {
		one, ok := v.(string)
		if !ok {
			zlog.Error("invalid one", "one", one, "key", tradeKey)
			return nil, et.ErrCfgFmt
		}
		ret = append(ret, one)
	}
	return
}

func parseConfig(cfg *types.Chain33Config, height int64) (*et.Econfig, error) {
	banks, err := ParseStrings(cfg, "banks", height)
	if err != nil || len(banks) == 0 {
		return nil, err
	}

	robots, err := ParseStrings(cfg, "robots", height)
	if err != nil || len(banks) == 0 {
		return nil, err
	}
	robotMap := make(map[string]bool)
	for _, v := range robots {
		robotMap[v] = true
	}

	coins, err := ParseCoins(cfg, "coins", height)
	if err != nil {
		return nil, err
	}
	exchanges, err := ParseSymbols(cfg, "exchanges", height)
	if err != nil {
		return nil, err
	}
	return &et.Econfig{
		Banks:     banks,
		RobotMap:  robotMap,
		Coins:     coins,
		Exchanges: exchanges,
	}, nil
}

// calcActualCost Calculate actual cost
func calcActualCost(op int32, amount int64, price, coinPrecision int64) int64 {
	if op == et.OpBuy {
		return safeMul(amount, price, coinPrecision)
	}
	return amount
}

// checkTradeAmount 最小交易 1coin
func checkTradeAmount(amount, coinPrecision int64) bool {
	if amount < 1 || amount >= types.MaxCoin*coinPrecision {
		return false
	}
	return true
}

// checkPrice price  1<=price<=1e16
func checkPrice(price int64) bool {
	if price > 1e16 || price < 1 {
		return false
	}
	return true
}

// checkOp ...
func checkOp(op int32) bool {
	if op == et.OpBuy || op == et.OpSell {
		return true
	}
	return false
}

// safeMul Safe multiplication of large numbers, prevent overflow
func safeMul(x, y, coinPrecision int64) int64 {
	res := big.NewInt(0).Mul(big.NewInt(x), big.NewInt(y))
	res = big.NewInt(0).Div(res, big.NewInt(coinPrecision))
	return res.Int64()
}

// Calculate the average transaction price
func caclAVGPrice(order *et.Order, price int64, amount int64) int64 {
	x := big.NewInt(0).Mul(big.NewInt(order.AVGPrice), big.NewInt(order.GetLimitOrder().Amount-order.GetBalance()))
	y := big.NewInt(0).Mul(big.NewInt(price), big.NewInt(amount))
	total := big.NewInt(0).Add(x, y)
	div := big.NewInt(0).Add(big.NewInt(order.GetLimitOrder().Amount-order.GetBalance()), big.NewInt(amount))
	avg := big.NewInt(0).Div(total, div)
	return avg.Int64()
}

// 计Calculation fee
func calcMtfFee(cost int64, rate int32) int64 {
	fee := big.NewInt(0).Mul(big.NewInt(cost), big.NewInt(int64(rate)))
	fee = big.NewInt(0).Div(fee, big.NewInt(types.DefaultCoinPrecision))
	return fee.Int64()
}
