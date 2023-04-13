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
	"math"
	"math/big"
	"strconv"
)

var coinPrecisionDecimal = 8
var coinPrecision = types.DefaultCoinPrecision
var initCoinPrecision = false
var tradePair2FeeMap map[string]*zt.ZkSetSpotFee
var freeFeeMap map[string]bool

func init() {
	tradePair2FeeMap = make(map[string]*zt.ZkSetSpotFee)
	freeFeeMap = make(map[string]bool)
}

func (a *Action) ZkSetSpotFee(payload *zt.ZkSetSpotFee) (*types.Receipt, error) {
	//只有管理员才能设置交易费
	cfg := a.api.GetConfig()
	if !isSuperManager(cfg, a.fromaddr) && !isVerifier(a.statedb, a.fromaddr) {
		return nil, errors.Wrapf(types.ErrNotAllow, "from addr is not manager")
	}

	leftTokenInfo, err := GetTokenByTokenId(a.statedb, strconv.Itoa(int(payload.LeftToken.TokenId)))
	if nil != err {
		zlog.Error("executor.ZkSetSpotFee.GetTokenByTokenId", "leftTokenID", payload.LeftToken.TokenId, "err", err)
		return nil, err
	}
	if leftTokenInfo.Symbol != payload.LeftToken.Symbol {
		zlog.Error("executor.ZkSetSpotFee", "ErrWrongTokenSymbol:%s != %s", leftTokenInfo.Symbol, payload.LeftToken.Symbol)
		return nil, errors.Wrapf(errors.New("ErrWrongTokenSymbol"), "%s != %s", leftTokenInfo.Symbol, payload.LeftToken.Symbol)
	}

	rightTokenInfo, err := GetTokenByTokenId(a.statedb, strconv.Itoa(int(payload.RightToken.TokenId)))
	if nil != err {
		zlog.Error("executor matchLimitOrder.GetTokenByTokenId", "rightTokenID", payload.RightToken.TokenId, "err", err)
		return nil, err
	}
	if rightTokenInfo.Symbol != payload.RightToken.Symbol {
		zlog.Error("executor.ZkSetSpotFee", "ErrWrongTokenSymbol:%s != %s", rightTokenInfo.Symbol, payload.RightToken.Symbol)
		return nil, errors.Wrapf(errors.New("ErrWrongTokenSymbol"), "%s != %s", rightTokenInfo.Symbol, payload.RightToken.Symbol)
	}

	key := getSpotTradePairFeeKeyPrefix(leftTokenInfo.Symbol, rightTokenInfo.Symbol)
	data := types.Encode(payload)

	kv := &types.KeyValue{
		Key:   key,
		Value: data,
	}

	old, _ := getSpotTradeFeeFromDB(a.statedb, leftTokenInfo.Symbol, rightTokenInfo.Symbol)

	setFeelog := &zt.ReceiptSpotTradeFee{
		LeftToken:         leftTokenInfo.Symbol,
		RightToken:        rightTokenInfo.Symbol,
		TakerRatePrevious: old.TakerRate,
		MakerRatePrevious: old.MakerRate,
		TakerRateAfter:    payload.TakerRate,
		MakerRateAfter:    payload.MakerRate,
	}

	receiptLog := &types.ReceiptLog{Ty: zt.TySetSpotTradeFeeLog, Log: types.Encode(setFeelog)}
	receipts := &types.Receipt{
		Ty:   types.ExecOk,
		KV:   []*types.KeyValue{kv},
		Logs: []*types.ReceiptLog{receiptLog}}

	//在此处更新交易费列表
	tradePair2FeeMap[string(key)] = payload
	return receipts, nil
}

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
	if leftTokenID == rightTokenID {
		return nil, errors.Wrapf(types.ErrNotAllow, "asset with the same token ID =%d", leftTokenID)
	}
	if !checkTradeAmount(tradeInfo.GetAmount(), a.api.GetConfig().GetCoinPrecision()) {
		return nil, zt.ErrAssetAmount
	}
	if !checkPrice(tradeInfo.GetPrice()) {
		return nil, zt.ErrAssetPrice
	}
	if !checkOp(tradeInfo.GetOp()) {
		return nil, zt.ErrAssetOp
	}

	return a.matchLimitOrder(tradeInfo)
}

func (a *Action) ZkRevokeTrade(payload *zt.ZkRevokeTrade) (*types.Receipt, error) {
	return nil, nil
}

func (a *Action) ZkTransfer2Ex(payload *zt.ZkTransfer2Ex, actionTy int32) (*types.Receipt, error) {
	if actionTy != zt.TyTransfer2Trade && actionTy != zt.TyTransferFromTrade {
		return nil, errors.New("Neigher TyTransfer2Trade nor TyTransferFromTrade")
	}

	tokenID := payload.TokenID
	if actionTy == zt.TyTransferFromTrade {
		tokenID = tokenID | zt.SpotTradeTokenFlag
	}
	para := &transfer2TradePara{
		FromAccountId: payload.FromAccountId,
		FromTokenId:   tokenID,
		Amount:        payload.Amount,
	}

	return a.transfer2Trade(para)
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
		taker        int64
		maker        int64
		leftTokenID  uint32
		rightTokenID uint32
	)

	leftTokenID = tradeInfo.LeftAssetTokenID
	rightTokenID = tradeInfo.RightAssetTokenID

	leftTokenInfo, err := GetTokenByTokenId(a.statedb, strconv.Itoa(int(leftTokenID)))
	if nil != err {
		zlog.Error("executor matchLimitOrder.GetTokenByTokenId", "leftTokenID", leftTokenID, "err", err)
		return nil, err
	}
	rightTokenInfo, err := GetTokenByTokenId(a.statedb, strconv.Itoa(int(rightTokenID)))
	if nil != err {
		zlog.Error("executor matchLimitOrder.GetTokenByTokenId", "rightTokenID", rightTokenID, "err", err)
		return nil, err
	}

	cfg := a.api.GetConfig()
	if !initCoinPrecision {
		//初始化
		coinPrecision = cfg.GetCoinPrecision()
		coinPrecisionDecimal = int(math.Log(float64(cfg.GetCoinPrecision())))
		initCoinPrecision = true
	}

	//如果不是免收交易费，则设置相应的交易费
	if !freeFeeMap[a.fromaddr] {
		keyStr := string(getSpotTradePairFeeKeyPrefix(leftTokenInfo.Symbol, rightTokenInfo.Symbol))
		tradePair2Fee, ok := tradePair2FeeMap[keyStr]
		if !ok {
			current, _ := getSpotTradeFeeFromDB(a.statedb, leftTokenInfo.Symbol, rightTokenInfo.Symbol)
			tradePair2FeeMap[keyStr] = current
			tradePair2Fee = current
		}
		taker = tradePair2Fee.TakerRate
		maker = tradePair2Fee.MakerRate
	}

	or := &zt.Order{
		OrderID:    a.GetIndex(),
		Value:      &zt.Order_SpotTradeInfo{tradeInfo},
		Ty:         et.TyLimitOrderAction,
		Executed:   0,
		AVGPrice:   0,
		Balance:    tradeInfo.GetAmount(),
		Status:     et.Ordered,
		AccountID:  tradeInfo.FromAccountID,
		UpdateTime: a.blocktime,
		Index:      a.GetIndex(),
		Rate:       maker,
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
		marketDepthList, _ := queryMarketDepthList(a.localDB, leftTokenID, rightTokenID, opSwap(tradeInfo.Op), priceKey, zt.Count)
		if marketDepthList == nil || len(marketDepthList.List) == 0 {
			break
		}
		for _, marketDepth := range marketDepthList.List {
			zlog.Info("LimitOrder debug find depth", "height", a.height, "amount", marketDepth.Amount, "price", marketDepth.Price, "order-price", tradeInfo.GetPrice(), "op", opSwap(tradeInfo.Op), "index", a.GetIndex())
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
				orderList, err := findOrderIDListByPrice(a.localDB, leftTokenID, rightTokenID, marketDepth.Price, opSwap(tradeInfo.Op), et.ListASC, orderKey)
				if orderList != nil && !hasOrder {
					hasOrder = true
				}
				if err != nil {
					if err == types.ErrNotFound {
						break
					}
					zlog.Error("findOrderIDListByPrice error", "height", a.height, "token ID", leftTokenID, "price", marketDepth.Price, "op", opSwap(tradeInfo.Op), "error", err)
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
					special := &zt.ZkSpotTradeWitnessInfo{
						LeftTokenID:  tradeInfo.LeftAssetTokenID,
						RightTokenID: tradeInfo.RightAssetTokenID,
						//交易购买信息
						LeftTokenFrom:  &zt.ZkSpotTradeDealInfo{},
						RightTokenFrom: &zt.ZkSpotTradeDealInfo{},
						BlockInfo: &zt.OpBlockInfo{
							Height:  a.height,
							TxIndex: int32(a.index),
						},
					}
					log, kv, err := a.matchModel(leftTokenInfo, rightTokenInfo, tradeInfo, order, or, re, taker, special) // payload, or redundant
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
				var matchorder zt.Order
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
	//根据未成交数量锁定相应的资产
	if tradeInfo.Op == zt.OpBuy {
		//如果发起方是购买，则冻结usdt资产
		amount, err := calcActualCost(et.OpBuy, or.Balance, tradeInfo.Price, coinPrecisionDecimal, int(rightTokenInfo.Decimal))
		if nil != err {
			return nil, err
		}

		receipt, err := freezeOrUnfreezeToken(tradeInfo.FromAccountID, uint64(rightTokenID|zt.SpotTradeTokenFlag), amount, zt.Freeze, a.statedb)
		if err != nil {
			zlog.Error("matchLimitOrder.freezeOrUnfreezeToken", "FromAccountID", tradeInfo.FromAccountID, "amount", amount, "err", err)
			return nil, err
		}

		logs = append(logs, receipt.Logs...)
		kvs = append(kvs, receipt.KV...)
	}
	if tradeInfo.Op == et.OpSell {
		//如果发起方是出售，则冻结eth资产
		amount, err := calcActualCost(et.OpSell, or.Balance, tradeInfo.Price, coinPrecisionDecimal, int(leftTokenInfo.Decimal))
		if nil != err {
			return nil, err
		}
		receipt, err := freezeOrUnfreezeToken(tradeInfo.FromAccountID, uint64(leftTokenID|zt.SpotTradeTokenFlag), amount, zt.Freeze, a.statedb)
		if err != nil {
			zlog.Error("matchLimitOrder.freezeOrUnfreezeToken", "FromAccountID", tradeInfo.FromAccountID, "amount", amount, "err", err)
			return nil, err
		}
		logs = append(logs, receipt.Logs...)
		kvs = append(kvs, receipt.KV...)
	}
	kvs = append(kvs, getKVSet(or)...)
	re.Order = or
	receiptlog := &types.ReceiptLog{Ty: et.TyLimitOrderLog, Log: types.Encode(re)}
	logs = append(logs, receiptlog)
	receipts := &types.Receipt{Ty: types.ExecOk, KV: kvs, Logs: logs}

	return receipts, nil
}

func freezeOrUnfreezeToken(from, tokenID uint64, amount string, option int32, statedb dbm.KV) (*types.Receipt, error) {
	if option != zt.UnFreeze || option != zt.Freeze {
		return nil, errors.New("Only opertion freeze and unfreeze is supported")
	}

	leaf, err := GetLeafByAccountId(statedb, from)
	if nil != err {
		return nil, err
	}

	updateKVs, log, _, err := applyL2AccountUpdate(leaf.AccountId, tokenID, amount, option, statedb, leaf, false, zt.FromActive)
	if nil != err {
		return nil, errors.Wrapf(err, "applyL2AccountUpdate")
	}
	receipt := &types.Receipt{Ty: types.ExecOk, KV: updateKVs, Logs: []*types.ReceiptLog{log}}
	return receipt, nil
}

func (a *Action) matchModel(
	leftTokenInfo, rightTokenInfo *zt.ZkTokenSymbol,
	payload *zt.SpotTradeInfo,
	matchorder *zt.Order,
	or *zt.Order,
	re *zt.ReceiptExchange,
	taker int64,
	special *zt.ZkSpotTradeWitnessInfo,
) ([]*types.ReceiptLog, []*types.KeyValue, error) {

	var logs []*types.ReceiptLog
	var kvs []*types.KeyValue
	var matched int64

	leftTokenID, _ := strconv.Atoi(leftTokenInfo.Id)
	rightTokenID, _ := strconv.Atoi(rightTokenInfo.Id)

	if matchorder.GetBalance() > or.GetBalance() {
		matched = or.GetBalance()
	} else {
		matched = matchorder.GetBalance()
	}

	zlog.Info("try match", "activeId", or.OrderID, "passiveId", matchorder.OrderID, "activeAddr", or.AccountID, "passiveAddr",
		matchorder.AccountID, "matched", matched, "price", payload.Price)

	var receipts *types.Receipt
	var err error
	var ops []*zt.ZkOperation
	//eth-usdt交易对
	//case1:如果出售方为发起方时：
	//case1-op1:将usdt（rightTokenID）发送给发起方（出售方）
	//          如果对手方就是自己，则直接激活即可
	//case1-op2:将eth（leftTokenID）发送给订单方（购买方）
	if payload.Op == zt.OpSell {
		//Transfer of frozen assets
		//如果发起交易是出售资产
		//TODO:关于数量的处理，需要统一考虑 by Hezhenghun on 4.4 2023
		amountInStr, err := calcActualCost(matchorder.GetSpotTradeInfo().Op, matched, matchorder.GetSpotTradeInfo().Price, coinPrecisionDecimal, int(rightTokenInfo.Decimal))
		if err != nil {
			zlog.Error("matchModel", "methodName", "calcActualCost", "from", matchorder.AccountID, "to", a.fromaddr, "amount", amountInStr, "err", err)
			return nil, nil, err
		}

		special.Price = matchorder.GetSpotTradeInfo().Price
		special.RightTokenFrom.AccountID = matchorder.AccountID
		special.RightTokenFrom.Amount = amountInStr
		special.RightTokenFrom.TakerMaker = zt.TySpotTradeTakerTransfer
		if matchorder.AccountID != payload.FromAccountID {
			//如果对手方和发起方为不同的地址，则将冻结的资产（即划入交易账户的资产）转账到出售方（即通过基础货币进行支付）支付Ｕ
			transferPara := &zt.ZkTransfer{
				TokenId:       uint64(rightTokenID),
				Amount:        amountInStr,
				FromAccountId: matchorder.AccountID,
				ToAccountId:   payload.FromAccountID,
			}
			receipts, err = a.l2TransferProc(transferPara, zt.TySpotTrade, 18, zt.FromFrozen)
			if err != nil {
				zlog.Error("matchModel", "methodName", "l2TransferProc", "from", matchorder.AccountID, "to", a.fromaddr, "amount", amountInStr, "err", err)
				return nil, nil, err
			}

		} else {
			//如果匹配方是自己，则直接解冻相应的资产
			receipts, err = freezeOrUnfreezeToken(payload.FromAccountID, uint64(rightTokenID|zt.SpotTradeTokenFlag), amountInStr, zt.UnFreeze, a.statedb)
			if err != nil {
				zlog.Error("matchModel", "methodName", "freezeOrUnfreezeToken", "from", matchorder.AccountID, "to", a.fromaddr, "amount", amountInStr, "err", err)
				return nil, nil, err
			}

			//Charge fee
			activeFeeInstr, activeFee, err := calcMtfFee(amountInStr, taker, coinPrecision, coinPrecisionDecimal, int(rightTokenInfo.Decimal)) //Transaction fee of the active party
			if err == nil {
				feeReceipt, feeQueue, err := a.MakeFeeLog(activeFeeInstr, uint64(rightTokenID|zt.SpotTradeTokenFlag))
				if err != nil {
					return nil, nil, errors.Wrapf(err, "MakeFeeLog")
				}
				or.DigestedFee += activeFee

				ops = append(ops, feeQueue)
				receipts = mergeReceipt(receipts, feeReceipt)
			}
		}

		//The settlement of the corresponding assets for the transaction to be concluded
		amountInStr, err = calcActualCost(payload.Op, matched, matchorder.GetSpotTradeInfo().Price, coinPrecisionDecimal, int(leftTokenInfo.Decimal))
		if nil != err {
			return nil, nil, err
		}
		special.LeftTokenFrom.AccountID = payload.FromAccountID
		special.LeftTokenFrom.Amount = amountInStr
		special.LeftTokenFrom.TakerMaker = zt.TySpotTradeMakerTransfer
		if payload.FromAccountID != matchorder.AccountID {
			//支付标的资产给购买方
			transferPara := &zt.ZkTransfer{
				TokenId:       uint64(leftTokenID),
				Amount:        amountInStr,
				FromAccountId: payload.FromAccountID,
				ToAccountId:   matchorder.GetSpotTradeInfo().FromAccountID,
			}
			//包括maker支付交易费
			receipts, err = a.l2TransferProc(transferPara, zt.TySpotTrade, 18, zt.FromActive)
			if err != nil {
				zlog.Error("matchModel.l2TransferProc", "transferPara", transferPara, "err", err.Error())
				return nil, nil, err
			}
		}

		or.AVGPrice = caclAVGPrice(or, matchorder.GetSpotTradeInfo().Price, matched)
		//Calculate the average transaction price
		matchorder.AVGPrice = caclAVGPrice(matchorder, matchorder.GetSpotTradeInfo().Price, matched)
	} else {
		//case2:如果购买方为发起方时：
		//case2-op1:将eth（leftTokenID）发送给发起方（购买方）
		//          如果对手方就是自己，则直接激活即可
		//case2-op2:将usdt（rightTokenID）发送给订单方（出售方）
		amountInStr, err := calcActualCost(matchorder.GetSpotTradeInfo().Op, matched, matchorder.GetSpotTradeInfo().Price, coinPrecisionDecimal, int(leftTokenInfo.Decimal))
		if nil != err {
			return nil, nil, err
		}
		special.Price = matchorder.GetSpotTradeInfo().Price
		special.LeftTokenFrom.AccountID = matchorder.AccountID
		special.LeftTokenFrom.Amount = amountInStr
		special.LeftTokenFrom.TakerMaker = zt.TySpotTradeMakerTransfer
		if payload.FromAccountID != matchorder.AccountID {
			//将出售方的冻结标的资产转移至买方，如ETH-USDT交易对中的ETH资产
			transferPara := &zt.ZkTransfer{
				TokenId:       uint64(leftTokenID),
				Amount:        amountInStr,
				FromAccountId: matchorder.GetSpotTradeInfo().FromAccountID,
				ToAccountId:   payload.FromAccountID,
			}
			receipts, err = a.l2TransferProc(transferPara, zt.TySpotTrade, 18, zt.FromFrozen)
			if err != nil {
				zlog.Error("matchModel.l2TransferProc", "from", matchorder.AccountID, "to", a.fromaddr, "amount", amountInStr, "err", err)
				return nil, nil, err
			}
		} else {
			//如果匹配方是自己，则直接解冻相应的资产
			receipts, err = freezeOrUnfreezeToken(payload.FromAccountID, uint64(leftTokenID|zt.SpotTradeTokenFlag), amountInStr, zt.UnFreeze, a.statedb)
			if err != nil {
				zlog.Error("matchModel.freezeOrUnfreezeToken", "from", matchorder.AccountID, "to", a.fromaddr, "amount", amountInStr, "err", err)
				return nil, nil, err
			}

			//Charge fee
			activeFeeInstr, activeFee, err := calcMtfFee(amountInStr, taker, coinPrecision, coinPrecisionDecimal, int(leftTokenInfo.Decimal))
			if err == nil {
				feeReceipt, feeQueue, err := a.MakeFeeLog(activeFeeInstr, uint64(leftTokenID|zt.SpotTradeTokenFlag))
				if err != nil {
					return nil, nil, errors.Wrapf(err, "MakeFeeLog")
				}
				or.DigestedFee += activeFee

				ops = append(ops, feeQueue)
				receipts = mergeReceipt(receipts, feeReceipt)
			}
		}
		if err != nil {
			zlog.Error("matchModel.ExecTransferFrozen2", "from", matchorder.AccountID, "to", a.fromaddr, "amount", amountInStr, "err", err.Error())
			return nil, nil, err
		}

		amountInStr, err = calcActualCost(payload.Op, matched, matchorder.GetSpotTradeInfo().Price, coinPrecisionDecimal, int(rightTokenInfo.Decimal))
		if nil != err {
			return nil, nil, err
		}
		special.RightTokenFrom.AccountID = payload.FromAccountID
		special.RightTokenFrom.Amount = amountInStr
		special.RightTokenFrom.TakerMaker = zt.TySpotTradeTakerTransfer
		if payload.FromAccountID != matchorder.AccountID {
			//购买方支付基础计价货币USDT
			transferPara := &zt.ZkTransfer{
				TokenId:       uint64(rightTokenID),
				Amount:        amountInStr,
				FromAccountId: payload.FromAccountID,
				ToAccountId:   matchorder.GetSpotTradeInfo().FromAccountID,
			}
			receipts, err = a.l2TransferProc(transferPara, zt.TySpotTrade, 18, zt.FromActive)
			if err != nil {
				zlog.Error("matchModel.l2TransferProc", "from", matchorder.AccountID, "to", a.fromaddr, "amount", amountInStr, "err", err)
				return nil, nil, err
			}
		}

		or.AVGPrice = caclAVGPrice(or, matchorder.GetSpotTradeInfo().Price, matched)
		matchorder.AVGPrice = caclAVGPrice(matchorder, matchorder.GetSpotTradeInfo().Price, matched)
	}
	ops = append(ops, &zt.ZkOperation{Ty: zt.TySpotTrade, Op: &zt.OperationSpecialInfo{Value: &zt.OperationSpecialInfo_SpotTrade{SpotTrade: special}}})

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

		matchorder.Executed = matched

		kvs = append(kvs, getKVSet(matchorder)...)

		or.Executed += matched
		or.Balance = 0
		kvs = append(kvs, getKVSet(or)...) //or complete
	} else {
		or.Balance -= matched
		or.Executed += matched

		matchorder.Executed = matched
		matchorder.Balance = 0
		kvs = append(kvs, getKVSet(matchorder)...) //matchorder complete
	}

	re.Order = or
	re.MatchOrders = append(re.MatchOrders, matchorder)

	r, _, err := setL2QueueData(a.statedb, ops)
	if err != nil {
		return nil, nil, err
	}
	receipts = mergeReceipt(receipts, r)

	return logs, kvs, nil
}

func getKVSet(order *zt.Order) (kvset []*types.KeyValue) {
	kvset = append(kvset, &types.KeyValue{Key: calcOrderKey(order.OrderID), Value: types.Encode(order)})
	return kvset
}

// Query the status database according to the order number
// Localdb deletion sequence: delete the cache in real time first, and modify the DB uniformly during block generation.
// The cache data will be deleted. However, if the cache query fails, the deleted data can still be queried in the DB
func findOrderByOrderID(statedb dbm.KV, orderID int64) (*zt.Order, error) {
	data, err := statedb.Get(calcOrderKey(orderID))
	if err != nil {
		zlog.Error("findOrderByOrderID.Get", "orderID", orderID, "err", err.Error())
		return nil, err
	}
	var order zt.Order
	err = types.Decode(data, &order)
	if err != nil {
		zlog.Error("findOrderByOrderID.Decode", "orderID", orderID, "err", err.Error())
		return nil, err
	}
	order.Executed = order.GetSpotTradeInfo().Amount - order.Balance
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

// queryMarketDepthList 这里primaryKey当作主键索引来用，
// The first query does not need to fill in the value, pay according to the price from high to low, selling orders according to the price from low to high query
func queryMarketDepthList(localdb dbm.KV, leftTokenID, rightTokenID uint32, op int32, primaryKey string, count int32) (*zt.MarketDepthList, error) {
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

	var list zt.MarketDepthList
	for _, row := range rows {
		list.List = append(list.List, row.Data.(*zt.MarketDepth))
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

func calcActualCost(op int32, amount, price int64, coinDecimal int, tokenDecimal int) (string, error) {
	amountBig := big.NewInt(amount)
	if op == et.OpBuy {
		// amount:0.1eth = 1e7
		// price:2000.xxu/eth = 2.000xxe11/eth
		// res: 1e7 * 2.000xxe11 = 2.000xxe18 u
		// res: 2.000xxe18 u / 1e16 = 200.xx u
		//根据u的deciml进行扩展200.xx u　200.xx 1edecimal
		priceBig := big.NewInt(price)
		res := big.NewInt(0).Mul(amountBig, priceBig)

		// 8*2 ---> 8
		return TransferDecimalAmount(res.String(), coinDecimal*2, tokenDecimal)
	}

	return TransferDecimalAmount(amountBig.String(), coinDecimal, tokenDecimal)
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
func caclAVGPrice(order *zt.Order, price, amount int64) int64 {
	amountBig := big.NewInt(amount)
	priceBig := big.NewInt(price)
	avgPrice := big.NewInt(order.AVGPrice)
	//订单总量
	total := big.NewInt(order.GetSpotTradeInfo().Amount)
	//订单未成交余额
	balace := big.NewInt(order.GetBalance())
	//已经成交量
	dealAmount := total.Sub(total, balace)

	//根据平均价和已经成交量　计算成交总价
	deal := avgPrice.Mul(avgPrice, dealAmount)
	current := amountBig.Mul(priceBig, amountBig)

	deal = deal.Add(deal, current)
	div := amountBig.Add(amountBig, dealAmount)
	return deal.Div(deal, div).Int64()
}

// 计Calculation fee
func calcMtfFee(cost string, rate, coinPrecision int64, decimalFrom, decimalTo int) (string, int64, error) {
	amount, _ := new(big.Int).SetString(cost, 10)
	fee := big.NewInt(0).Mul(amount, big.NewInt(rate))
	fee = big.NewInt(0).Div(fee, big.NewInt(coinPrecision))
	feeStr, err := TransferDecimalAmount(fee.String(), decimalFrom, decimalTo)
	return feeStr, fee.Int64(), err
}

func opSwap(op int32) int32 {
	if op == et.OpBuy {
		return et.OpSell
	}
	return et.OpBuy
}

// 如果交易费未设置,则直接返回空值
func getSpotTradeFeeFromDB(db dbm.KV, left, right string) (*zt.ZkSetSpotFee, error) {
	var fee zt.ZkSetSpotFee
	key := getSpotTradePairFeeKeyPrefix(left, right)
	v, err := db.Get(key)
	if err != nil {
		return &fee, err
	}
	err = types.Decode(v, &fee)
	if nil != err {
		return &fee, err
	}

	return &fee, nil
}
