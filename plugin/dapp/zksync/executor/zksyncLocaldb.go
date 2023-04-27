package executor

import (
	"fmt"
	"github.com/33cn/chain33/common/db/table"
	"github.com/33cn/chain33/types"
	zty "github.com/33cn/plugin/plugin/dapp/zksync/types"
	"math/big"
)

func (z *zksync) execAutoLocalZksync(tx *types.Transaction, receiptData *types.ReceiptData, index int) (*types.LocalDBSet, error) {
	if receiptData.Ty != types.ExecOk {
		return nil, types.ErrInvalidParam
	}
	set, err := z.execLocalZksync(tx, receiptData, index)
	if err != nil {
		return set, err
	}
	dbSet := &types.LocalDBSet{}
	dbSet.KV = z.AddRollbackKV(tx, tx.Execer, set.KV)
	return dbSet, nil
}

func (z *zksync) execLocalZksync(tx *types.Transaction, receiptData *types.ReceiptData, index int) (*types.LocalDBSet, error) {
	infoTable := NewAccountTreeTable(z.GetLocalDB())

	dbSet := &types.LocalDBSet{}
	for _, log := range receiptData.Logs {
		switch log.Ty {
		case zty.TyDepositLog:
			var receipt zty.AccountTokenBalanceReceipt
			err := types.Decode(log.GetLog(), &receipt)
			if err != nil {
				return nil, err
			}
			leaf := &zty.Leaf{
				AccountId:   receipt.AccountId,
				EthAddress:  receipt.EthAddress,
				Chain33Addr: receipt.Chain33Addr,
			}

			err = infoTable.Replace(leaf)
			if err != nil {
				return nil, err
			}
		}
	}
	kvs, err := infoTable.Save()
	if err != nil {
		return nil, err
	}
	dbSet.KV = append(dbSet.KV, kvs...)
	return dbSet, nil
}

func (z *zksync) execAutoDelLocal(tx *types.Transaction, receiptData *types.ReceiptData) (*types.LocalDBSet, error) {
	kvs, err := z.DelRollbackKV(tx, tx.Execer)
	if err != nil {
		return nil, err
	}
	dbSet := &types.LocalDBSet{}
	dbSet.KV = append(dbSet.KV, kvs...)
	return dbSet, nil
}

func (z *zksync) execCommitProofLocal(payload *zty.ZkCommitProof, tx *types.Transaction, receiptData *types.ReceiptData, index int) (*types.LocalDBSet, error) {
	if receiptData.Ty != types.ExecOk {
		return nil, types.ErrInvalidParam
	}

	proofTable := NewCommitProofTable(z.GetLocalDB())

	set := &types.LocalDBSet{}
	payload.CommitBlockHeight = z.GetHeight()
	err := proofTable.Replace(payload)
	if err != nil {
		return nil, err
	}

	kvs, err := proofTable.Save()
	if err != nil {
		return nil, err
	}
	set.KV = append(set.KV, kvs...)

	dbSet := &types.LocalDBSet{}
	dbSet.KV = z.AddRollbackKV(tx, tx.Execer, set.KV)
	return dbSet, nil
}

func (z *zksync) interExecLocal(tx *types.Transaction, receiptData *types.ReceiptData, index int) (*types.LocalDBSet, error) {
	dbSet := &types.LocalDBSet{}
	historyTable := NewHistoryOrderTable(z.GetLocalDB())
	marketTable := NewMarketDepthTable(z.GetLocalDB())
	orderTable := NewMarketOrderTable(z.GetLocalDB())
	if receiptData.Ty == types.ExecOk {
		for _, log := range receiptData.Logs {
			switch log.Ty {
			case zty.TySpotTradeOrderLog, zty.TyRevokeOrderLog, zty.TyTransfer2TradeLog:
				receipt := &zty.SpotTradeReceiptExchange{}
				if err := types.Decode(log.Log, receipt); err != nil {
					return nil, err
				}
				z.updateIndex(marketTable, orderTable, historyTable, receipt)
			}
		}
	}

	var kvs []*types.KeyValue
	kv, err := marketTable.Save()
	if err != nil {
		zlog.Error("updateIndex", "marketTable.Save", err.Error())
		return nil, nil
	}
	kvs = append(kvs, kv...)

	kv, err = orderTable.Save()
	if err != nil {
		zlog.Error("updateIndex", "orderTable.Save", err.Error())
		return nil, nil
	}
	kvs = append(kvs, kv...)

	kv, err = historyTable.Save()
	if err != nil {
		zlog.Error("updateIndex", "historyTable.Save", err.Error())
		return nil, nil
	}
	kvs = append(kvs, kv...)
	dbSet.KV = append(dbSet.KV, kvs...)
	dbSet = z.addAutoRollBack(tx, dbSet.KV)
	localDB := z.GetLocalDB()
	for _, kv1 := range dbSet.KV {
		err := localDB.Set(kv1.Key, kv1.Value)
		if err != nil {
			zlog.Error("updateIndex", "localDB.Set", err.Error())
			return dbSet, err
		}
	}
	return dbSet, nil
}

// Set automatic rollback
func (z *zksync) addAutoRollBack(tx *types.Transaction, kv []*types.KeyValue) *types.LocalDBSet {
	dbSet := &types.LocalDBSet{}
	dbSet.KV = z.AddRollbackKV(tx, tx.Execer, kv)
	return dbSet
}

func (z *zksync) updateIndex(marketTable, orderTable, historyTable *table.Table, receipt *zty.SpotTradeReceiptExchange) (kvs []*types.KeyValue) {
	switch receipt.Order.Status {
	case zty.Ordered:
		err := z.updateOrder(marketTable, orderTable, historyTable, receipt.GetOrder(), receipt.GetIndex())
		if err != nil {
			return nil
		}
		err = z.updateMatchOrders(marketTable, orderTable, historyTable, receipt.GetOrder(), receipt.GetMatchOrders(), receipt.GetIndex())
		if err != nil {
			return nil
		}
	case zty.Completed:
		err := z.updateOrder(marketTable, orderTable, historyTable, receipt.GetOrder(), receipt.GetIndex())
		if err != nil {
			return nil
		}
		err = z.updateMatchOrders(marketTable, orderTable, historyTable, receipt.GetOrder(), receipt.GetMatchOrders(), receipt.GetIndex())
		if err != nil {
			return nil
		}
	case zty.Revoked:
		err := z.updateOrder(marketTable, orderTable, historyTable, receipt.GetOrder(), receipt.GetIndex())
		if err != nil {
			return nil
		}
	}

	return
}

func (z *zksync) updateOrder(marketTable, orderTable, historyTable *table.Table, order *zty.SpotTradeOrder, index int64) error {
	left := order.GetSpotTradeInfo().LeftAssetTokenID
	right := order.GetSpotTradeInfo().RightAssetTokenID
	op := order.GetSpotTradeInfo().GetOp()
	price := order.GetSpotTradeInfo().GetPrice()
	switch order.Status {
	case zty.Ordered:
		var markDepth zty.SpotTradeMarketDepth
		depth, err := queryMarketDepth(marketTable, left, right, op, price)
		if err == types.ErrNotFound {
			markDepth.Price = price
			markDepth.LeftAssetTokenID = left
			markDepth.RightAssetTokenID = right
			markDepth.Op = op
			markDepth.Amount = order.Balance
		} else {
			markDepth.Price = price
			markDepth.LeftAssetTokenID = left
			markDepth.LeftAssetTokenID = right
			markDepth.Op = op
			markDepth.Amount = depth.Amount + order.Balance
		}
		err = marketTable.Replace(&markDepth)
		if err != nil {
			zlog.Error("updateOrder", "marketTable.Replace", err.Error())
		}
		err = orderTable.Replace(order)
		if err != nil {
			zlog.Error("updateOrder", "orderTable.Replace", err.Error())
		}

	case zty.Completed:
		err := historyTable.Replace(order)
		if err != nil {
			zlog.Error("updateOrder", "historyTable.Replace", err.Error())
		}
	case zty.Revoked:
		var marketDepth zty.SpotTradeMarketDepth
		depth, err := queryMarketDepth(marketTable, left, right, op, price)
		if depth != nil {
			marketDepth.Price = price
			marketDepth.LeftAssetTokenID = left
			marketDepth.RightAssetTokenID = right
			marketDepth.Op = op

			depthAmount := new(big.Int).SetInt64(depth.Amount)
			orderBalance := new(big.Int).SetInt64(order.Balance)

			zeroBig := new(big.Int)
			marketDepthAmount := depthAmount.Sub(depthAmount, orderBalance)
			marketDepth.Amount = marketDepthAmount.Int64()

			if marketDepthAmount.Cmp(zeroBig) > 0 {
				err = marketTable.Replace(&marketDepth)
				if err != nil {
					zlog.Error("updateIndex", "marketTable.Replace", err.Error())
				}
			} else {
				err = marketTable.DelRow(&marketDepth)
				if err != nil {
					zlog.Error("updateIndex", "marketTable.DelRow", err.Error())
				}
			}
		}

		primaryKey := []byte(fmt.Sprintf("%022d", order.OrderID))
		err = orderTable.Del(primaryKey)
		if err != nil {
			zlog.Error("updateIndex", "orderTable.Del", err.Error())
		}
		order.Status = zty.Revoked
		order.Index = index
		err = historyTable.Replace(order)
		if err != nil {
			zlog.Error("updateIndex", "historyTable.Replace", err.Error())
		}
	}
	return nil
}

func (z *zksync) updateMatchOrders(marketTable, orderTable, historyTable *table.Table, order *zty.SpotTradeOrder, matchOrders []*zty.SpotTradeOrder, index int64) error {
	left := order.GetSpotTradeInfo().GetLeftAssetTokenID()
	right := order.GetSpotTradeInfo().GetRightAssetTokenID()
	op := order.GetSpotTradeInfo().GetOp()
	if len(matchOrders) == 0 {
		zlog.Debug("updateMatchOrders", "nil matchOrders", 0)
		return nil
	}

	cache := make(map[int64]*big.Int)
	zeroBigInt := new(big.Int)
	for i, matchOrder := range matchOrders {
		matchOrderBalance := new(big.Int).SetInt64(matchOrder.Balance)
		matchOrderExecuted := new(big.Int).SetInt64(matchOrder.Balance)
		if matchOrderBalance.Cmp(zeroBigInt) <= 0 && matchOrderExecuted.Cmp(zeroBigInt) <= 0 {
			var matchDepth zty.SpotTradeMarketDepth
			matchDepth.Price = matchOrder.AVGPrice
			matchDepth.LeftAssetTokenID = left
			matchDepth.RightAssetTokenID = right
			matchDepth.Op = OpSwap(op)
			matchDepth.Amount = 0
			err := marketTable.DelRow(&matchDepth)
			if err != nil && err != types.ErrNotFound {
				zlog.Error("updateMatchOrders", "marketTable.DelRow", err.Error())
			}
			continue
		}
		if matchOrder.Status == zty.Completed {
			err := orderTable.DelRow(matchOrder)
			if err != nil {
				zlog.Error("updateMatchOrders", "orderTable.DelRow", err.Error())
			}
			matchOrder.Index = index + int64(i+1)
			err = historyTable.Replace(matchOrder)
			if err != nil {
				zlog.Error("updateMatchOrders", "historyTable.Replace", err.Error())
			}
		} else if matchOrder.Status == zty.Ordered {
			err := orderTable.Replace(matchOrder)
			if err != nil {
				zlog.Error("updateIndex", "orderTable.Replace", err.Error())
			}
		}
		executed := cache[matchOrder.GetSpotTradeInfo().Price]
		executed = executed.Add(executed, matchOrderExecuted)
		cache[matchOrder.GetSpotTradeInfo().Price] = executed
	}

	for pr, executed := range cache {
		var matchDepth zty.SpotTradeMarketDepth
		depth, err := queryMarketDepth(marketTable, left, right, OpSwap(op), pr)
		if err != nil {
			zlog.Error("updateMatchOrders", "Failed to set depth.Amount", depth.Amount, "error", err.Error())
			return err
		}
		matchDepth.Price = pr
		matchDepth.LeftAssetTokenID = left
		matchDepth.RightAssetTokenID = right
		matchDepth.Op = OpSwap(op)
		depthAmount := new(big.Int).SetInt64(depth.Amount)

		matchDepthAmount := depthAmount.Sub(depthAmount, executed)
		matchDepth.Amount = matchDepthAmount.Int64()

		if matchDepthAmount.Cmp(zeroBigInt) > 0 {
			err = marketTable.Replace(&matchDepth)
			if err != nil {
				zlog.Error("updateMatchOrders", "marketTable.Replace", err.Error())
				return err
			}
		} else {
			err = marketTable.DelRow(&matchDepth)
			if err != nil {
				zlog.Error("updateMatchOrders", "marketTable.DelRow", err.Error())
				return err
			}
		}
	}

	return nil
}

func queryMarketDepth(marketTable *table.Table, leftTokenID, rightTokenID uint32, op int32, price int64) (*zty.SpotTradeMarketDepth, error) {
	primaryKey := []byte(fmt.Sprintf("%d:%d:%d:%s", leftTokenID, rightTokenID, op, price))
	row, err := marketTable.GetData(primaryKey)
	if err != nil {
		// In localDB, delete is set to nil first and deleted last
		if err == types.ErrDecode && row == nil {
			err = types.ErrNotFound
		}
		return nil, err
	}
	return row.Data.(*zty.SpotTradeMarketDepth), nil
}
