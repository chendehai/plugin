// Copyright Fuzamei Corp. 2018 All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package para

import (
	"bytes"
	"context"
	"time"

	"github.com/33cn/chain33/common"
	"github.com/33cn/chain33/common/crypto"
	"github.com/33cn/chain33/types"
	paracross "github.com/33cn/plugin/plugin/dapp/paracross/types"
	pt "github.com/33cn/plugin/plugin/dapp/paracross/types"
	"github.com/pkg/errors"
)

var (
	consensusInterval = 16 //about 1 new block interval
)

type commitMsgClient struct {
	paraClient         *client
	waitMainBlocks     int32
	commitMsgNotify    chan int64
	delMsgNotify       chan int64
	mainBlockAdd       chan *types.BlockDetail
	currentTx          *types.Transaction
	checkTxCommitTimes int32
	privateKey         crypto.PrivKey
	quit               chan struct{}
}

//获取主链和平行链本身节点的平行链共识状态
type consensStatus struct {
	mainStatus *pt.ParacrossStatus
	selfStatus *pt.ParacrossStatus
}

func (client *commitMsgClient) handler() {
	var isSync bool
	var notification []int64 //记录每次系统重启后 min and current height
	var finishHeight int64
	var sendingHeight int64 //当前发送的最大高度
	var sendingMsgs []*pt.ParacrossNodeStatus
	var readTick <-chan time.Time

	client.paraClient.wg.Add(1)
	consensusCh := make(chan *consensStatus, 1)
	go client.getConsensusHeight(consensusCh)

	client.paraClient.wg.Add(1)
	priKeyCh := make(chan crypto.PrivKey, 1)
	go client.fetchPrivacyKey(priKeyCh)

	client.paraClient.wg.Add(1)
	sendMsgCh := make(chan *types.Transaction, 1)
	go client.sendCommitMsg(sendMsgCh)

out:
	for {
		select {
		case height := <-client.commitMsgNotify:
			if notification == nil {
				notification = append(notification, height)
				notification = append(notification, height)
				finishHeight = height - 1
			} else {
				//[0] need update to min value if any, [1] always get current height, as for fork case, the height may lower than before
				if height < notification[0] {
					notification[0] = height
					finishHeight = height - 1
				}
				notification[1] = height
				if finishHeight >= notification[1] {
					finishHeight = notification[1] - 1
				}
			}

		case height := <-client.delMsgNotify:
			if len(notification) > 0 && height <= notification[1] {
				notification[1] = height - 1
			}
			if height <= sendingHeight && client.currentTx != nil {
				sendingMsgs = nil
				client.currentTx = nil
			}
			//在分叉的主链上，有可能在del完全之前收到共识消息后sync又置为true，然后发送消息，不过影响不大，共识消息间隔比较长
			isSync = false
			plog.Debug("para del block", "delHeight", height)

		case block := <-client.mainBlockAdd:
			if client.currentTx != nil && client.paraClient.isCaughtUp {
				exist := checkTxInMainBlock(client.currentTx, block)
				if exist {
					finishHeight = sendingHeight
					sendingMsgs = nil
					client.currentTx = nil
				} else {
					client.checkTxCommitTimes++
					if client.checkTxCommitTimes > client.waitMainBlocks {
						//超过等待最大次数，reset，重新组织发送，防止一直发送同一笔消息
						sendingMsgs = nil
						client.currentTx = nil
						client.checkTxCommitTimes = 0
					}
				}
			}

		case <-readTick:
			plog.Debug("para readTick", "notify", notification, "sending", len(sendingMsgs),
				"finishHeight", finishHeight, "txIsNil", client.currentTx == nil, "sync", isSync)

			if notification != nil && finishHeight < notification[1] && client.currentTx == nil && isSync {
				count := notification[1] - finishHeight
				if count > types.TxGroupMaxCount {
					count = types.TxGroupMaxCount
				}
				status, err := client.getNodeStatus(finishHeight+1, finishHeight+count)
				if err != nil {
					plog.Error("para commit msg read tick", "err", err.Error())
					continue
				}

				signTx, count, err := client.calcCommitMsgTxs(status)
				if err != nil || signTx == nil {
					continue
				}
				sendingHeight = finishHeight + count
				sendingMsgs = status[:count]
				client.currentTx = signTx
				client.checkTxCommitTimes = 0
				sendMsgCh <- client.currentTx

				for i, msg := range sendingMsgs {
					plog.Info("paracommitmsg sending", "idx", i, "height", msg.Height, "mainheight", msg.MainBlockHeight,
						"blockhash", common.HashHex(msg.BlockHash), "mainHash", common.HashHex(msg.MainBlockHash),
						"from", client.paraClient.authAccount)
				}
			}

		//获取正在共识的高度，同步有两层意思，一个是主链跟其他节点完成了同步，另一个是当前平行链节点的高度追赶上了共识高度
		//一般来说高度增长从小到大： notifiy[0] -- selfConsensusHeight(mainHeight) -- finishHeight -- sendingHeight -- notify[1]
		case rsp := <-consensusCh:
			selfConsensusHeight := rsp.selfStatus.Height
			mainConsensHeight := rsp.mainStatus.Height
			plog.Info("para consensus rcv", "notify", notification, "sending", len(sendingMsgs),
				"mainHeigt", rsp.mainStatus.Height, "mainlockhash", common.ToHex(rsp.mainStatus.BlockHash),
				"selfHeight", rsp.selfStatus.Height, "selfHash", common.ToHex(rsp.selfStatus.BlockHash), "sync", isSync)

			if notification == nil {
				continue
			}

			//所有节点还没有共识场景或新节点或重启节点catchingUp场景，要等到收到区块高度大于主链共识高度时候发送，在catchingup时候本身共识高度和块高度一起增长
			if selfConsensusHeight == -1 || (notification[1] > mainConsensHeight) {
				isSync = true
			}

			//未共识过的小于当前共识高度的区块，可以不参与共识, 如果是新节点，一直等到同步的区块达到了共识高度，才设置同步参与共识

			if finishHeight < selfConsensusHeight {
				finishHeight = selfConsensusHeight
			}

			// 自共识分叉高度切换场景， 分叉高度前平行链共识高度是-1，分叉高度后，需要重发tx平行链共识高度才能增长
			if isMainCommitHeightForked() && selfConsensusHeight == -1 && mainConsensHeight > selfConsensusHeight {
				finishHeight = selfConsensusHeight
			}

			//系统每次重启都有检查一次共识，如果共识高度落后于系统起来后完成的第一个高度或最小高度，说明可能有共识空洞，需要把从当前共识高度到完成的
			//最大高度重发一遍，直到确认收到，发过的最小到最大高度也要重发是因为之前空洞原因共识不连续，即便满足2/3节点也不会增长，需要重发来触发commit
			//此处也整合了当前consensus height=-1 场景
			// 需要是<而不是<=, 因为notification[0]被认为是系统起来后已经发送过的
			nextConsensHeight := selfConsensusHeight + 1
			if nextConsensHeight < notification[0] {
				notification[0] = nextConsensHeight
				finishHeight = selfConsensusHeight
				sendingMsgs = nil
				client.currentTx = nil
			}

			//在某些特殊场景下，比如平行链连接的主链节点分叉后又恢复，主链的共识高度低于分叉高度时候，主链上形成共识空洞，需要从共识高度重新发送而不是分叉高度
			//共识高度和分叉高度不一致其中一个原因是共识交易组里面某个高度分叉了，分叉的主链节点执行成功，而其他主链节点执行失败
			//理论上来说selfConsensusHeight只能小于等于mainConsensusHeihgt，因为在主链先共识之后才会同步到平行链
			//此处主链共识高度应该会开始追赶平行链高度，在这种异常场景下，可能会有重复发送
			if mainConsensHeight < selfConsensusHeight {
				plog.Info("para consensus reset", "finishHeight", finishHeight, "mainHeight", mainConsensHeight, "selfHeight", selfConsensusHeight)
				finishHeight = mainConsensHeight
				sendingMsgs = nil
				client.currentTx = nil
			}

		case key, ok := <-priKeyCh:
			if !ok {
				priKeyCh = nil
				continue
			}
			client.privateKey = key
			readTick = time.Tick(time.Second * 2)

		case <-client.quit:
			break out
		}
	}

	client.paraClient.wg.Done()
}

func (client *commitMsgClient) calcCommitMsgTxs(notifications []*pt.ParacrossNodeStatus) (*types.Transaction, int64, error) {
	txs, count, err := client.batchCalcTxGroup(notifications)
	if err != nil {
		txs, err = client.singleCalcTx((notifications)[0])
		if err != nil {
			plog.Error("single calc tx", "height", notifications[0].Height)

			return nil, 0, err
		}
		return txs, 1, nil
	}
	return txs, int64(count), nil
}

func (client *commitMsgClient) getTxsGroup(txsArr *types.Transactions) (*types.Transaction, error) {
	if len(txsArr.Txs) < 2 {
		tx := txsArr.Txs[0]
		tx.Sign(types.SECP256K1, client.privateKey)
		return tx, nil
	}

	group, err := types.CreateTxGroup(txsArr.Txs)
	if err != nil {
		plog.Error("para CreateTxGroup", "err", err.Error())
		return nil, err
	}
	err = group.Check(0, types.GInt("MinFee"), types.GInt("MaxFee"))
	if err != nil {
		plog.Error("para CheckTxGroup", "err", err.Error())
		return nil, err
	}
	for i := range group.Txs {
		group.SignN(i, int32(types.SECP256K1), client.privateKey)
	}

	newtx := group.Tx()
	return newtx, nil
}

func (client *commitMsgClient) batchCalcTxGroup(notifications []*pt.ParacrossNodeStatus) (*types.Transaction, int, error) {
	var rawTxs types.Transactions
	for _, status := range notifications {
		execName := pt.ParaX
		if isMainCommitHeightForked() {
			execName = paracross.GetExecName()
		}
		tx, err := paracross.CreateRawCommitTx4MainChain(status, execName, 0)
		if err != nil {
			plog.Error("para get commit tx", "block height", status.Height)
			return nil, 0, err
		}
		rawTxs.Txs = append(rawTxs.Txs, tx)
	}

	txs, err := client.getTxsGroup(&rawTxs)
	if err != nil {
		return nil, 0, err
	}
	return txs, len(notifications), nil
}

func (client *commitMsgClient) singleCalcTx(status *pt.ParacrossNodeStatus) (*types.Transaction, error) {
	execName := pt.ParaX
	if isMainCommitHeightForked() {
		execName = paracross.GetExecName()
	}
	tx, err := paracross.CreateRawCommitTx4MainChain(status, execName, 0)
	if err != nil {
		plog.Error("para get commit tx", "block height", status.Height)
		return nil, err
	}
	tx.Sign(types.SECP256K1, client.privateKey)
	return tx, nil

}

// 从ch收到tx有两种可能，readTick和addBlock, 如果
// 3 input case from ch: readTick , addBlock and delMsg to readTick, readTick trigger firstly and will block until received from addBlock
// if sendCommitMsgTx block quite long, write channel will be block in handle(), addBlock will not send new msg until rpc send over
// if sendCommitMsgTx block quite long, if delMsg occur, after send over, ignore previous tx succ or fail, new msg will be rcv and sent
// if sendCommitMsgTx fail, wait 1s resend the failed tx, if new tx rcv from ch, send the new one.
func (client *commitMsgClient) sendCommitMsg(ch chan *types.Transaction) {
	var err error
	var tx *types.Transaction
	resendTimer := time.After(time.Second * 1)

out:
	for {
		select {
		case tx = <-ch:
			err = client.sendCommitMsgTx(tx)
			if err != nil {
				resendTimer = time.After(time.Second * 1)
			}
		case <-resendTimer:
			if err != nil && tx != nil {
				err = client.sendCommitMsgTx(tx)
				if err != nil {
					resendTimer = time.After(time.Second * 1)
				}
			}
		case <-client.quit:
			break out
		}
	}

	client.paraClient.wg.Done()
}

func (client *commitMsgClient) sendCommitMsgTx(tx *types.Transaction) error {
	if tx == nil {
		return nil
	}
	resp, err := client.paraClient.grpcClient.SendTransaction(context.Background(), tx)
	if err != nil {
		plog.Error("sendCommitMsgTx send tx", "tx", tx.Hash(), "err", err.Error())
		return err
	}

	if !resp.GetIsOk() {
		plog.Error("sendCommitMsgTx send tx Nok", "tx", tx.Hash(), "err", string(resp.GetMsg()))
		return errors.New(string(resp.GetMsg()))
	}

	return nil

}

func checkTxInMainBlock(targetTx *types.Transaction, detail *types.BlockDetail) bool {
	targetHash := targetTx.Hash()

	for i, tx := range detail.Block.Txs {
		if bytes.Equal(targetHash, tx.Hash()) && detail.Receipts[i].Ty == types.ExecOk {
			return true
		}
	}
	return false

}

func isMainCommitHeightForked() bool {
	return curMainChainHeight > mainParaSelfConsensusForkHeight+100
}

//当前未考虑获取key非常多失败的场景， 如果获取height非常多，block模块会比较大，但是使用完了就释放了
//如果有必要也可以考虑每次最多取20个一个txgroup，发送共识部分循环获取发送也没问题
func (client *commitMsgClient) getNodeStatus(start, end int64) ([]*pt.ParacrossNodeStatus, error) {
	var ret []*pt.ParacrossNodeStatus
	if start == 0 {
		geneStatus, err := client.getGenesisNodeStatus()
		if err != nil {
			return nil, err
		}
		ret = append(ret, geneStatus)
		start++
	}
	if end < start {
		return ret, nil
	}

	req := &types.ReqBlocks{Start: start, End: end}
	count := req.End - req.Start + 1
	nodeList := make(map[int64]*pt.ParacrossNodeStatus, count+1)
	keys := &types.LocalDBGet{}
	for i := 0; i < int(count); i++ {
		key := paracross.CalcMinerHeightKey(types.GetTitle(), req.Start+int64(i))
		keys.Keys = append(keys.Keys, key)
	}

	r, err := client.paraClient.GetAPI().LocalGet(keys)
	if err != nil {
		return nil, err
	}
	if count != int64(len(r.Values)) {
		plog.Error("paracommitmsg get node status key", "expect count", count, "actual count", len(r.Values))
		return nil, err
	}
	for _, val := range r.Values {
		status := &pt.ParacrossNodeStatus{}
		err = types.Decode(val, status)
		if err != nil {
			return nil, err
		}
		if !(status.Height >= req.Start && status.Height <= req.End) {
			plog.Error("paracommitmsg decode node status", "height", status.Height, "expect start", req.Start,
				"end", req.End, "status", status)
			return nil, errors.New("paracommitmsg wrong key result")
		}
		nodeList[status.Height] = status

	}
	for i := 0; i < int(count); i++ {
		if nodeList[req.Start+int64(i)] == nil {
			plog.Error("paracommitmsg get node status key nil", "height", req.Start+int64(i))
			return nil, errors.New("paracommitmsg wrong key status result")
		}
	}

	v, err := client.paraClient.GetAPI().GetBlocks(req)
	if err != nil {
		return nil, err
	}
	if count != int64(len(v.Items)) {
		plog.Error("paracommitmsg get node status block", "expect count", count, "actual count", len(v.Items))
		return nil, err
	}
	for _, block := range v.Items {
		if !(block.Block.Height >= req.Start && block.Block.Height <= req.End) {
			plog.Error("paracommitmsg get node status block", "height", block.Block.Height, "expect start", req.Start, "end", req.End)
			return nil, errors.New("paracommitmsg wrong block result")
		}
		nodeList[block.Block.Height].BlockHash = block.Block.Hash()
		nodeList[block.Block.Height].StateHash = block.Block.StateHash
	}

	for i := 0; i < int(count); i++ {
		ret = append(ret, nodeList[req.Start+int64(i)])
	}
	return ret, nil

}

func (client *commitMsgClient) getGenesisNodeStatus() (*pt.ParacrossNodeStatus, error) {
	var status pt.ParacrossNodeStatus
	req := &types.ReqBlocks{Start: 0, End: 0}
	v, err := client.paraClient.GetAPI().GetBlocks(req)
	if err != nil {
		return nil, err
	}
	block := v.Items[0].Block
	if block.Height != 0 {
		return nil, errors.New("block chain not return 0 height block")
	}
	status.Title = types.GetTitle()
	status.Height = block.Height
	status.PreBlockHash = zeroHash[:]
	status.BlockHash = block.Hash()
	status.PreStateHash = zeroHash[:]
	status.StateHash = block.StateHash
	return &status, nil
}

func (client *commitMsgClient) onBlockAdded(height int64) error {
	select {
	case client.commitMsgNotify <- height:
	case <-client.quit:
	}

	return nil
}

func (client *commitMsgClient) onBlockDeleted(height int64) {
	select {
	case client.delMsgNotify <- height:
	case <-client.quit:
	}
}

func (client *commitMsgClient) onMainBlockAdded(block *types.BlockDetail) {
	select {
	case client.mainBlockAdd <- block:
	case <-client.quit:
	}
}

//only sync once, as main usually sync, here just need the first sync status after start up
func (client *commitMsgClient) mainSync() error {
	req := &types.ReqNil{}
	reply, err := client.paraClient.grpcClient.IsSync(context.Background(), req)
	if err != nil {
		plog.Error("Paracross main is syncing", "err", err.Error())
		return err
	}
	if !reply.IsOk {
		plog.Error("Paracross main reply not ok")
		return err
	}

	plog.Info("Paracross main sync succ")
	return nil

}

func (client *commitMsgClient) getConsensusHeight(consensusRst chan *consensStatus) {
	ticker := time.NewTicker(time.Second * time.Duration(consensusInterval))
	isSync := false
	defer ticker.Stop()

out:
	for {
		select {
		case <-client.quit:
			break out
		case <-ticker.C:
			if !isSync {
				err := client.mainSync()
				if err != nil {
					continue
				}
				isSync = true
			}

			var status consensStatus

			//从本地查询共识高度
			ret, err := client.paraClient.GetAPI().QueryChain(&types.ChainExecutor{
				Driver:   "paracross",
				FuncName: "GetTitle",
				Param:    types.Encode(&types.ReqString{Data: types.GetTitle()}),
			})
			if err != nil {
				plog.Error("getConsensusHeight ", "err", err.Error())
				continue
			}
			resp, ok := ret.(*pt.ParacrossStatus)
			if !ok {
				plog.Error("getConsensusHeight ParacrossStatus nok")
				continue
			}
			status.selfStatus = resp

			//获取主链共识高度
			reply, err := client.paraClient.grpcClient.QueryChain(context.Background(), &types.ChainExecutor{
				Driver:   "paracross",
				FuncName: "GetTitle",
				Param:    types.Encode(&types.ReqString{Data: types.GetTitle()}),
			})
			if err != nil {
				plog.Error("getMainConsensusHeight", "err", err.Error())
				continue
			}
			if !reply.GetIsOk() {
				plog.Info("getMainConsensusHeight nok", "error", reply.GetMsg())
				continue
			}
			var result pt.ParacrossStatus
			err = types.Decode(reply.Msg, &result)
			if err != nil {
				plog.Error("getMainConsensusHeight decode", "err", err.Error())
				continue
			}
			status.mainStatus = &result
			//如果没有开启平行链自共识， 采用主链共识, 平行链自共识开启会影响发送tx高度的判断
			if !isMainCommitHeightForked() {
				status.selfStatus = status.mainStatus
			}
			consensusRst <- &status
		}
	}

	client.paraClient.wg.Done()
}

func (client *commitMsgClient) fetchPrivacyKey(ch chan crypto.PrivKey) {
	defer client.paraClient.wg.Done()
	if client.paraClient.authAccount == "" {
		close(ch)
		return
	}

	req := &types.ReqString{Data: client.paraClient.authAccount}
out:
	for {
		select {
		case <-client.quit:
			break out
		case <-time.NewTimer(time.Second * 2).C:
			msg := client.paraClient.GetQueueClient().NewMessage("wallet", types.EventDumpPrivkey, req)
			err := client.paraClient.GetQueueClient().Send(msg, true)
			if err != nil {
				plog.Error("para commit send msg", "err", err.Error())
				break out
			}
			resp, err := client.paraClient.GetQueueClient().Wait(msg)
			if err != nil {
				plog.Error("para commit msg sign to wallet", "err", err.Error())
				continue
			}
			str := resp.GetData().(*types.ReplyString).Data
			pk, err := common.FromHex(str)
			if err != nil && pk == nil {
				panic(err)
			}

			secp, err := crypto.New(types.GetSignName("", types.SECP256K1))
			if err != nil {
				panic(err)
			}

			priKey, err := secp.PrivKeyFromBytes(pk)
			if err != nil {
				panic(err)
			}

			ch <- priKey
			close(ch)
			break out
		}
	}

}
