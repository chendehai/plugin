package chain33

import (
	"errors"
	"github.com/33cn/plugin/plugin/dapp/cross2eth/ebrelayer/relayer/events"
	ebTypes "github.com/33cn/plugin/plugin/dapp/cross2eth/ebrelayer/types"
	chain33EvmCommon "github.com/33cn/plugin/plugin/dapp/evm/executor/vm/common"
	"github.com/ethereum/go-ethereum/common"
	"math/big"
)

var clientChainID = int64(0)
var bridgeBankAddr = "0x8afdadfc88a1087c9a1d6c0f5dd04634b87f303a"

func (chain33Relayer *Relayer4Chain33) SimLockFromBtc(lock *ebTypes.LockBRC20) error {
	amount := big.NewInt(1)
	amount.SetString(lock.Amount, 10)
	evmEventType := events.ClaimTypeLock
	chainName := ebTypes.BinanceChainName
	if lock.ChainName != "" {
		chainName = lock.ChainName
	}
	var chain33Msg = &events.Chain33Msg{
		ClaimType:        evmEventType,
		Chain33Sender:    *chain33EvmCommon.StringToAddress(lock.OwnerAddr),
		EthereumReceiver: common.HexToAddress(lock.EtherumReceiver),
		Symbol:           lock.Symbol,
		Amount:           amount,
		TxHash:           common.FromHex(lock.BtcTxHash),
		Nonce:            1,
	}

	channel, ok := chain33Relayer.chain33MsgChan[chainName]
	if !ok {
		relayerLog.Error("handleBurnLockWithdrawEvent", "No bridgeSymbol2EthChainName", chainName)
		return errors.New("ErrNoChain33MsgChan4EthChainName")
	}

	channel <- chain33Msg

	return nil
}
