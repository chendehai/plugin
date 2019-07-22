// Copyright Fuzamei Corp. 2018 All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package executor

import (
	"testing"
	"github.com/33cn/chain33/types"
	auty "github.com/33cn/plugin/plugin/dapp/autonomy/types"
	"github.com/stretchr/testify/require"
	"github.com/33cn/chain33/system/dapp"
	"github.com/33cn/chain33/util"
)

func TestExecLocalRule(t *testing.T) {
	au := &Autonomy{}
	//TyLogPropRule
	cur := &auty.AutonomyProposalRule{
		PropRule: &auty.ProposalRule{},
		CurRule: &auty.RuleConfig{},
		VoteResult: &auty.VoteResult{},
		Status: auty.AutonomyStatusProposalRule,
		Address: "11111111111111",
		Height: 1,
		Index: 2,
	}
	receiptRule := &auty.ReceiptProposalRule{
		Prev: nil,
		Current: cur,
	}
	receipt := &types.ReceiptData{
		Logs: []*types.ReceiptLog{
			{Ty: auty.TyLogPropRule, Log:types.Encode(receiptRule)},
		},
	}
	set, err := au.execLocalRule(receipt)
	require.NoError(t, err)
	require.NotNil(t, set)
	require.Equal(t, set.KV[0].Key, calcRuleKey4StatusHeight(cur.Status,
		dapp.HeightIndexStr(cur.Height, int64(cur.Index))))

	// TyLogRvkPropRule
	pre1 := copyAutonomyProposalRule(cur)
	cur.Status = auty.AutonomyStatusRvkPropRule
	cur.Height = 2
	cur.Index = 3
	receiptRule1 := &auty.ReceiptProposalRule{
		Prev: pre1,
		Current: cur,
	}
	set, err = au.execLocalRule(&types.ReceiptData{
		Logs: []*types.ReceiptLog{
			{Ty: auty.TyLogRvkPropRule, Log:types.Encode(receiptRule1)},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, set)
	require.Equal(t, set.KV[0].Key, calcRuleKey4StatusHeight(pre1.Status,
		dapp.HeightIndexStr(pre1.Height, int64(pre1.Index))))
	require.Equal(t, set.KV[0].Value, []byte(nil))
	require.Equal(t, set.KV[1].Key, calcRuleKey4StatusHeight(cur.Status,
		dapp.HeightIndexStr(cur.Height, int64(cur.Index))))

	// TyLogVotePropRule
	cur.Status = auty.AutonomyStatusProposalRule
	cur.Height = 1
	cur.Index = 2
	pre2 := copyAutonomyProposalRule(cur)
	cur.Status = auty.AutonomyStatusVotePropRule
	cur.Height = 2
	cur.Index = 3
	receiptRule2 := &auty.ReceiptProposalRule{
		Prev: pre2,
		Current: cur,
	}
	set, err = au.execLocalRule(&types.ReceiptData{
		Logs: []*types.ReceiptLog{
			{Ty: auty.TyLogVotePropRule, Log:types.Encode(receiptRule2)},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, set)
	require.Equal(t, set.KV[0].Key, calcRuleKey4StatusHeight(pre2.Status,
		dapp.HeightIndexStr(pre1.Height, int64(pre2.Index))))
	require.Equal(t, set.KV[0].Value, []byte(nil))
	require.Equal(t, set.KV[1].Key, calcRuleKey4StatusHeight(cur.Status,
		dapp.HeightIndexStr(cur.Height, int64(cur.Index))))
}

func TestExecDelLocalRule(t *testing.T) {
	au := &Autonomy{}
	//TyLogPropRule
	cur := &auty.AutonomyProposalRule{
		PropRule: &auty.ProposalRule{},
		CurRule: &auty.RuleConfig{},
		VoteResult: &auty.VoteResult{},
		Status: auty.AutonomyStatusProposalRule,
		Address: "11111111111111",
		Height: 1,
		Index: 2,
	}
	receiptRule := &auty.ReceiptProposalRule{
		Prev: nil,
		Current: cur,
	}
	receipt := &types.ReceiptData{
		Logs: []*types.ReceiptLog{
			{Ty: auty.TyLogPropRule, Log:types.Encode(receiptRule)},
		},
	}
	set, err := au.execDelLocalRule(receipt)
	require.NoError(t, err)
	require.NotNil(t, set)
	require.Equal(t, set.KV[0].Key, calcRuleKey4StatusHeight(cur.Status,
		dapp.HeightIndexStr(cur.Height, int64(cur.Index))))
	require.Equal(t, set.KV[0].Value, []byte(nil))

	// TyLogVotePropRule
	pre1 := copyAutonomyProposalRule(cur)
	cur.Status = auty.AutonomyStatusVotePropRule
	cur.Height = 2
	cur.Index = 3
	receiptRule2 := &auty.ReceiptProposalRule{
		Prev: pre1,
		Current: cur,
	}
	set, err = au.execDelLocalRule(&types.ReceiptData{
		Logs: []*types.ReceiptLog{
			{Ty: auty.TyLogVotePropRule, Log:types.Encode(receiptRule2)},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, set)
	require.Equal(t, set.KV[0].Key, calcRuleKey4StatusHeight(cur.Status,
		dapp.HeightIndexStr(cur.Height, int64(cur.Index))))
	require.Equal(t, set.KV[0].Value, []byte(nil))
	require.Equal(t, set.KV[1].Key, calcRuleKey4StatusHeight(pre1.Status,
		dapp.HeightIndexStr(pre1.Height, int64(pre1.Index))))
	require.NotNil(t, set.KV[1].Value)
}

func TestGetProposalRule(t *testing.T) {
	au := &Autonomy{
		dapp.DriverBase{},
	}
	_, storedb, _ := util.CreateTestDB()
	au.SetStateDB(storedb)
	tx := "1111111111111111111"
	storedb.Set(propRuleID(tx), types.Encode(&auty.AutonomyProposalRule{}))
	rsp, err := au.getProposalRule(&types.ReqString{Data:tx})
	require.NoError(t, err)
	require.NotNil(t, rsp)
	require.Equal(t, len(rsp.(*auty.ReplyQueryProposalRule).PropRules), 1)
}

func TestListProposalRule(t *testing.T) {
	au := &Autonomy{
		dapp.DriverBase{},
	}
	_, _, kvdb := util.CreateTestDB()
	au.SetLocalDB(kvdb)

	type statu struct {
		status int32
		height int64
		index  int64
	}

	testcase1 := []statu{
		{auty.AutonomyStatusRvkPropRule, 10, 2},
		{auty.AutonomyStatusVotePropRule, 15, 1},
		{auty.AutonomyStatusTmintPropRule, 20, 1},
	}
	testcase2 := []statu{
		{auty.AutonomyStatusProposalRule, 10, 1},
		{auty.AutonomyStatusProposalRule, 20, 2},
		{auty.AutonomyStatusProposalRule, 20, 5},
	}
	var testcase []statu
	testcase = append(testcase, testcase1...)
	testcase = append(testcase, testcase2...)
	cur := &auty.AutonomyProposalRule{
		PropRule: &auty.ProposalRule{},
		CurRule: &auty.RuleConfig{},
		VoteResult: &auty.VoteResult{},
		Status: auty.AutonomyStatusProposalRule,
		Address: "11111111111111",
		Height: 1,
		Index: 2,
	}
	for _, tcase := range testcase {
		key := calcRuleKey4StatusHeight(tcase.status,
			dapp.HeightIndexStr(tcase.height, int64(tcase.index)))
		cur.Status = tcase.status
		cur.Height = tcase.height
		cur.Index = int32(tcase.index)
		value := types.Encode(cur)
		kvdb.Set(key, value)
	}

	// 反向查找
	req := &auty.ReqQueryProposalRule{
		Status:auty.AutonomyStatusProposalRule,
		Count:10,
		Direction:0,
		Index: -1,
	}
	rsp, err := au.listProposalRule(req)
	require.NoError(t, err)
	require.Equal(t, len(rsp.(*auty.ReplyQueryProposalRule).PropRules), len(testcase2))
	k := 2
	for _, tcase := range testcase2 {
		require.Equal(t, rsp.(*auty.ReplyQueryProposalRule).PropRules[k].Height, tcase.height)
		require.Equal(t, rsp.(*auty.ReplyQueryProposalRule).PropRules[k].Index, int32(tcase.index))
		k--
	}

	// 正向查找
	req = &auty.ReqQueryProposalRule{
		Status:auty.AutonomyStatusProposalRule,
		Count:10,
		Direction:1,
		Index: -1,
	}
	rsp, err = au.listProposalRule(req)
	require.NoError(t, err)
	require.Equal(t, len(rsp.(*auty.ReplyQueryProposalRule).PropRules), len(testcase2))
	for i, tcase := range testcase2 {
		require.Equal(t, rsp.(*auty.ReplyQueryProposalRule).PropRules[i].Height, tcase.height)
		require.Equal(t, rsp.(*auty.ReplyQueryProposalRule).PropRules[i].Index, int32(tcase.index))
	}

	// 翻页查找
	req = &auty.ReqQueryProposalRule{
		Status:auty.AutonomyStatusProposalRule,
		Count:1,
		Direction:0,
		Index: -1,
	}
	rsp, err = au.listProposalRule(req)
	require.NoError(t, err)
	require.Equal(t, len(rsp.(*auty.ReplyQueryProposalRule).PropRules), 1)
	height := rsp.(*auty.ReplyQueryProposalRule).PropRules[0].Height
	index := rsp.(*auty.ReplyQueryProposalRule).PropRules[0].Index
	require.Equal(t, height, testcase2[2].height)
	require.Equal(t, index, int32(testcase2[2].index))
	//
	Index := height*types.MaxTxsPerBlock + int64(index)
	req = &auty.ReqQueryProposalRule{
		Status:auty.AutonomyStatusProposalRule,
		Count:10,
		Direction:0,
		Index: Index,
	}
	rsp, err = au.listProposalRule(req)
	require.Equal(t, len(rsp.(*auty.ReplyQueryProposalRule).PropRules), 2)
	require.Equal(t, rsp.(*auty.ReplyQueryProposalRule).PropRules[0].Height, testcase2[1].height)
	require.Equal(t, rsp.(*auty.ReplyQueryProposalRule).PropRules[0].Index, int32(testcase2[1].index))
	require.Equal(t, rsp.(*auty.ReplyQueryProposalRule).PropRules[1].Height, testcase2[0].height)
	require.Equal(t, rsp.(*auty.ReplyQueryProposalRule).PropRules[1].Index, int32(testcase2[0].index))
}