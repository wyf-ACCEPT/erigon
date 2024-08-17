// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package exec3

import (
	"fmt"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"

	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/transactions"
)

type GenericTracer interface {
	vm.EVMLogger
	SetTransaction(tx types.Transaction)
	Found() bool
}

type Resetable interface {
	Reset()
}

type TraceWorker struct {
	stateReader  *state.HistoryReaderV3
	engine       consensus.EngineReader
	headerReader services.HeaderReader
	tracer       GenericTracer
	ibs          *state.IntraBlockState
	evm          *vm.EVM

	// calculated by .changeBlock()
	blockHash common.Hash
	blockNum  uint64
	header    *types.Header
	blockCtx  *evmtypes.BlockContext
	rules     *chain.Rules
	signer    *types.Signer
	vmConfig  *vm.Config
}

func NewTraceWorker(cc *chain.Config, engine consensus.EngineReader, br services.HeaderReader) *TraceWorker {
	stateReader := state.NewHistoryReaderV3()
	ie := &TraceWorker{
		engine:       engine,
		headerReader: br,
		stateReader:  stateReader,
		evm:          vm.NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, cc, vm.Config{}),
		vmConfig:     &vm.Config{},
		ibs:          state.New(stateReader),
	}
	return ie
}

func (e *TraceWorker) SetTracer(tracer GenericTracer) {
	e.vmConfig.Tracer = tracer
	e.vmConfig.Debug = tracer != nil
	if casted, ok := tracer.(GenericTracer); ok {
		e.tracer = casted
	}
}

func (e *TraceWorker) Close() {
	e.evm.JumpDestCache.LogStats()
}

func (e *TraceWorker) ChangeBlock(tx kv.TemporalTx, header *types.Header) {
	e.stateReader.SetTx(tx)
	e.blockNum = header.Number.Uint64()
	cc := e.evm.ChainConfig()
	blockCtx := transactions.NewEVMBlockContext(e.engine, header, true /* requireCanonical */, tx, e.headerReader, cc)
	e.blockCtx = &blockCtx
	e.blockHash = header.Hash()
	e.header = header
	e.rules = cc.Rules(e.blockNum, header.Time)
	e.signer = types.MakeSigner(cc, e.blockNum, header.Time)
	e.vmConfig.SkipAnalysis = core.SkipAnalysis(cc, e.blockNum)
	e.evm.ResetBetweenBlocks(*e.blockCtx, *e.vmConfig, e.rules)
}

func (e *TraceWorker) GetLogs(txIdx int, txn types.Transaction) types.Logs {
	return e.ibs.GetLogs(txn.Hash())
}

func (e *TraceWorker) ExecTxn(txNum uint64, txIndex int, txn types.Transaction) (*evmtypes.ExecutionResult, error) {
	e.stateReader.SetTxNum(txNum)
	txHash := txn.Hash()
	e.ibs.Reset()
	e.ibs.SetTxContext(txHash, e.blockHash, txIndex)
	gp := new(core.GasPool).AddGas(txn.GetGas()).AddBlobGas(txn.GetBlobGas())
	msg, err := txn.AsMessage(*e.signer, e.header.BaseFee, e.rules)
	if err != nil {
		return nil, err
	}
	e.evm.Reset(core.NewEVMTxContext(msg), e.ibs)
	if msg.FeeCap().IsZero() {
		// Only zero-gas transactions may be service ones
		syscall := func(contract common.Address, data []byte) ([]byte, error) {
			return core.SysCallContract(contract, data, e.evm.ChainConfig(), e.ibs, e.header, e.engine, true /* constCall */)
		}
		msg.SetIsFree(e.engine.IsServiceTransaction(msg.From(), syscall))
	}
	res, err := core.ApplyMessage(e.evm, msg, gp, true /* refunds */, false /* gasBailout */)
	if err != nil {
		return nil, fmt.Errorf("%w: blockNum=%d, txNum=%d, %s", err, e.blockNum, txNum, e.ibs.Error())
	}
	if e.tracer != nil {
		if e.tracer.Found() {
			e.tracer.SetTransaction(txn)
		}
	}
	return res, nil
}
