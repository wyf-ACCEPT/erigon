package parallel_tests

import (
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	libcommon "github.com/erigontech/erigon-lib/common"

	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/tests"
	"github.com/erigontech/erigon/turbo/stages/mock"
)

func TestSimpleTransfer(t *testing.T) {

	address_num := 10
	block_txn_num := 128

	// ============================================================
	// Build evm context
	context := evmtypes.BlockContext{
		CanTransfer: core.CanTransfer,
		Transfer:    consensus.Transfer,
		Coinbase:    libcommon.Address{},
		BlockNumber: 8000000,
		Time:        5,
		Difficulty:  big.NewInt(0x30000),
		GasLimit:    uint64(6000000),
		BaseFee:     uint256.NewInt(0),
		BlobBaseFee: uint256.NewInt(50000),
	}

	m := mock.Mock(t)
	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	rules := params.AllProtocolChanges.Rules(context.BlockNumber, context.Time)
	signer := types.LatestSignerForChainID(big.NewInt(1))

	// ============================================================
	// Generate addresses & balance
	sendersPk, sendersAddr, _ := generateAccounts(address_num)
	_, receiversAddr, _ := generateAccounts(address_num)

	alloc := types.GenesisAlloc{}
	for i := 0; i < address_num; i++ {
		alloc[sendersAddr[i]] = types.GenesisAccount{
			Nonce:   1,
			Code:    []byte{},
			Balance: big.NewInt(500000000000000),
		}
	}

	statedb, _ := tests.MakePreState(rules, tx, alloc, context.BlockNumber)
	evm := vm.NewEVM(context, evmtypes.TxContext{
		GasPrice: uint256.NewInt((1)),
	}, statedb, params.AllProtocolChanges, vm.Config{})

	// ============================================================
	// Simple transfer
	var st, ed time.Time
	for i := 0; i < address_num; i++ {
		if i == 0 {
			st = time.Now()
		} else if i%block_txn_num == 0 {
			ed = time.Now()
			fmt.Printf("block %d Cost %v\n", i/128-1, ed.Sub(st))
			st = time.Now()
		}

		unsignedTx := types.NewTransaction(
			1, receiversAddr[i], uint256.NewInt(155000), 5000000, uint256.NewInt(1), []byte{},
		)
		txn, _ := types.SignTx(unsignedTx, *signer, sendersPk[i])
		msg, _ := txn.AsMessage(*signer, nil, rules)

		evm.TxContext.Origin = sendersAddr[i]
		st := core.NewStateTransition(evm, msg, new(core.GasPool).AddGas(txn.GetGas()).AddBlobGas(txn.GetBlobGas()))

		if _, err = st.TransitionDb(false, false); err != nil {
			t.Fatalf("failed to execute transaction: %v", err)
		}

		if i < 15 {
			sender, _ := txn.GetSender()
			line := fmt.Sprintf(
				"[%d] Transfer %d wei from %s to %s", i,
				txn.GetValue().Uint64(), sender, txn.GetTo(),
			)
			fmt.Println(line)
		}
	}
}
