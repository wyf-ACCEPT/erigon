package parallel_tests

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"

	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/tests"
	"github.com/ledgerwatch/erigon/turbo/stages/mock"
)

func TestSimpleStorage(t *testing.T) {

	address_num := 10

	// ============================================================
	// Build evm context
	excessBlobGas := uint64(50000)
	context := evmtypes.BlockContext{
		CanTransfer:   core.CanTransfer,
		Transfer:      core.Transfer,
		Coinbase:      libcommon.Address{},
		BlockNumber:   8000000,
		Time:          5,
		Difficulty:    big.NewInt(0x30000),
		GasLimit:      uint64(6000000),
		BaseFee:       uint256.NewInt(0),
		ExcessBlobGas: &excessBlobGas,
	}
	m := mock.Mock(t)
	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	rules := params.AllProtocolChanges.Rules(context.BlockNumber, context.Time)

	// ============================================================
	// Generate addresses & allocate balance
	sendersPk, sendersAddr, _ := generateAccounts(address_num)

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
	// Deploy contract
	bytecode := hexutil.MustDecode("0x608060405234801561000f575f80fd5b5061018a8061001d5f395ff3fe608060405234801561000f575f80fd5b506004361061003f575f3560e01c80632e64cec1146100435780636057361d14610061578063d145890f1461007d575b5f80fd5b61004b61009b565b60405161005891906100e2565b60405180910390f35b61007b60048036038101906100769190610129565b6100a3565b005b6100856100ac565b60405161009291906100e2565b60405180910390f35b5f8054905090565b805f8190555050565b5f3073ffffffffffffffffffffffffffffffffffffffff163b905090565b5f819050919050565b6100dc816100ca565b82525050565b5f6020820190506100f55f8301846100d3565b92915050565b5f80fd5b610108816100ca565b8114610112575f80fd5b50565b5f81359050610123816100ff565b92915050565b5f6020828403121561013e5761013d6100fb565b5b5f61014b84828501610115565b9150509291505056fea2646970667358221220612d74d879ce08572434f599a9607d32f4442a31635246b833866b468c7d921564736f6c63430008140033")

	contractAddr, err := sendCreate(
		sendersAddr[0], 1, bytecode, sendersPk[0], evm, t,
	)
	if err != nil {
		t.Fatalf("failed to deploy contract: %v", err)
	} else {
		fmt.Println("Contract address:", contractAddr)
	}

	// ============================================================
	// Simple storage - write function
	if err = sendTransaction(
		sendersAddr[1], contractAddr, uint256.NewInt(0), 1,
		hexutil.MustDecode("0x6057361d0000000000000000000000000000000000000000000000000000000000300004"),
		sendersPk[1], evm, t,
	); err != nil {
		t.Fatalf("failed to execute transaction: %v", err)
	}

	// ============================================================
	// Simple storage - read function
	if ret, gas, err := evm.StaticCall(
		vm.AccountRef(sendersAddr[0]), contractAddr, hexutil.MustDecode("0x2e64cec1"), 5000000,
	); err != nil {
		t.Fatalf("failed to call contract: %v", err)
	} else {
		fmt.Println("View result:", hexutility.Encode(ret))
		fmt.Println("Gas left:", gas)
	}
}
