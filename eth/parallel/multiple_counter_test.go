package parallel_tests

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"
	"os"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/tests"
	"github.com/ledgerwatch/erigon/turbo/stages/mock"
)

func encodeCalldata(addrs []libcommon.Address) []byte {
	lengthArray := make([]byte, 8)
	binary.BigEndian.PutUint64(lengthArray, uint64(len(addrs)))
	data := hexutil.MustDecode("0x5437ab8c")
	data = append(data, hexutil.MustDecode(
		"0x0000000000000000000000000000000000000000000000000000000000000020",
	)...)
	data = append(data, leftPadBytes(lengthArray, 32)...)
	for _, addr := range addrs {
		data = append(data, leftPadBytes(addr.Bytes(), 32)...)
	}
	return data
}

func TestMultipleCounter(t *testing.T) {

	ADDRESS_NUM := 20000
	BATCH_SIZE := 200

	// ============================================================
	// Build evm context
	m := mock.Mock(t)
	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	// ============================================================
	// Generate addresses & allocate balance
	sendersPk, sendersAddr, _ := generateAccounts(ADDRESS_NUM)

	alloc := types.GenesisAlloc{}
	for i := 0; i < ADDRESS_NUM; i++ {
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
	bytecodeRaw, _ := os.ReadFile("./multiple_counter.bytecode")
	bytecode := hexutil.MustDecode("0x" + string(bytecodeRaw))

	contractAddr, err := sendCreate(
		sendersAddr[0], 1, bytecode, sendersPk[0], evm, t,
	)
	if err != nil {
		t.Fatalf("failed to deploy contract: %v", err)
	} else {
		fmt.Println("Contract address:", contractAddr)
	}

	// ============================================================
	// Multiple counter - write function
	for i := 0; i < ADDRESS_NUM/BATCH_SIZE; i += 1 {
		data := encodeCalldata(sendersAddr[i*BATCH_SIZE : (i+1)*BATCH_SIZE])
		if err = sendTransaction(
			sendersAddr[1], contractAddr, uint256.NewInt(0), uint64(i)+1,
			data, sendersPk[1], evm, t,
		); err != nil {
			t.Fatalf("Failed to execute transaction: %v", err)
		} else if i%10 == 0 {
			fmt.Printf(
				"Transaction executed successfully for address %d to %d\n", i*BATCH_SIZE, (i+1)*BATCH_SIZE,
			)
		}
	}

	// var txc wrap.TxContainer
	// txc.Tx = tx
	// domains, _ := state2.NewSharedDomains(tx, log.New())
	// defer domains.Close()
	// defer domains.Flush(context2.Background(), tx)
	// txc.Doms = domains

	// stateWriter := rpchelper.NewLatestStateWriter(txc, nil, context.BlockNumber-1)
	// statedb.CommitBlock(rules, stateWriter)

	// ============================================================
	// Multiple counter - read function
	static1 := append(hexutil.MustDecode("0xf07ec373"), leftPadBytes(sendersAddr[0].Bytes(), 32)...)
	static2 := append(hexutil.MustDecode("0xf07ec373"), leftPadBytes(sendersAddr[ADDRESS_NUM-1].Bytes(), 32)...)
	ret1, gas1, err1 := evm.StaticCall(vm.AccountRef(sendersAddr[0]), contractAddr, static1, 5000000)
	ret2, gas2, err2 := evm.StaticCall(vm.AccountRef(sendersAddr[0]), contractAddr, static2, 5000000)
	if err1 != nil || err2 != nil {
		t.Fatalf("failed to call contract (1): %v", err1)
		t.Fatalf("failed to call contract (2): %v", err2)
	} else {
		fmt.Println("View result 1:", hex.EncodeToString(ret1))
		fmt.Println("View result 2:", hex.EncodeToString(ret2))
		fmt.Println("Gas left 1:", gas1)
		fmt.Println("Gas left 2:", gas2)
	}
}
