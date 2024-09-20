package parallel_tests

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"

	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
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

	ADDRESS_NUM := 10000
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

	// alloc := types.GenesisAlloc{}
	// for i := 0; i < ADDRESS_NUM; i++ {
	// 	alloc[sendersAddr[i]] = types.GenesisAccount{
	// 		Nonce:   1,
	// 		Code:    []byte{},
	// 		Balance: big.NewInt(500000000000000),
	// 	}
	// }

	// statedb, _ := tests.MakePreState(rules, tx, alloc, context.BlockNumber)

	timeStart := time.Now()
	r := state.NewPlainStateReader(NewRedisDB())
	// r := rpchelper.NewLatestStateReader(tx)
	statedb := state.New(r)

	evm := vm.NewEVM(context, evmtypes.TxContext{
		GasPrice: uint256.NewInt((1)),
	}, statedb, params.AllProtocolChanges, vm.Config{})

	contractAddrTmp := crypto.CreateAddress(sendersAddr[0], 1)

	for i := 0; i < ADDRESS_NUM; i += 1 {
		static := append(hexutil.MustDecode("0xf07ec373"), leftPadBytes(sendersAddr[i].Bytes(), 32)...)
		_, _, err := evm.StaticCall(vm.AccountRef(sendersAddr[0]), contractAddrTmp, static, 5000000)
		if err != nil {
			t.Fatalf("failed to call contract: %v", err)
		}
	}
	fmt.Println("Duration for reading all counters: ", time.Since(timeStart))

	// static0 := append(hexutil.MustDecode("0xf07ec373"), leftPadBytes(sendersAddr[0].Bytes(), 32)...)
	// ret0, _, err0 := evm.StaticCall(vm.AccountRef(sendersAddr[0]), contractAddrTmp, static0, 5000000)
	// if err0 != nil {
	// 	t.Fatalf("failed to call contract (0): %v", err0)
	// } else {
	// 	fmt.Printf("Counter for [%s]: %s\n", sendersAddr[0], hex.EncodeToString(ret0))
	// }

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

	// ============================================================
	// Duration test for commit block
	fmt.Println("Current block number: ", context.BlockNumber)
	stateWriter := rpchelper.NewLatestStateWriter(tx, context.BlockNumber+1)
	statedb.CommitBlock(rules, stateWriter)
	statedb.FinalizeTx(rules, stateWriter)
	tx.Commit()

	fmt.Println("Current block number: ", context.BlockNumber)

	stateWriterRedis := state.NewPlainStateWriterNoHistory(NewRedisDB())
	statedb.CommitBlock(rules, stateWriterRedis)

	// for name, stateWriter := range map[string]state.StateWriter{
	// 	"Plain State Writer":            rpchelper.NewLatestStateWriter(tx, context.BlockNumber+1),
	// 	"Plain State Writer no history": state.NewPlainStateWriterNoHistory(tx),
	// 	// "Redis State Writer":            state.NewPlainStateWriterNoHistory(NewRedisDB()),
	// } {
	// 	fmt.Printf("\n========== %s ==========\n", name)
	// 	durationList := make([]int64, 10)
	// 	for i := 0; i < 10; i += 1 {
	// 		timeStart := time.Now()
	// 		statedb.CommitBlock(rules, stateWriter)
	// 		statedb.FinalizeTx(rules, stateWriter)
	// 		tx.Commit()
	// 		duration := time.Since(timeStart)
	// 		durationList[i] = int64(duration)
	// 	}
	// 	logMeanAndStd(durationList)
	// }

	// ============================================================
	// Multiple counter - read function
	static1 := append(hexutil.MustDecode("0xf07ec373"), leftPadBytes(sendersAddr[0].Bytes(), 32)...)
	static2 := append(hexutil.MustDecode("0xf07ec373"), leftPadBytes(sendersAddr[ADDRESS_NUM-1].Bytes(), 32)...)
	ret1, _, err1 := evm.StaticCall(vm.AccountRef(sendersAddr[0]), contractAddr, static1, 5000000)
	ret2, _, err2 := evm.StaticCall(vm.AccountRef(sendersAddr[0]), contractAddr, static2, 5000000)
	if err1 != nil || err2 != nil {
		t.Fatalf("failed to call contract (1): %v", err1)
		t.Fatalf("failed to call contract (2): %v", err2)
	} else {
		fmt.Printf("Counter for [%s]: %s\n", sendersAddr[0], hex.EncodeToString(ret1))
		fmt.Printf("Counter for [%s]: %s\n", sendersAddr[ADDRESS_NUM-1], hex.EncodeToString(ret2))
	}
}
