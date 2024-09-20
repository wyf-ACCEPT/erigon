package parallel_tests

import (
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/common/hexutil"

	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/stages/mock"
)

func TestMultipleCounterRead(t *testing.T) {

	// ============================================================
	// Build evm context
	m := mock.Mock(t)
	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	// ============================================================
	// Generate addresses & allocate balance
	_, sendersAddr, _ := generateAccounts(ADDRESS_NUM)

	// r := rpchelper.NewLatestStateReader(tx)
	r := state.NewPlainStateReader(NewRedisDB())
	// cachedbreader, _ := LoadMapFromFile("./cachedb_data.gob")
	// r := state.NewPlainStateReader(cachedbreader)
	statedb := state.New(r)

	evm := vm.NewEVM(context, evmtypes.TxContext{
		GasPrice: uint256.NewInt((1)),
	}, statedb, params.AllProtocolChanges, vm.Config{})

	contractAddr := crypto.CreateAddress(sendersAddr[0], 1)

	timeStart := time.Now()
	for i := 0; i < ADDRESS_NUM; i += 1 {
		static := append(hexutil.MustDecode("0xf07ec373"), leftPadBytes(sendersAddr[i].Bytes(), 32)...)
		_, _, err := evm.StaticCall(vm.AccountRef(sendersAddr[0]), contractAddr, static, 5000000)
		if err != nil {
			t.Fatalf("failed to call contract: %v", err)
		}
	}
	fmt.Println("Duration for reading all counters: ", time.Since(timeStart))

	static0 := append(hexutil.MustDecode("0xf07ec373"), leftPadBytes(sendersAddr[0].Bytes(), 32)...)
	ret0, _, err0 := evm.StaticCall(vm.AccountRef(sendersAddr[0]), contractAddr, static0, 5000000)
	if err0 != nil {
		t.Fatalf("failed to call contract (0): %v", err0)
	} else {
		fmt.Printf("Counter for [%s]: %s\n", sendersAddr[0], hex.EncodeToString(ret0))
	}

}
