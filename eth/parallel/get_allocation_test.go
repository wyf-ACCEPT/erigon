package parallel_tests

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"strconv"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/tests"
	"github.com/ledgerwatch/erigon/turbo/stages/mock"
	"github.com/stretchr/testify/require"
)

type BlockState struct {
	BlockNumber uint64                  `json:"blockNumber"`
	Pre         map[string]AccountState `json:"pre"`
	Post        map[string]AccountState `json:"post"`
}

type AccountState struct {
	Nonce    *string            `json:"nonce,omitempty"`
	CodeHash *string            `json:"codeHash,omitempty"`
	Code     *string            `json:"code,omitempty"`
	Balance  *string            `json:"balance,omitempty"`
	Storage  *map[string]string `json:"storage,omitempty"`
}

func (a *AccountState) SetDefaults() {
	if a.Nonce == nil {
		nonce := "0"
		a.Nonce = &nonce
	}
	if a.Code == nil {
		code := ""
		a.Code = &code
	}
	if a.Balance == nil {
		balance := "0"
		a.Balance = &balance
	}
	if a.Storage == nil {
		storage := map[string]string{}
		a.Storage = &storage
	}
}

func GetAllocation(block BlockState) (*types.GenesisAlloc, error) {
	alloc := types.GenesisAlloc{}
	for addressHex, accountState := range block.Pre {
		accountState.SetDefaults()
		address := common.HexToAddress(addressHex)
		nonce, err1 := strconv.ParseInt(*accountState.Nonce, 16, 64)
		code, err2 := hex.DecodeString(*accountState.Code)
		balanceBigint := new(big.Int)
		balance, ok3 := balanceBigint.SetString(*accountState.Balance, 16)
		if err1 != nil || err2 != nil || ok3 == false {
			return nil, fmt.Errorf("failed to parse data: [1]%v [2]%v [3]%v", err1, err2, ok3)
		}

		storage := map[common.Hash]common.Hash{}
		for key, value := range *accountState.Storage {
			keyBytes, err4 := hex.DecodeString(key[2:]) // With '0x' prefix
			valueBytes, err5 := hex.DecodeString(value) // Without '0x' prefix
			if err4 != nil || err5 != nil {
				return nil, fmt.Errorf("failed to parse storage: [4]%v [5]%v", err4, err5)
			}
			storage[common.BytesToHash(keyBytes)] = common.BytesToHash(valueBytes)
		}

		alloc[address] = types.GenesisAccount{
			Nonce:   uint64(nonce),
			Code:    code,
			Balance: balance,
			Storage: storage,
		}
	}
	return &alloc, nil
}

func TestGetAllocation(t *testing.T) {

	// ============================================================
	// Build evm context
	m := mock.Mock(t)
	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	// ============================================================
	// Read file and decode json
	// file, err := os.Open("/chaindata/statedata/block18500000-18500999.json")
	file, err := os.Open("/home/ubuntu/pland/erigon/eth/parallel/sample_block.json")
	if err != nil {
		t.Fatalf("failed to load file: %v", err)
	}
	defer file.Close()
	var blocks []BlockState

	fmt.Println("[info] decoding json, might take a while...")
	decoder := json.NewDecoder(file)
	if err = decoder.Decode(&blocks); err != nil {
		t.Fatalf("failed to decode json: %v", err)
	} else {
		fmt.Println("[info] decode success")
	}

	// ============================================================
	// Allocate storage
	alloc, err := GetAllocation(blocks[3])
	if err != nil {
		t.Fatalf("failed to get allocation: %v", err)
	}

	statedb, _ := tests.MakePreState(rules, tx, *alloc, context.BlockNumber)
	evm := vm.NewEVM(context, evmtypes.TxContext{
		GasPrice: uint256.NewInt((1)),
	}, statedb, params.AllProtocolChanges, vm.Config{})

	fmt.Println(alloc)
	fmt.Println(evm)

}
