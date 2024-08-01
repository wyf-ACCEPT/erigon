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

package tests

import (
	"context"
	"math/big"
	"testing"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/params"

	mdbx2 "github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon/turbo/silkworm"
	"github.com/erigontech/mdbx-go/mdbx"
	silkworm_go "github.com/erigontech/silkworm-go"

	"github.com/erigontech/erigon/core/rawdb"
)

func setup(t *testing.T) (*silkworm.Silkworm, kv.RwDB, *types.Block) {
	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))
	dirs := datadir.New(t.TempDir())
	num_contexts := 1
	log_level := log.LvlInfo

	silkworm, err := silkworm.New(dirs.DataDir, mdbx.Version(), uint32(num_contexts), log_level)
	require.NoError(t, err)

	db := mdbx2.NewMDBX(log.New()).Path(dirs.DataDir).Exclusive().InMem(dirs.DataDir).Label(kv.ChainDB).MustOpen()

	network := "mainnet"

	genesis := core.GenesisBlockByChainName(network)
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	_, genesisBlock, err := core.WriteGenesisBlock(tx, genesis, nil, dirs, log.New())
	require.NoError(t, err)
	expect := params.GenesisHashByChainName(network)
	require.NotNil(t, expect, network)
	require.EqualValues(t, genesisBlock.Hash(), *expect, network)
	tx.Commit()

	return silkworm, db, genesisBlock
}

func SampleBlock(parent *types.Header) *types.Block {
	return types.NewBlockWithHeader(&types.Header{
		Number:     new(big.Int).Add(parent.Number, big.NewInt(1)),
		Difficulty: new(big.Int).Add(parent.Number, big.NewInt(17000000000)),
		ParentHash: parent.Hash(),
		//Beneficiary: crypto.PubkeyToAddress(crypto.MustGenerateKey().PublicKey),
		TxHash:      types.EmptyRootHash,
		ReceiptHash: types.EmptyRootHash,
		GasLimit:    10000000,
		GasUsed:     0,
		Time:        parent.Time + 12,
	})
}

func ForkValidatorTestSettings() silkworm_go.ForkValidatorSettings {
	return silkworm_go.ForkValidatorSettings{
		BatchSize:               512 * 1024 * 1024,
		EtlBufferSize:           256 * 1024 * 1024,
		SyncLoopThrottleSeconds: 0,
		StopBeforeSendersStage:  true,
	}
}

func TestSilkwormInitialization(t *testing.T) {
	silkworm, _, _ := setup(t)
	require.NotNil(t, silkworm)
}

func TestSilkwormForkValidatorInitialization(t *testing.T) {
	silkwormInstance, db, _ := setup(t)

	err := silkwormInstance.StartForkValidator(db.CHandle(), ForkValidatorTestSettings())
	require.NoError(t, err)
}

func TestSilkwormForkValidatorTermination(t *testing.T) {
	silkwormInstance, db, _ := setup(t)

	err := silkwormInstance.StartForkValidator(db.CHandle(), ForkValidatorTestSettings())
	require.NoError(t, err)

	err = silkwormInstance.StopForkValidator()
	require.NoError(t, err)
}

func TestSilkwormVerifyChainSingleBlock(t *testing.T) {
	silkwormInstance, db, genesisBlock := setup(t)

	tx, err := db.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	newBlock := SampleBlock(genesisBlock.Header())

	err = rawdb.WriteBlock(tx, newBlock)
	require.NoError(t, err)
	tx.Commit()

	err2 := silkwormInstance.StartForkValidator(db.CHandle(), ForkValidatorTestSettings())
	require.NoError(t, err2)

	err3 := silkwormInstance.VerifyChain(silkworm_go.Hash(newBlock.Header().Hash()))
	require.NoError(t, err3)
}

func TestSilkwormForkChoiceUpdateSingleBlock(t *testing.T) {
	silkwormInstance, db, genesisBlock := setup(t)

	tx, err := db.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	newBlock := SampleBlock(genesisBlock.Header())

	err = rawdb.WriteBlock(tx, newBlock)
	require.NoError(t, err)
	tx.Commit()

	err = silkwormInstance.StartForkValidator(db.CHandle(), ForkValidatorTestSettings())
	require.NoError(t, err)

	err = silkwormInstance.VerifyChain(silkworm_go.Hash(newBlock.Header().Hash()))
	require.NoError(t, err)

	err = silkwormInstance.ForkChoiceUpdate(silkworm_go.Hash(newBlock.Header().Hash()), silkworm_go.Hash{}, silkworm_go.Hash{})
	require.NoError(t, err)
}

func TestSilkwormVerifyChainTwoBlocks(t *testing.T) {
	silkwormInstance, db, genesisBlock := setup(t)

	tx, err := db.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	newBlock1 := SampleBlock(genesisBlock.Header())
	err = rawdb.WriteBlock(tx, newBlock1)
	require.NoError(t, err)

	newBlock2 := SampleBlock(newBlock1.Header())
	err = rawdb.WriteBlock(tx, newBlock2)
	require.NoError(t, err)

	tx.Commit()

	err = silkwormInstance.StartForkValidator(db.CHandle(), ForkValidatorTestSettings())
	require.NoError(t, err)

	err = silkwormInstance.VerifyChain(silkworm_go.Hash(newBlock2.Header().Hash()))
	require.NoError(t, err)

	err = silkwormInstance.VerifyChain(silkworm_go.Hash(newBlock1.Header().Hash()))
	require.NoError(t, err)
}

func TestSilkwormVerifyTwoChains(t *testing.T) {
	silkwormInstance, db, genesisBlock := setup(t)

	tx, err := db.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	newBlock1 := SampleBlock(genesisBlock.Header())
	err = rawdb.WriteBlock(tx, newBlock1)
	require.NoError(t, err)

	newBlock2 := SampleBlock(genesisBlock.Header())
	err = rawdb.WriteBlock(tx, newBlock2)
	require.NoError(t, err)

	tx.Commit()

	err = silkwormInstance.StartForkValidator(db.CHandle(), ForkValidatorTestSettings())
	require.NoError(t, err)

	err = silkwormInstance.VerifyChain(silkworm_go.Hash(newBlock1.Header().Hash()))
	require.NoError(t, err)

	err = silkwormInstance.VerifyChain(silkworm_go.Hash(newBlock2.Header().Hash()))
	require.NoError(t, err)
}

func TestSilkwormForkChoiceUpdateTwoChains(t *testing.T) {
	silkwormInstance, db, genesisBlock := setup(t)

	tx, err := db.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	newBlock1 := SampleBlock(genesisBlock.Header())
	err = rawdb.WriteBlock(tx, newBlock1)
	require.NoError(t, err)

	newBlock2 := SampleBlock(genesisBlock.Header())
	err = rawdb.WriteBlock(tx, newBlock2)
	require.NoError(t, err)

	tx.Commit()

	err = silkwormInstance.StartForkValidator(db.CHandle(), ForkValidatorTestSettings())
	require.NoError(t, err)

	err = silkwormInstance.VerifyChain(silkworm_go.Hash(newBlock1.Header().Hash()))
	require.NoError(t, err)

	err = silkwormInstance.ForkChoiceUpdate(silkworm_go.Hash(newBlock2.Header().Hash()), silkworm_go.Hash(newBlock2.Header().Hash()), silkworm_go.Hash(newBlock2.Header().Hash()))
	require.NoError(t, err)
}
