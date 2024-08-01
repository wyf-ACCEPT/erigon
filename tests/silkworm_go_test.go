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

func setup(t *testing.T) *silkworm.Silkworm {
	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))
	dirs := datadir.New(t.TempDir())
	num_contexts := 1
	log_level := log.LvlInfo

	silkworm, err := silkworm.New(dirs.DataDir, mdbx.Version(), uint32(num_contexts), log_level)
	require.NoError(t, err)

	return silkworm
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

func TestSilkwormInitialization(t *testing.T) {
	silkworm := setup(t)
	require.NotNil(t, silkworm)
}

func TestSilkwormForkValidatorInitialization(t *testing.T) {
	silkwormInstance := setup(t)
	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	dirs := datadir.New(t.TempDir())
	db := mdbx2.NewMDBX(log.New()).Path(dirs.DataDir).Exclusive().MustOpen()

	err := silkwormInstance.StartForkValidator(db.CHandle())
	require.NoError(t, err)
}

func TestSilkwormForkValidatorTermination(t *testing.T) {
	silkwormInstance := setup(t)
	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	dirs := datadir.New(t.TempDir())

	db := mdbx2.NewMDBX(log.New()).Path(dirs.DataDir).MustOpen()

	err := silkwormInstance.StartForkValidator(db.CHandle())
	require.NoError(t, err)
}

func TestSilkwormInsertBlock(t *testing.T) {
	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))
	dirs := datadir.New(t.TempDir())

	db := mdbx2.NewMDBX(log.New()).Path(dirs.DataDir).Exclusive().InMem(dirs.DataDir).Label(kv.ChainDB).MustOpen()

	network := "mainnet"
	logger := log.New()

	genesis := core.GenesisBlockByChainName(network)
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	_, block, err := core.WriteGenesisBlock(tx, genesis, nil, "", logger)
	require.NoError(t, err)
	expect := params.GenesisHashByChainName(network)
	require.NotNil(t, expect, network)
	require.EqualValues(t, block.Hash(), *expect, network)

	canon_zero, _ := rawdb.ReadCanonicalHash(tx, 0)
	chain_conf, _ := rawdb.ReadChainConfig(tx, canon_zero)

	require.NotNil(t, chain_conf, network)

	newBlock := SampleBlock(block.Header())
	hh := newBlock.Header().Hash()

	err = rawdb.WriteBlock(tx, newBlock)
	require.NoError(t, err)
	tx.Commit()

	logger.Info("Starting silkworm fork validator    ")

	silkwormInstance := setup(t)
	err2 := silkwormInstance.StartForkValidator(db.CHandle())
	require.NoError(t, err2)

	logger.Info("Verifying chain for block", "hash", hh)
	err3 := silkwormInstance.VerifyChain(silkworm_go.Hash(hh))
	require.NoError(t, err3)
}
