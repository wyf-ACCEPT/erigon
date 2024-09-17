package parallel_tests

import (
	"crypto/ecdsa"
	"crypto/rand"
	"math/big"
	"testing"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/crypto"
	"github.com/erigontech/erigon/params"
	"github.com/holiman/uint256"
)

var context = evmtypes.BlockContext{
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
var signer = types.LatestSignerForChainID(big.NewInt(1))
var rules = params.AllProtocolChanges.Rules(context.BlockNumber, context.Time)

func generateAccounts(count int) ([]*ecdsa.PrivateKey, []libcommon.Address, error) {
	privateKeys := make([]*ecdsa.PrivateKey, count)
	addresses := make([]libcommon.Address, count)
	for i := 0; i < count; i++ {
		pk, err := ecdsa.GenerateKey(crypto.S256(), rand.Reader)
		if err != nil {
			return nil, nil, err
		}
		publicKey := pk.PublicKey
		address := crypto.PubkeyToAddress(publicKey)
		addresses[i] = address
		privateKeys[i] = pk
	}
	return privateKeys, addresses, nil
}

func sendCreate(
	deployer libcommon.Address, nonce uint64, bytecode []byte,
	deployerPk *ecdsa.PrivateKey, evm *vm.EVM, t *testing.T,
) (libcommon.Address, error) {
	contractAddr := crypto.CreateAddress(deployer, nonce)
	unsignedTx := types.NewContractCreation(
		1, uint256.NewInt(0), 5000000, uint256.NewInt(1), bytecode,
	)
	txn, _ := types.SignTx(unsignedTx, *signer, deployerPk)
	msg, _ := txn.AsMessage(*signer, nil, rules)
	evm.TxContext.Origin = deployer
	st := core.NewStateTransition(
		evm, msg, new(core.GasPool).AddGas(txn.GetGas()).AddBlobGas(txn.GetBlobGas()),
	)
	_, err := st.TransitionDb(false, false)
	return contractAddr, err
}

func sendTransaction(
	from libcommon.Address, to libcommon.Address, value *uint256.Int, nonce uint64, data []byte,
	fromPk *ecdsa.PrivateKey, evm *vm.EVM, t *testing.T,
) error {
	unsignedTx := types.NewTransaction(
		nonce, to, uint256.NewInt(0), 5000000, uint256.NewInt(0), data,
	)
	txn, _ := types.SignTx(unsignedTx, *signer, fromPk)
	msg, _ := txn.AsMessage(*signer, nil, rules)
	evm.TxContext.Origin = from

	st := core.NewStateTransition(
		evm, msg, new(core.GasPool).AddGas(txn.GetGas()).AddBlobGas(txn.GetBlobGas()),
	)
	_, err := st.TransitionDb(false, false)
	return err
}

func leftPadBytes(slice []byte, length int) []byte {
	if len(slice) >= length {
		return slice
	}
	padded := make([]byte, length)
	copy(padded[length-len(slice):], slice)
	return padded
}
