package parallel_tests

import (
	"crypto/ecdsa"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
)

var ADDRESS_NUM = 10000
var BATCH_SIZE = 200

var excessBlobGas = uint64(50000)
var context = evmtypes.BlockContext{
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
var signer = types.LatestSignerForChainID(big.NewInt(1))
var rules = params.AllProtocolChanges.Rules(context.BlockNumber, context.Time)

type deterministicReader struct {
	currentNumber int
}

func (r *deterministicReader) Read(p []byte) (n int, err error) {
	for i := range p {
		p[i] = byte(r.currentNumber >> (i * 8) & 0xff)
	}
	return len(p), nil
}

func newDeterministicReader(start int) *deterministicReader {
	return &deterministicReader{currentNumber: start}
}

func generateAccounts(count int) ([]*ecdsa.PrivateKey, []libcommon.Address, error) {
	privateKeys := make([]*ecdsa.PrivateKey, count)
	addresses := make([]libcommon.Address, count)
	for i := 0; i < count; i++ {
		pk, err := ecdsa.GenerateKey(
			crypto.S256(), newDeterministicReader(i+0xdeadbeef),
		)
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

func logMeanAndStd(values []int64) {
	var sum int64
	var variance int64
	for _, value := range values {
		sum += value
	}
	mean := sum / int64(len(values))
	for _, value := range values {
		variance += (value - mean) * (value - mean)
	}
	std := math.Sqrt(float64(variance) / float64(len(values)))
	fmt.Printf(
		"Average duration in %d runs: %v ± %v\n",
		len(values), time.Duration(mean), time.Duration(std),
	)
}

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
