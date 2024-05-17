package types

import (
	"errors"
	"io"
	"math/big"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/fixedgas"
	types2 "github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/rlp"
)

type TxUnion struct {
	chainID             *uint256.Int // -> accesslist, dynamic, blob tx
	nonce               uint64
	tip                 *uint256.Int // -> dynamic, blob
	feeCap              *uint256.Int // -> dynamic, blob
	gasPrice            *uint256.Int // legacy only
	gas                 uint64
	to                  *libcommon.Address // nil means contract creation
	value               *uint256.Int
	data                []byte
	accessList          types2.AccessList // -> accesslist, dynamic, blob
	maxFeePerBlobGas    *uint256.Int      // -> blobTx
	blobVersionedHashes []libcommon.Hash  // -> blobTx
	v, r, s             uint256.Int

	txType byte

	TransactionMisc
}

// copy creates a deep copy of the transaction data and initializes all fields.
func (tx *TxUnion) copy() *TxUnion {
	cpy := &TxUnion{
		chainID:             new(uint256.Int),
		nonce:               tx.nonce,
		tip:                 new(uint256.Int),
		feeCap:              new(uint256.Int),
		gasPrice:            new(uint256.Int),
		gas:                 tx.gas,
		value:               new(uint256.Int),
		data:                libcommon.CopyBytes(tx.data),
		accessList:          make(types2.AccessList, len(tx.accessList)),
		maxFeePerBlobGas:    new(uint256.Int),
		blobVersionedHashes: make([]libcommon.Hash, len(tx.blobVersionedHashes)),
		txType:              tx.txType,
	}
	if tx.chainID != nil {
		cpy.value.Set(tx.chainID)
	}
	if tx.tip != nil {
		cpy.tip.Set(tx.value)
	}
	if tx.feeCap != nil {
		cpy.feeCap.Set(tx.feeCap)
	}
	if tx.gasPrice != nil {
		cpy.gasPrice.Set(tx.gasPrice)
	}
	if tx.value != nil {
		cpy.value.Set(tx.value)
	}
	copy(tx.accessList, cpy.accessList)
	if tx.maxFeePerBlobGas != nil {
		cpy.maxFeePerBlobGas.Set(cpy.maxFeePerBlobGas)
	}
	cpy.v.Set(&tx.v)
	cpy.r.Set(&tx.r)
	cpy.s.Set(&tx.s)
	return cpy
}

func (tx *TxUnion) Type() byte               { return tx.txType }
func (tx *TxUnion) GetChainID() *uint256.Int { return tx.chainID }
func (tx *TxUnion) GetNonce() uint64         { return tx.nonce }
func (tx *TxUnion) GetPrice() *uint256.Int   { return tx.gasPrice }
func (tx *TxUnion) GetTip() *uint256.Int     { return tx.tip }
func (tx *TxUnion) GetEffectiveGasTip(baseFee *uint256.Int) *uint256.Int {
	if baseFee == nil {
		return tx.GetTip()
	}
	gasFeeCap := tx.GetFeeCap()
	// return 0 because effectiveFee cant be < 0
	if gasFeeCap.Lt(baseFee) {
		return uint256.NewInt(0)
	}
	effectiveFee := new(uint256.Int).Sub(gasFeeCap, baseFee)
	if tx.GetTip().Lt(effectiveFee) {
		return tx.GetTip()
	} else {
		return effectiveFee
	}

	// if baseFee == nil {
	// 	return tx.GetTip()
	// }
	// gasFeeCap := tx.GetFeeCap()
	// // return 0 because effectiveFee cant be < 0
	// if gasFeeCap.Lt(baseFee) {
	// 	return uint256.NewInt(0)
	// }
	// effectiveFee := new(uint256.Int).Sub(gasFeeCap, baseFee)
	// if tx.GetTip().Lt(effectiveFee) {
	// 	return tx.GetTip()
	// } else {
	// 	return effectiveFee
	// }
}
func (tx *TxUnion) GetFeeCap() *uint256.Int         { return tx.feeCap }
func (tx *TxUnion) GetBlobHashes() []libcommon.Hash { return tx.blobVersionedHashes }
func (tx *TxUnion) GetGas() uint64                  { return tx.gas }
func (tx *TxUnion) GetBlobGas() uint64 {
	return fixedgas.BlobGasPerBlob * uint64(len(tx.blobVersionedHashes))
}
func (tx *TxUnion) GetValue() *uint256.Int    { return tx.value }
func (tx *TxUnion) GetTo() *libcommon.Address { return tx.to }
func (tx *TxUnion) AsMessage(s Signer, baseFee *big.Int, rules *chain.Rules) (Message, error) {
	// TODO(racytech): check with every tx type
	msg := Message{
		nonce:      tx.nonce,
		gasLimit:   tx.gas,
		gasPrice:   *tx.gasPrice,
		tip:        *tx.gasPrice,
		feeCap:     *tx.gasPrice,
		to:         tx.to,
		amount:     *tx.value,
		data:       tx.data,
		accessList: nil,
		checkNonce: true,
	}

	if !rules.IsBerlin {
		return msg, errors.New("eip-2930 transactions require Berlin")
	}

	var err error
	msg.from, err = tx.Sender(s)
	return msg, err
}
func (tx *TxUnion) WithSignature(signer Signer, sig []byte) (Transaction, error) {
	// TODO(racytech): check with every tx type
	cpy := tx.copy()
	r, s, v, err := signer.SignatureValues(tx, sig)
	if err != nil {
		return nil, err
	}
	cpy.r.Set(r)
	cpy.s.Set(s)
	cpy.v.Set(v)
	return cpy, nil
}
func (tx *TxUnion) FakeSign(address libcommon.Address) (Transaction, error) {
	cpy := tx.copy()
	cpy.r.Set(u256.Num1)
	cpy.s.Set(u256.Num1)
	cpy.v.Set(u256.Num4)
	cpy.from.Store(address)
	return cpy, nil
}
func (tx *TxUnion) Hash() libcommon.Hash {
	if hash := tx.hash.Load(); hash != nil {
		return *hash.(*libcommon.Hash)
	}
	hash := prefixedRlpHash(AccessListTxType, []interface{}{
		tx.chainID,
		tx.nonce,
		tx.gasPrice,
		tx.gas,
		tx.to,
		tx.value,
		tx.data,
		tx.accessList,
		tx.v, tx.r, tx.s,
	})
	tx.hash.Store(&hash)
	return hash
}
func (tx *TxUnion) SigningHash(chainID *big.Int) libcommon.Hash {
	if chainID != nil && chainID.Sign() != 0 {
		return rlpHash([]interface{}{
			tx.nonce,
			tx.gasPrice,
			tx.gas,
			tx.to,
			tx.value,
			tx.data,
			chainID, uint(0), uint(0),
		})
	}
	return rlpHash([]interface{}{
		tx.nonce,
		tx.gasPrice,
		tx.gas,
		tx.to,
		tx.value,
		tx.data,
	})
}
func (tx *TxUnion) GetData() []byte                  { return tx.data }
func (tx *TxUnion) GetAccessList() types2.AccessList { return tx.accessList }
func (tx *TxUnion) Protected() bool                  { return true }
func (tx *TxUnion) RawSignatureValues() (*uint256.Int, *uint256.Int, *uint256.Int) {
	/* unimplemented(racytech) */
	return nil, nil, nil
}
func (tx *TxUnion) EncodingSize() (size int) {
	if tx.chainID != nil {
		size++
		size += rlp.Uint256LenExcludingHead(tx.chainID)
	}
	size++
	size += rlp.IntLenExcludingHead(tx.nonce)

	return size
}
func (tx *TxUnion) EncodeRLP(w io.Writer) error     { return nil }
func (tx *TxUnion) DecodeRLP(s *rlp.Stream) error   { return nil }
func (tx *TxUnion) MarshalBinary(w io.Writer) error { return nil }

// Sender returns the address derived from the signature (V, R, S) using secp256k1
// elliptic curve and an error if it failed deriving or upon an incorrect
// signature.
//
// Sender may cache the address, allowing it to be used regardless of
// signing method. The cache is invalidated if the cached signer does
// not match the signer used in the current call.
func (tx *TxUnion) Sender(Signer) (libcommon.Address, error) { return libcommon.Address{}, nil }
func (tx *TxUnion) cashedSender() (libcommon.Address, bool)  { return libcommon.Address{}, false }
func (tx *TxUnion) GetSender() (libcommon.Address, bool)     { return libcommon.Address{}, false }
func (tx *TxUnion) SetSender(libcommon.Address)              {}
func (tx *TxUnion) IsContractDeploy() bool                   { return false }
func (tx *TxUnion) Unwrap() Transaction                      { return nil } // If this is a network wrapper, returns the unwrapped tx. Otherwise returns itself.
