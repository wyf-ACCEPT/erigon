package main

import (
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/common/hexutil"
)

type XYZ struct {
	Balance *hexutil.Big `json:"balance"`
}

func main() {
	//printBigToHex("21481514137811887511", "0x5124fcc2b3f99f571ad67d075643c743f38f1c34 - pre ")
	//printBigToHex("21481515790486643551", "0x5124fcc2b3f99f571ad67d075643c743f38f1c34 - post")
	//println()
	printBigToHex("1797000000000000000", "0xa7613D72e7E0C4f9d58A24e40D47449A86C6ac4C - pre - expected")
	printHexToBig("0x8fb2dbe636505f6", "0xa7613D72e7E0C4f9d58A24e40D47449A86C6ac4C - post - in test")
	//             647161267192399350
	printBigToHex("687838999279300930", "0xa7613D72e7E0C4f9d58A24e40D47449A86C6ac4C - post - expected")
	printBigToHex("21481515790486643551", "0x5124fcC2B3F99F571AD67D075643C743F38f1C34 - post - expected")

	//"0x5124fcc2b3f99f571ad67d075643c743f38f1c34": {
	//	"balance": "0x12a1dab4b5dcf2597",
	//		"nonce": 40990
	//},
	printHexToBig("0x12a1dab4b5dcf2597", "0x5124fcc2b3f99f571ad67d075643c743f38f1c34 - post - expected")
	//"0xa7613d72e7e0c4f9d58a24e40d47449a86c6ac4c": {
	//	"balance": "0x26d0f0ae4cc48000"
	//}
	printHexToBig("0x26d0f0ae4cc48000", "0xa7613d72e7e0c4f9d58a24e40d47449a86c6ac4c - post - expected")

	printHexToBig("0x12a1daccc28e6a35f", "0x5124fcc2b3f99f571ad67d075643c743f38f1c34 - post - in test")

	printHexToBig("0x1294400ef3db5e928", "balance check")

	printHexToBig("0x2cdb96c56db040b43", "0x808b4da0be6c9512e948521452227efc619bea52 - pre")
	printHexToBig("0x2cd987071ba2346b6", "0x808b4da0be6c9512e948521452227efc619bea52 - post - in test")
	printHexToBig("0x2cd72a36dd031f089", "0x808b4da0be6c9512e948521452227efc619bea52 - post - expected")

	printBigToHex("51717486916183264067", "0x808b4dA0Be6c9512E948521452227EFc619BeA52 - pre - REAL")
	printBigToHex("51709147410379106417", "0x808b4dA0Be6c9512E948521452227EFc619BeA52 - post - REAL")
}

func printHexToBig(hex string, printTag string) {
	payload := fmt.Sprintf(`{"balance":"%s"}`, hex)
	var xyz XYZ
	err := json.Unmarshal([]byte(payload), &xyz)
	if err != nil {
		panic(err)
	}

	println(fmt.Sprintf("%s: %s -> %s", printTag, payload, xyz.Balance.ToInt().String()))
}

func printBigToHex(s string, printTag string) {
	bal, ok := new(big.Int).SetString(s, 10)
	if !ok {
		panic("failed to set big.Int")
	}

	xyz := XYZ{
		Balance: (*hexutil.Big)(bal),
	}

	res, err := json.Marshal(xyz)
	if err != nil {
		panic(err)
	}

	println(fmt.Sprintf("%s: %s -> %s", printTag, s, string(res)))
}
