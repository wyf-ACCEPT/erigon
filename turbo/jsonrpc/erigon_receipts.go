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

package jsonrpc

import (
	"context"
	"fmt"

	"github.com/RoaringBitmap/roaring"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/state/exec3"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethutils"
	"github.com/erigontech/erigon/eth/filters"
	bortypes "github.com/erigontech/erigon/polygon/bor/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/turbo/rpchelper"
)

// GetLogsByHash implements erigon_getLogsByHash. Returns an array of arrays of logs generated by the transactions in the block given by the block's hash.
func (api *ErigonImpl) GetLogsByHash(ctx context.Context, hash common.Hash) ([][]*types.Log, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	block, err := api.blockByHashWithSenders(ctx, tx, hash)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	receipts, err := api.getReceipts(ctx, tx, block, block.Body().SendersFromTxs())
	if err != nil {
		return nil, fmt.Errorf("getReceipts error: %w", err)
	}

	logs := make([][]*types.Log, len(receipts))
	for i, receipt := range receipts {
		logs[i] = receipt.Logs
	}
	return logs, nil
}

// GetLogs implements erigon_getLogs. Returns an array of logs matching a given filter object.
func (api *ErigonImpl) GetLogs(ctx context.Context, crit filters.FilterCriteria) (types.ErigonLogs, error) {
	var begin, end uint64
	erigonLogs := types.ErigonLogs{}

	tx, beginErr := api.db.BeginRo(ctx)
	if beginErr != nil {
		return erigonLogs, beginErr
	}
	defer tx.Rollback()

	if crit.BlockHash != nil {
		header, err := api._blockReader.HeaderByHash(ctx, tx, *crit.BlockHash)
		if header == nil {
			return nil, err
		}
		begin = header.Number.Uint64()
		end = header.Number.Uint64()

	} else {
		// Convert the RPC block numbers into internal representations
		latest, err := rpchelper.GetLatestBlockNumber(tx)
		if err != nil {
			return nil, err
		}

		begin = 0
		if crit.FromBlock != nil {
			if crit.FromBlock.Sign() >= 0 {
				begin = crit.FromBlock.Uint64()
			} else if !crit.FromBlock.IsInt64() || crit.FromBlock.Int64() != int64(rpc.LatestBlockNumber) {
				return nil, fmt.Errorf("negative value for FromBlock: %v", crit.FromBlock)
			}
		}
		end = latest
		if crit.ToBlock != nil {
			if crit.ToBlock.Sign() >= 0 {
				end = crit.ToBlock.Uint64()
			} else if !crit.ToBlock.IsInt64() || crit.ToBlock.Int64() != int64(rpc.LatestBlockNumber) {
				return nil, fmt.Errorf("negative value for ToBlock: %v", crit.ToBlock)
			}
		}
	}
	if end < begin {
		return nil, fmt.Errorf("end (%d) < begin (%d)", end, begin)
	}
	if end > roaring.MaxUint32 {
		return nil, fmt.Errorf("end (%d) > MaxUint32", end)
	}

	return api.getLogsV3(ctx, tx.(kv.TemporalTx), begin, end, crit)
}

// GetLatestLogs implements erigon_getLatestLogs.
// Return specific number of logs or block matching a give filter objects by descend.
// IgnoreTopicsOrder option provide a way to match the logs with addresses and topics without caring about the topics' orders
// When IgnoreTopicsOrde option is true, once the logs have a topic that matched, it will be returned no matter what topic position it is in.
//
// blockCount parameter is for better pagination.
// `crit` filter is the same filter.
//
// Examples:
// {} or nil          matches any topics list
// {{A}}              matches topic A in any positions. Logs with {{B}, {A}} will be matched
func (api *ErigonImpl) GetLatestLogs(ctx context.Context, crit filters.FilterCriteria, logOptions filters.LogFilterOptions) (types.ErigonLogs, error) {
	if logOptions.LogCount != 0 && logOptions.BlockCount != 0 {
		return nil, fmt.Errorf("logs count & block count are ambigious")
	}
	if logOptions.LogCount == 0 && logOptions.BlockCount == 0 {
		logOptions = filters.DefaultLogFilterOptions()
	}
	erigonLogs := types.ErigonLogs{}
	dbTx, beginErr := api.db.BeginRo(ctx)
	if beginErr != nil {
		return erigonLogs, beginErr
	}
	defer dbTx.Rollback()

	tx := dbTx.(kv.TemporalTx)

	var err error
	var begin, end uint64 // Filter range: begin-end(from-to). Two limits are included in the filter

	if crit.BlockHash != nil {
		header, err := api._blockReader.HeaderByHash(ctx, tx, *crit.BlockHash)
		if err != nil {
			return nil, err
		}
		if header == nil {
			return nil, fmt.Errorf("block header not found %x", *crit.BlockHash)
		}
		begin = header.Number.Uint64()
		end = header.Number.Uint64()
	} else {
		// Convert the RPC block numbers into internal representations
		latest, err := rpchelper.GetLatestBlockNumber(tx)
		if err != nil {
			return nil, err
		}

		begin = 0
		if crit.FromBlock != nil {
			if crit.FromBlock.Sign() >= 0 {
				begin = crit.FromBlock.Uint64()
			} else if !crit.FromBlock.IsInt64() || crit.FromBlock.Int64() != int64(rpc.LatestBlockNumber) {
				return nil, fmt.Errorf("negative value for FromBlock: %v", crit.FromBlock)
			}
		}
		end = latest
		if crit.ToBlock != nil {
			if crit.ToBlock.Sign() >= 0 {
				end = crit.ToBlock.Uint64()
			} else if !crit.ToBlock.IsInt64() || crit.ToBlock.Int64() != int64(rpc.LatestBlockNumber) {
				return nil, fmt.Errorf("negative value for ToBlock: %v", crit.ToBlock)
			}
		}
	}
	if end < begin {
		return nil, fmt.Errorf("end (%d) < begin (%d)", end, begin)
	}

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	exec := exec3.NewTraceWorker(tx, chainConfig, api.engine(), api._blockReader, nil)

	txNumbers, err := applyFiltersV3(tx, begin, end, crit)
	if err != nil {
		return erigonLogs, err
	}

	addrMap := make(map[common.Address]struct{}, len(crit.Addresses))
	for _, v := range crit.Addresses {
		addrMap[v] = struct{}{}
	}
	topicsMap := make(map[common.Hash]struct{})
	for i := range crit.Topics {
		for j := range crit.Topics[i] {
			topicsMap[crit.Topics[i][j]] = struct{}{}
		}
	}

	// latest logs that match the filter crit
	it := rawdbv3.TxNums2BlockNums(tx, txNumbers, order.Asc)
	defer it.Close()

	var blockHash common.Hash
	var header *types.Header
	var logCount, blockCount, timestamp uint64

	for it.HasNext() {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		txNum, blockNum, txIndex, isFinalTxn, blockNumChanged, err := it.Next()
		if err != nil {
			return nil, err
		}
		if isFinalTxn {
			continue
		}

		// if block number changed, calculate all related field
		if blockNumChanged {
			if header, err = api._blockReader.HeaderByNumber(ctx, tx, blockNum); err != nil {
				return nil, err
			}
			if header == nil {
				log.Warn("[rpc] header is nil", "blockNum", blockNum)
				continue
			}
			blockHash = header.Hash()
			exec.ChangeBlock(header)
			timestamp = header.Time
		}
		var logIndex uint
		var blockLogs types.Logs

		txn, err := api._txnReader.TxnByIdxInBlock(ctx, tx, blockNum, txIndex)
		if err != nil {
			return nil, err
		}
		if txn == nil {
			continue
		}

		_, err = exec.ExecTxn(txNum, txIndex, txn)
		if err != nil {
			return nil, err
		}
		blockLogs = exec.GetLogs(txIndex, txn)
		for _, log := range blockLogs {
			log.Index = logIndex
			logIndex++
		}
		var filtered types.Logs
		var maxLogCount uint64
		maxLogCount = 0
		if logOptions.LogCount != 0 {
			maxLogCount = logOptions.LogCount - logCount
		}
		if logOptions.IgnoreTopicsOrder {
			filtered = blockLogs.CointainTopics(addrMap, topicsMap, maxLogCount)
		} else {
			filtered = blockLogs.Filter(addrMap, crit.Topics, maxLogCount)
		}
		if len(filtered) == 0 {
			continue
		}
		for i := range filtered {
			filtered[i].TxIndex = uint(txIndex)
			logCount++
		}

		blockCount++

		if len(blockLogs) == 0 {
			continue
		}

		body, err := api._blockReader.BodyWithTransactions(ctx, tx, blockHash, blockNum)
		if err != nil {
			return nil, err
		}
		if body == nil {
			return nil, fmt.Errorf("block not found %d", blockNum)
		}
		for _, log := range filtered {
			erigonLog := &types.ErigonLog{}
			erigonLog.BlockNumber = blockNum
			erigonLog.BlockHash = blockHash
			if log.TxIndex == uint(len(body.Transactions)) {
				erigonLog.TxHash = bortypes.ComputeBorTxHash(blockNum, blockHash)
			} else {
				erigonLog.TxHash = body.Transactions[log.TxIndex].Hash()
			}
			erigonLog.Timestamp = timestamp
			erigonLog.Address = log.Address
			erigonLog.Topics = log.Topics
			erigonLog.Data = log.Data
			erigonLog.Index = log.Index
			erigonLog.TxIndex = log.TxIndex
			erigonLog.Removed = log.Removed
			erigonLogs = append(erigonLogs, erigonLog)
		}

		if logOptions.LogCount != 0 && logOptions.LogCount <= logCount {
			return erigonLogs, nil
		}
		if logOptions.BlockCount != 0 && logOptions.BlockCount <= blockCount {
			return erigonLogs, nil
		}
	}
	return erigonLogs, nil
}

func (api *ErigonImpl) GetBlockReceiptsByBlockHash(ctx context.Context, cannonicalBlockHash common.Hash) ([]map[string]interface{}, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	{
		blockNum := rawdb.ReadHeaderNumber(tx, cannonicalBlockHash)
		if blockNum == nil {
			return nil, fmt.Errorf("the hash %s is not cannonical", cannonicalBlockHash)
		}
		isCanonicalHash, err := rawdb.IsCanonicalHash(tx, cannonicalBlockHash, *blockNum)
		if err != nil {
			return nil, err
		}
		if !isCanonicalHash {
			return nil, fmt.Errorf("the hash %s is not cannonical", cannonicalBlockHash)
		}
	}

	blockNum, _, _, err := rpchelper.GetBlockNumber(rpc.BlockNumberOrHashWithHash(cannonicalBlockHash, true), tx, api.filters)
	if err != nil {
		return nil, err
	}
	block, err := api.blockWithSenders(ctx, tx, cannonicalBlockHash, blockNum)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	receipts, err := api.getReceipts(ctx, tx, block, block.Body().SendersFromTxs())
	if err != nil {
		return nil, fmt.Errorf("getReceipts error: %w", err)
	}

	result := make([]map[string]interface{}, 0, len(receipts))
	for _, receipt := range receipts {
		txn := block.Transactions()[receipt.TransactionIndex]
		result = append(result, ethutils.MarshalReceipt(receipt, txn, chainConfig, block.HeaderNoCopy(), txn.Hash(), true))
	}

	if chainConfig.Bor != nil {
		borTx := rawdb.ReadBorTransactionForBlock(tx, blockNum)
		if borTx != nil {
			borReceipt, err := rawdb.ReadBorReceipt(tx, block.Hash(), blockNum, receipts)
			if err != nil {
				return nil, err
			}
			if borReceipt != nil {
				result = append(result, ethutils.MarshalReceipt(borReceipt, borTx, chainConfig, block.HeaderNoCopy(), borReceipt.TxHash, false))
			}
		}
	}

	return result, nil
}

// GetLogsByNumber implements erigon_getLogsByHash. Returns all the logs that appear in a block given the block's hash.
// func (api *ErigonImpl) GetLogsByNumber(ctx context.Context, number rpc.BlockNumber) ([][]*types.Log, error) {
// 	tx, err := api.db.Begin(ctx, false)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer tx.Rollback()

// 	number := rawdb.ReadHeaderNumber(tx, hash)
// 	if number == nil {
// 		return nil, fmt.Errorf("block not found: %x", hash)
// 	}

// 	receipts, err := getReceipts(ctx, tx, *number, hash)
// 	if err != nil {
// 		return nil, fmt.Errorf("getReceipts error: %w", err)
// 	}

// 	logs := make([][]*types.Log, len(receipts))
// 	for i, receipt := range receipts {
// 		logs[i] = receipt.Logs
// 	}
// 	return logs, nil
// }
