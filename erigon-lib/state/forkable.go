/*
   Copyright 2022 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/assert"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/seg"
	"github.com/ledgerwatch/log/v3"
	"github.com/spaolacci/murmur3"
	btree2 "github.com/tidwall/btree"
	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
)

// Forkable - data type allows store data for different blockchain forks. Example: headers, bodies, receipts.
// Each record key has `timestamp + hash` format. It allows store multiple values for same timestamp.
// Keys may be canonical and non-canonical.
// Invariants:
//   - Forkable doesn't require unwind on re-org
//   - Forkable doesn't have Delete method - all data are immutable
//   - Only canonical keys will be moved to files
//   - Only 1 canonical key per 1 timestamp is allowed.
//   - Prune method delete canonical and non-canonical.
type Forkable struct {
	forkableCfg

	// dirtyFiles - list of ALL files - including: un-indexed-yet, garbage, merged-into-bigger-one, ...
	// thread-safe, but maybe need 1 RWLock for all trees in Aggregator
	//
	// _visibleFiles derivative from field `file`, but without garbage:
	//  - no files with `canDelete=true`
	//  - no overlaps
	//  - no un-indexed files (`power-off` may happen between .ef and .efi creation)
	//
	// BeginRo() using _visibleFiles in zero-copy way
	dirtyFiles *btree2.BTreeG[*filesItem]

	// _visibleFiles - underscore in name means: don't use this field directly, use BeginFilesRo()
	// underlying array is immutable - means it's ready for zero-copy use
	_visibleFiles []ctxItem

	table           string // txnNum_u64 -> key (k+auto_increment)
	filenameBase    string
	aggregationStep uint64

	//TODO: re-visit this check - maybe we don't need it. It's abot kill in the middle of merge
	integrityCheck func(fromStep, toStep uint64) bool

	// fields for history write
	logger log.Logger

	noFsync bool // fsync is enabled by default, but tests can manually disable

	compression     FileCompression
	compressWorkers int
	indexList       idxList
}

type forkableCfg struct {
	salt *uint32
	dirs datadir.Dirs
	db   kv.RoDB // global db pointer. mostly for background warmup.
}

func NewForkable(cfg forkableCfg, aggregationStep uint64, filenameBase, table string, integrityCheck func(fromStep uint64, toStep uint64) bool, logger log.Logger) (*Forkable, error) {
	if cfg.dirs.SnapDomain == "" {
		panic("empty `dirs` varialbe")
	}
	fk := Forkable{
		forkableCfg:     cfg,
		dirtyFiles:      btree2.NewBTreeGOptions[*filesItem](filesItemLess, btree2.Options{Degree: 128, NoLocks: false}),
		aggregationStep: aggregationStep,
		filenameBase:    filenameBase,
		table:           table,
		compressWorkers: 1,
		integrityCheck:  integrityCheck,
		logger:          logger,
		compression:     CompressKeys | CompressVals,
	}
	fk.indexList = withHashMap
	fk._visibleFiles = []ctxItem{}

	return &fk, nil
}

func (fk *Forkable) fkAccessorFilePath(fromStep, toStep uint64) string {
	return filepath.Join(fk.dirs.SnapAccessors, fmt.Sprintf("v1-%s.%d-%d.fki", fk.filenameBase, fromStep, toStep))
}
func (fk *Forkable) fkFilePath(fromStep, toStep uint64) string {
	return filepath.Join(fk.dirs.SnapForkable, fmt.Sprintf("v1-%s.%d-%d.fk", fk.filenameBase, fromStep, toStep))
}

func (fk *Forkable) fileNamesOnDisk() (idx, hist, domain []string, err error) {
	idx, err = filesFromDir(fk.dirs.SnapIdx)
	if err != nil {
		return
	}
	hist, err = filesFromDir(fk.dirs.SnapHistory)
	if err != nil {
		return
	}
	domain, err = filesFromDir(fk.dirs.SnapDomain)
	if err != nil {
		return
	}
	return
}

func (fk *Forkable) OpenList(fNames []string, readonly bool) error {
	fk.closeWhatNotInList(fNames)
	fk.scanStateFiles(fNames)
	if err := fk.openFiles(); err != nil {
		return fmt.Errorf("NewHistory.openFiles: %w, %s", err, fk.filenameBase)
	}
	_ = readonly // for future safety features. RPCDaemon must not delte files
	return nil
}

func (fk *Forkable) OpenFolder(readonly bool) error {
	idxFiles, _, _, err := fk.fileNamesOnDisk()
	if err != nil {
		return err
	}
	return fk.OpenList(idxFiles, readonly)
}

func (fk *Forkable) scanStateFiles(fileNames []string) (garbageFiles []*filesItem) {
	re := regexp.MustCompile("^v([0-9]+)-" + fk.filenameBase + ".([0-9]+)-([0-9]+).ef$")
	var err error
	for _, name := range fileNames {
		subs := re.FindStringSubmatch(name)
		if len(subs) != 4 {
			if len(subs) != 0 {
				fk.logger.Warn("File ignored by inverted index scan, more than 3 submatches", "name", name, "submatches", len(subs))
			}
			continue
		}
		var startStep, endStep uint64
		if startStep, err = strconv.ParseUint(subs[2], 10, 64); err != nil {
			fk.logger.Warn("File ignored by inverted index scan, parsing startTxNum", "error", err, "name", name)
			continue
		}
		if endStep, err = strconv.ParseUint(subs[3], 10, 64); err != nil {
			fk.logger.Warn("File ignored by inverted index scan, parsing endTxNum", "error", err, "name", name)
			continue
		}
		if startStep > endStep {
			fk.logger.Warn("File ignored by inverted index scan, startTxNum > endTxNum", "name", name)
			continue
		}

		startTxNum, endTxNum := startStep*fk.aggregationStep, endStep*fk.aggregationStep
		var newFile = newFilesItem(startTxNum, endTxNum, fk.aggregationStep)

		if fk.integrityCheck != nil && !fk.integrityCheck(startStep, endStep) {
			continue
		}

		if _, has := fk.dirtyFiles.Get(newFile); has {
			continue
		}

		fk.dirtyFiles.Set(newFile)
	}
	return garbageFiles
}

func (fk *Forkable) reCalcVisibleFiles() {
	fk._visibleFiles = calcVisibleFiles(fk.dirtyFiles, fk.indexList, false)
}

func (fk *Forkable) missedIdxFiles() (l []*filesItem) {
	fk.dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			fromStep, toStep := item.startTxNum/fk.aggregationStep, item.endTxNum/fk.aggregationStep
			if !dir.FileExist(fk.fkAccessorFilePath(fromStep, toStep)) {
				l = append(l, item)
			}
		}
		return true
	})
	return l
}

func (fk *Forkable) buildIdx(ctx context.Context, fromStep, toStep uint64, d *seg.Decompressor, ps *background.ProgressSet) error {
	if d == nil {
		return fmt.Errorf("buildIdx: passed item with nil decompressor %s %d-%d", fk.filenameBase, fromStep, toStep)
	}
	idxPath := fk.fkAccessorFilePath(fromStep, toStep)
	cfg := recsplit.RecSplitArgs{
		Enums: true,

		BucketSize: 2000,
		LeafSize:   8,
		TmpDir:     fk.dirs.Tmp,
		IndexFile:  idxPath,
		Salt:       fk.salt,
		NoFsync:    fk.noFsync,

		KeyCount: d.Count(),
	}
	_, fileName := filepath.Split(idxPath)
	count := d.Count()
	p := ps.AddNew(fileName, uint64(count))
	defer ps.Delete(p)

	num := make([]byte, binary.MaxVarintLen64)
	return buildSimpleIndex(ctx, d, cfg, fk.logger, func(idx *recsplit.RecSplit, i, offset uint64, word []byte) error {
		if p != nil {
			p.Processed.Add(1)
		}
		n := binary.PutUvarint(num, i)
		if err := idx.AddKey(num[:n], offset); err != nil {
			return err
		}
		return nil
	})
}

// BuildMissedIndices - produce .efi/.vi/.kvi from .ef/.v/.kv
func (fk *Forkable) BuildMissedIndices(ctx context.Context, g *errgroup.Group, ps *background.ProgressSet) {
	for _, item := range fk.missedIdxFiles() {
		item := item
		g.Go(func() error {
			fromStep, toStep := item.startTxNum/fk.aggregationStep, item.endTxNum/fk.aggregationStep
			return fk.buildIdx(ctx, fromStep, toStep, item.decompressor, ps)
		})
	}
}

func (fk *Forkable) openFiles() error {
	var invalidFileItems []*filesItem
	invalidFileItemsLock := sync.Mutex{}
	fk.dirtyFiles.Walk(func(items []*filesItem) bool {
		var err error
		for _, item := range items {
			item := item
			fromStep, toStep := item.startTxNum/fk.aggregationStep, item.endTxNum/fk.aggregationStep
			if item.decompressor == nil {
				fPath := fk.fkFilePath(fromStep, toStep)
				if !dir.FileExist(fPath) {
					_, fName := filepath.Split(fPath)
					fk.logger.Debug("[agg] InvertedIndex.openFiles: file does not exists", "f", fName)
					invalidFileItemsLock.Lock()
					invalidFileItems = append(invalidFileItems, item)
					invalidFileItemsLock.Unlock()
					continue
				}

				if item.decompressor, err = seg.NewDecompressor(fPath); err != nil {
					_, fName := filepath.Split(fPath)
					if errors.Is(err, &seg.ErrCompressedFileCorrupted{}) {
						fk.logger.Debug("[agg] InvertedIndex.openFiles", "err", err, "f", fName)
					} else {
						fk.logger.Warn("[agg] InvertedIndex.openFiles", "err", err, "f", fName)
					}
					invalidFileItemsLock.Lock()
					invalidFileItems = append(invalidFileItems, item)
					invalidFileItemsLock.Unlock()
					// don't interrupt on error. other files may be good. but skip indices open.
					continue
				}
			}

			if item.index == nil {
				fPath := fk.fkAccessorFilePath(fromStep, toStep)
				if dir.FileExist(fPath) {
					if item.index, err = recsplit.OpenIndex(fPath); err != nil {
						_, fName := filepath.Split(fPath)
						fk.logger.Warn("[agg] InvertedIndex.openFiles", "err", err, "f", fName)
						// don't interrupt on error. other files may be good
					}
				}
			}
		}

		return true
	})
	for _, item := range invalidFileItems {
		item.closeFiles()
		fk.dirtyFiles.Delete(item)
	}

	return nil
}

func (fk *Forkable) closeWhatNotInList(fNames []string) {
	var toDelete []*filesItem
	fk.dirtyFiles.Walk(func(items []*filesItem) bool {
	Loop1:
		for _, item := range items {
			for _, protectName := range fNames {
				if item.decompressor != nil && item.decompressor.FileName() == protectName {
					continue Loop1
				}
			}
			toDelete = append(toDelete, item)
		}
		return true
	})
	for _, item := range toDelete {
		item.closeFiles()
		fk.dirtyFiles.Delete(item)
	}
}

func (fk *Forkable) Close() {
	fk.closeWhatNotInList([]string{})
}

// DisableFsync - just for tests
func (fk *Forkable) DisableFsync() { fk.noFsync = true }

func (tx *ForkableRoTx) Files() (res []string) {
	for _, item := range tx.files {
		if item.src.decompressor != nil {
			res = append(res, item.src.decompressor.FileName())
		}
	}
	return res
}

// Add - !NotThreadSafe. Must use WalRLock/BatchHistoryWriteEnd
func (w *forkableBufferedWriter) Add(key []byte) error {
	if w.discard {
		return nil
	}
	if err := w.tableCollector.Collect(key, w.timestampBytes[:]); err != nil {
		return err
	}
	return nil
}

func (tx *ForkableRoTx) NewWriter() *forkableBufferedWriter {
	return tx.newWriter(tx.ii.dirs.Tmp, false)
}

type forkableBufferedWriter struct {
	tableCollector *etl.Collector
	tmpdir         string
	discard        bool
	filenameBase   string

	table string

	aggregationStep uint64

	//current TimeStamp - BlockNum or TxNum
	timestamp      uint64
	timestampBytes [8]byte
}

func (w *forkableBufferedWriter) SetTimeStamp(ts uint64) {
	w.timestamp = ts
	binary.BigEndian.PutUint64(w.timestampBytes[:], w.timestamp)
}

func (w *forkableBufferedWriter) Flush(ctx context.Context, tx kv.RwTx) error {
	if w.discard {
		return nil
	}
	if err := w.tableCollector.Load(tx, w.table, loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}
	w.close()
	return nil
}

func (w *forkableBufferedWriter) close() {
	if w == nil {
		return
	}
	if w.tableCollector != nil {
		w.tableCollector.Close()
	}
}

func (tx *ForkableRoTx) newWriter(tmpdir string, discard bool) *forkableBufferedWriter {
	w := &forkableBufferedWriter{
		discard:         discard,
		tmpdir:          tmpdir,
		filenameBase:    tx.ii.filenameBase,
		aggregationStep: tx.ii.aggregationStep,

		table: tx.ii.table,
		// etl collector doesn't fsync: means if have enough ram, all files produced by all collectors will be in ram
		tableCollector: etl.NewCollector("flush "+tx.ii.table, tmpdir, etl.NewSortableBuffer(WALCollectorRAM), tx.ii.logger),
	}
	w.tableCollector.LogLvl(log.LvlTrace)
	w.tableCollector.SortAndFlushInBackground(true)
	return w
}

func (fk *Forkable) BeginFilesRo() *ForkableRoTx {
	files := fk._visibleFiles
	for i := 0; i < len(files); i++ {
		if !files[i].src.frozen {
			files[i].src.refcount.Add(1)
		}
	}
	return &ForkableRoTx{
		ii:    fk,
		files: files,
	}
}

func (tx *ForkableRoTx) Close() {
	if tx.files == nil { // invariant: it's safe to call Close multiple times
		return
	}
	files := tx.files
	tx.files = nil
	for i := 0; i < len(files); i++ {
		if files[i].src.frozen {
			continue
		}
		refCnt := files[i].src.refcount.Add(-1)
		//GC: last reader responsible to remove useles files: close it and delete
		if refCnt == 0 && files[i].src.canDelete.Load() {
			if tx.ii.filenameBase == traceFileLife {
				tx.ii.logger.Warn(fmt.Sprintf("[agg] real remove at ctx close: %s", files[i].src.decompressor.FileName()))
			}
			files[i].src.closeFilesAndRemove()
		}
	}

	for _, r := range tx.readers {
		r.Close()
	}
}

type ForkableRoTx struct {
	ii      *Forkable
	files   []ctxItem // have no garbage (overlaps, etc...)
	getters []ArchiveGetter
	readers []*recsplit.IndexReader

	_hasher murmur3.Hash128
}

func (tx *ForkableRoTx) statelessHasher() murmur3.Hash128 {
	if tx._hasher == nil {
		tx._hasher = murmur3.New128WithSeed(*tx.ii.salt)
	}
	return tx._hasher
}
func (tx *ForkableRoTx) hashKey(k []byte) (hi, lo uint64) {
	hasher := tx.statelessHasher()
	tx._hasher.Reset()
	_, _ = hasher.Write(k) //nolint:errcheck
	return hasher.Sum128()
}

func (tx *ForkableRoTx) statelessGetter(i int) ArchiveGetter {
	if tx.getters == nil {
		tx.getters = make([]ArchiveGetter, len(tx.files))
	}
	r := tx.getters[i]
	if r == nil {
		g := tx.files[i].src.decompressor.MakeGetter()
		r = NewArchiveGetter(g, tx.ii.compression)
		tx.getters[i] = r
	}
	return r
}
func (tx *ForkableRoTx) statelessIdxReader(i int) *recsplit.IndexReader {
	if tx.readers == nil {
		tx.readers = make([]*recsplit.IndexReader, len(tx.files))
	}
	r := tx.readers[i]
	if r == nil {
		r = tx.files[i].src.index.GetReaderFromPool()
		tx.readers[i] = r
	}
	return r
}

func (tx *ForkableRoTx) getInFiles(ts uint64) (v []byte, ok bool) {
	i, ok := tx.fileByTS(ts)
	if !ok {
		return nil, false
	}

	offset := tx.statelessIdxReader(i).OrdinalLookup(ts)

	g := tx.statelessGetter(i)
	g.Reset(offset)
	k, _ := g.Next(nil)
	return k, true
}

func (tx *ForkableRoTx) Get(blockNum uint64, blockHash common.Hash, dbtx kv.Tx) ([]byte, bool, error) {
	v, ok := tx.getInFiles(blockNum)
	if ok {
		return v, true, nil
	}
	return tx.getInDB(blockNum, blockHash, dbtx)
}
func (tx *ForkableRoTx) fileByTS(ts uint64) (i int, ok bool) {
	for i = 0; i < len(tx.files); i++ {
		if tx.files[i].hasTS(ts) {
			return i, true
		}
	}
	return 0, false
}

func (tx *ForkableRoTx) Put(blockNum uint64, blockHash common.Hash, v []byte, dbtx kv.RwTx) error {
	k := make([]byte, length.BlockNum+length.Hash)
	binary.BigEndian.PutUint64(k, blockNum)
	copy(k[length.BlockNum:], blockHash[:])
	return dbtx.Put(tx.ii.table, k, v)
}

func (tx *ForkableRoTx) getInDB(blockNum uint64, blockHash common.Hash, dbtx kv.Tx) ([]byte, bool, error) {
	k := make([]byte, length.BlockNum+length.Hash)
	binary.BigEndian.PutUint64(k, blockNum)
	copy(k[length.BlockNum:], blockHash[:])
	v, err := dbtx.GetOne(tx.ii.table, k)
	if err != nil {
		return nil, false, err
	}
	return v, v != nil, err
}

func (tx *ForkableRoTx) smallestTxNum(dbtx kv.Tx) uint64 {
	fst, _ := kv.FirstKey(dbtx, tx.ii.table)
	if len(fst) > 0 {
		fstInDb := binary.BigEndian.Uint64(fst)
		return cmp.Min(fstInDb, math.MaxUint64)
	}
	return math.MaxUint64
}

func (tx *ForkableRoTx) highestTxNum(dbtx kv.Tx) uint64 {
	lst, _ := kv.LastKey(dbtx, tx.ii.table)
	if len(lst) > 0 {
		lstInDb := binary.BigEndian.Uint64(lst)
		return cmp.Max(lstInDb, 0)
	}
	return 0
}

func (tx *ForkableRoTx) CanPrune(dbtx kv.Tx) bool {
	return tx.smallestTxNum(dbtx) < tx.maxTxNumInFiles(false)
}

func (tx *ForkableRoTx) maxTxNumInFiles(cold bool) uint64 {
	if len(tx.files) == 0 {
		return 0
	}
	if !cold {
		return tx.files[len(tx.files)-1].endTxNum
	}
	for i := len(tx.files) - 1; i >= 0; i-- {
		if !tx.files[i].src.frozen {
			continue
		}
		return tx.files[i].endTxNum
	}
	return 0
}

type ForkablePruneStat struct {
	MinTxNum         uint64
	MaxTxNum         uint64
	PruneCountTx     uint64
	PruneCountValues uint64
}

func (is *ForkablePruneStat) String() string {
	if is.MinTxNum == math.MaxUint64 && is.PruneCountTx == 0 {
		return ""
	}
	return fmt.Sprintf("ii %d txs and %d vals in %.2fM-%.2fM", is.PruneCountTx, is.PruneCountValues, float64(is.MinTxNum)/1_000_000.0, float64(is.MaxTxNum)/1_000_000.0)
}

func (is *ForkablePruneStat) Accumulate(other *InvertedIndexPruneStat) {
	if other == nil {
		return
	}
	is.MinTxNum = min(is.MinTxNum, other.MinTxNum)
	is.MaxTxNum = max(is.MaxTxNum, other.MaxTxNum)
	is.PruneCountTx += other.PruneCountTx
	is.PruneCountValues += other.PruneCountValues
}

// [txFrom; txTo)
// forced - prune even if CanPrune returns false, so its true only when we do Unwind.
func (tx *ForkableRoTx) Prune(ctx context.Context, rwTx kv.RwTx, txFrom, txTo, limit uint64, logEvery *time.Ticker, forced, withWarmup bool, fn func(key []byte, txnum []byte) error) (stat *InvertedIndexPruneStat, err error) {
	stat = &InvertedIndexPruneStat{MinTxNum: math.MaxUint64}
	if !forced && !tx.CanPrune(rwTx) {
		return stat, nil
	}

	_ = withWarmup

	mxPruneInProgress.Inc()
	defer mxPruneInProgress.Dec()
	defer func(t time.Time) { mxPruneTookIndex.ObserveDuration(t) }(time.Now())

	if limit == 0 {
		limit = math.MaxUint64
	}

	ii := tx.ii
	//defer func() {
	//	ii.logger.Error("[snapshots] prune index",
	//		"name", ii.filenameBase,
	//		"forced", forced,
	//		"pruned tx", fmt.Sprintf("%.2f-%.2f", float64(minTxnum)/float64(iit.ii.aggregationStep), float64(maxTxnum)/float64(iit.ii.aggregationStep)),
	//		"pruned values", pruneCount,
	//		"tx until limit", limit)
	//}()

	keysCursor, err := rwTx.RwCursorDupSort(tx.ii.table)
	if err != nil {
		return stat, fmt.Errorf("create %s keys cursor: %w", tx.ii.filenameBase, err)
	}
	defer keysCursor.Close()
	keysCursorForDel, err := rwTx.RwCursorDupSort(tx.ii.table)
	if err != nil {
		return stat, fmt.Errorf("create %s keys cursor: %w", ii.filenameBase, err)
	}
	defer keysCursorForDel.Close()
	idxC, err := rwTx.RwCursorDupSort(ii.table)
	if err != nil {
		return nil, err
	}
	defer idxC.Close()
	idxValuesCount, err := idxC.Count()
	if err != nil {
		return nil, err
	}
	indexWithValues := idxValuesCount != 0 || fn != nil

	collector := etl.NewCollector("snapshots", ii.dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize/8), ii.logger)
	defer collector.Close()
	collector.LogLvl(log.LvlDebug)
	collector.SortAndFlushInBackground(true)

	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)

	// Invariant: if some `txNum=N` pruned - it's pruned Fully
	// Means: can use DeleteCurrentDuplicates all values of given `txNum`
	for k, v, err := keysCursor.Seek(txKey[:]); k != nil; k, v, err = keysCursor.NextNoDup() {
		if err != nil {
			return nil, fmt.Errorf("iterate over %s index keys: %w", ii.filenameBase, err)
		}

		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo || limit == 0 {
			break
		}
		if txNum < txFrom {
			panic(fmt.Errorf("assert: index pruning txn=%d [%d-%d)", txNum, txFrom, txTo))
		}
		limit--
		stat.MinTxNum = min(stat.MinTxNum, txNum)
		stat.MaxTxNum = max(stat.MaxTxNum, txNum)

		if indexWithValues {
			for ; v != nil; _, v, err = keysCursor.NextDup() {
				if err != nil {
					return nil, fmt.Errorf("iterate over %s index keys: %w", ii.filenameBase, err)
				}
				if err := collector.Collect(v, k); err != nil {
					return nil, err
				}
			}
		}

		stat.PruneCountTx++
		// This DeleteCurrent needs to the last in the loop iteration, because it invalidates k and v
		if err = rwTx.Delete(ii.table, k); err != nil {
			return nil, err
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	if !indexWithValues {
		return stat, nil
	}

	idxCForDeletes, err := rwTx.RwCursorDupSort(ii.table)
	if err != nil {
		return nil, err
	}
	defer idxCForDeletes.Close()

	binary.BigEndian.PutUint64(txKey[:], txFrom)
	err = collector.Load(nil, "", func(key, txnm []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if fn != nil {
			if err = fn(key, txnm); err != nil {
				return fmt.Errorf("fn error: %w", err)
			}
		}
		if idxValuesCount > 0 {
			if err = idxCForDeletes.DeleteExact(key, txnm); err != nil {
				return err
			}
		}
		mxPruneSizeIndex.Inc()
		stat.PruneCountValues++

		select {
		case <-logEvery.C:
			txNum := binary.BigEndian.Uint64(txnm)
			ii.logger.Info("[snapshots] prune index", "name", ii.filenameBase, "pruned tx", stat.PruneCountTx,
				"pruned values", stat.PruneCountValues,
				"steps", fmt.Sprintf("%.2f-%.2f", float64(txFrom)/float64(ii.aggregationStep), float64(txNum)/float64(ii.aggregationStep)))
		default:
		}
		return nil
	}, etl.TransformArgs{Quit: ctx.Done()})

	return stat, err
}

func (tx *ForkableRoTx) DebugEFAllValuesAreInRange(ctx context.Context) error {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	iterStep := func(item ctxItem) error {
		g := item.src.decompressor.MakeGetter()
		g.Reset(0)
		defer item.src.decompressor.EnableReadAhead().DisableReadAhead()

		for g.HasNext() {
			k, _ := g.NextUncompressed()
			_ = k
			eliasVal, _ := g.NextUncompressed()
			ef, _ := eliasfano32.ReadEliasFano(eliasVal)
			if ef.Count() == 0 {
				continue
			}
			if item.startTxNum > ef.Min() {
				err := fmt.Errorf("DebugEFAllValuesAreInRange1: %d > %d, %s, %x", item.startTxNum, ef.Min(), g.FileName(), k)
				log.Warn(err.Error())
				//return err
			}
			if item.endTxNum < ef.Max() {
				err := fmt.Errorf("DebugEFAllValuesAreInRange2: %d < %d, %s, %x", item.endTxNum, ef.Max(), g.FileName(), k)
				log.Warn(err.Error())
				//return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-logEvery.C:
				log.Info(fmt.Sprintf("[integrity] EFAllValuesAreInRange: %s, k=%x", g.FileName(), k))
			default:
			}
		}
		return nil
	}

	for _, item := range tx.files {
		if item.src.decompressor == nil {
			continue
		}
		if err := iterStep(item); err != nil {
			return err
		}
		//log.Warn(fmt.Sprintf("[dbg] see1: %s, min=%d,max=%d, before_max=%d, all: %d\n", item.src.decompressor.FileName(), ef.Min(), ef.Max(), last2, iter.ToArrU64Must(ef.Iterator())))
	}
	return nil
}

// collate [stepFrom, stepTo)
func (fk *Forkable) collate(ctx context.Context, step uint64, roTx kv.Tx) (ForkableCollation, error) {
	stepTo := step + 1
	txFrom, txTo := step*fk.aggregationStep, stepTo*fk.aggregationStep
	start := time.Now()
	defer mxCollateTookIndex.ObserveDuration(start)

	keysCursor, err := roTx.CursorDupSort(fk.table)
	if err != nil {
		return ForkableCollation{}, fmt.Errorf("create %s keys cursor: %w", fk.filenameBase, err)
	}
	defer keysCursor.Close()

	collector := etl.NewCollector("collate "+fk.table, fk.forkableCfg.dirs.Tmp, etl.NewSortableBuffer(CollateETLRAM), fk.logger)
	defer collector.Close()
	collector.LogLvl(log.LvlTrace)

	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txFrom)

	for k, v, err := keysCursor.Seek(txKey[:]); k != nil; k, v, err = keysCursor.Next() {
		if err != nil {
			return ForkableCollation{}, fmt.Errorf("iterate over %s keys cursor: %w", fk.filenameBase, err)
		}
		txNum := binary.BigEndian.Uint64(k)
		if txNum >= txTo { // [txFrom; txTo)
			break
		}
		if err := collector.Collect(v, k); err != nil {
			return ForkableCollation{}, fmt.Errorf("collect %s history key [%x]=>txn %d [%x]: %w", fk.filenameBase, k, txNum, k, err)
		}
		select {
		case <-ctx.Done():
			return ForkableCollation{}, ctx.Err()
		default:
		}
	}

	var (
		coll = ForkableCollation{
			iiPath: fk.fkFilePath(step, stepTo),
		}
		closeComp bool
	)
	defer func() {
		if closeComp {
			coll.Close()
		}
	}()

	comp, err := seg.NewCompressor(ctx, "snapshots", coll.iiPath, fk.dirs.Tmp, seg.MinPatternScore, fk.compressWorkers, log.LvlTrace, fk.logger)
	if err != nil {
		return ForkableCollation{}, fmt.Errorf("create %s compressor: %w", fk.filenameBase, err)
	}
	coll.writer = NewArchiveWriter(comp, fk.compression)

	var (
		prevEf      []byte
		prevKey     []byte
		initialized bool
		bitmap      = bitmapdb.NewBitmap64()
	)
	defer bitmapdb.ReturnToPool64(bitmap)

	loadBitmapsFunc := func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		txNum := binary.BigEndian.Uint64(v)
		if !initialized {
			prevKey = append(prevKey[:0], k...)
			initialized = true
		}

		if bytes.Equal(prevKey, k) {
			bitmap.Add(txNum)
			prevKey = append(prevKey[:0], k...)
			return nil
		}

		ef := eliasfano32.NewEliasFano(bitmap.GetCardinality(), bitmap.Maximum())
		it := bitmap.Iterator()
		for it.HasNext() {
			ef.AddOffset(it.Next())
		}
		bitmap.Clear()
		ef.Build()

		prevEf = ef.AppendBytes(prevEf[:0])

		if err = coll.writer.AddWord(prevKey); err != nil {
			return fmt.Errorf("add %s efi index key [%x]: %w", fk.filenameBase, prevKey, err)
		}
		if err = coll.writer.AddWord(prevEf); err != nil {
			return fmt.Errorf("add %s efi index val: %w", fk.filenameBase, err)
		}

		prevKey = append(prevKey[:0], k...)
		txNum = binary.BigEndian.Uint64(v)
		bitmap.Add(txNum)

		return nil
	}

	err = collector.Load(nil, "", loadBitmapsFunc, etl.TransformArgs{Quit: ctx.Done()})
	if err != nil {
		return ForkableCollation{}, err
	}
	if !bitmap.IsEmpty() {
		if err = loadBitmapsFunc(nil, make([]byte, 8), nil, nil); err != nil {
			return ForkableCollation{}, err
		}
	}

	closeComp = false
	return coll, nil
}

func (fk *Forkable) stepsRangeInDBAsStr(tx kv.Tx) string {
	a1, a2 := fk.stepsRangeInDB(tx)
	return fmt.Sprintf("%s: %.1f", fk.filenameBase, a2-a1)
}
func (fk *Forkable) stepsRangeInDB(tx kv.Tx) (from, to float64) {
	fst, _ := kv.FirstKey(tx, fk.table)
	if len(fst) > 0 {
		from = float64(binary.BigEndian.Uint64(fst)) / float64(fk.aggregationStep)
	}
	lst, _ := kv.LastKey(tx, fk.table)
	if len(lst) > 0 {
		to = float64(binary.BigEndian.Uint64(lst)) / float64(fk.aggregationStep)
	}
	if to == 0 {
		to = from
	}
	return from, to
}

type ForkableFiles struct {
	decomp *seg.Decompressor
	index  *recsplit.Index
}

func (sf ForkableFiles) CleanupOnError() {
	if sf.decomp != nil {
		sf.decomp.Close()
	}
	if sf.index != nil {
		sf.index.Close()
	}
}

type ForkableCollation struct {
	iiPath string
	writer ArchiveWriter
}

func (ic ForkableCollation) Close() {
	if ic.writer != nil {
		ic.writer.Close()
	}
}

// buildFiles - `step=N` means build file `[N:N+1)` which is equal to [N:N+1)
func (fk *Forkable) buildFiles(ctx context.Context, step uint64, coll ForkableCollation, ps *background.ProgressSet) (ForkableFiles, error) {
	var (
		decomp *seg.Decompressor
		index  *recsplit.Index
		err    error
	)
	mxRunningFilesBuilding.Inc()
	defer mxRunningFilesBuilding.Dec()
	closeComp := true
	defer func() {
		if closeComp {
			coll.Close()
			if decomp != nil {
				decomp.Close()
			}
			if index != nil {
				index.Close()
			}
		}
	}()

	if assert.Enable {
		if coll.iiPath == "" && reflect.ValueOf(coll.writer).IsNil() {
			panic("assert: collation is not initialized " + fk.filenameBase)
		}
	}

	{
		p := ps.AddNew(path.Base(coll.iiPath), 1)
		if err = coll.writer.Compress(); err != nil {
			ps.Delete(p)
			return ForkableFiles{}, fmt.Errorf("compress %s: %w", fk.filenameBase, err)
		}
		coll.Close()
		ps.Delete(p)
	}

	if decomp, err = seg.NewDecompressor(coll.iiPath); err != nil {
		return ForkableFiles{}, fmt.Errorf("open %s decompressor: %w", fk.filenameBase, err)
	}

	if err := fk.buildIdx(ctx, step, step+1, decomp, ps); err != nil {
		return ForkableFiles{}, fmt.Errorf("build %s efi: %w", fk.filenameBase, err)
	}
	if index, err = recsplit.OpenIndex(fk.fkAccessorFilePath(step, step+1)); err != nil {
		return ForkableFiles{}, err
	}

	closeComp = false
	return ForkableFiles{decomp: decomp, index: index}, nil
}

func (fk *Forkable) integrateDirtyFiles(sf ForkableFiles, txNumFrom, txNumTo uint64) {
	fi := newFilesItem(txNumFrom, txNumTo, fk.aggregationStep)
	fi.decompressor = sf.decomp
	fi.index = sf.index
	fk.dirtyFiles.Set(fi)
}

func (fk *Forkable) collectFilesStat() (filesCount, filesSize, idxSize uint64) {
	if fk.dirtyFiles == nil {
		return 0, 0, 0
	}
	fk.dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			if item.index == nil {
				return false
			}
			filesSize += uint64(item.decompressor.Size())
			idxSize += uint64(item.index.Size())
			idxSize += uint64(item.bindex.Size())
			filesCount += 3
		}
		return true
	})
	return filesCount, filesSize, idxSize
}
