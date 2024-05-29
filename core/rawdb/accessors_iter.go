package rawdb

import (
	"bytes"
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
)

type CanonicalTxs struct {
	blocks iter.KV

	// input params
	fromTxNum, toTxNum uint64
	orderAscend        order.By
	limit              int

	// private fields
	currentTxNum          int
	currentBlockLastTxNum int
}

func CanonicalTxNums(ctx context.Context, fromTxNum, toTxNum uint64, tx kv.Tx, asc order.By, limit int) (iter.U64, error) {
	if asc && fromTxNum > 0 && toTxNum > 0 && fromTxNum >= toTxNum {
		return nil, fmt.Errorf("fromTxNum >= toTxNum: %d, %d", fromTxNum, toTxNum)
	}
	if !asc && fromTxNum > 0 && toTxNum > 0 && fromTxNum <= toTxNum {
		return nil, fmt.Errorf("fromTxNum <= toTxNum: %d, %d", fromTxNum, toTxNum)
	}
	if !asc {
		return nil, fmt.Errorf("CanonicalTxNums: iteration Desc not supported yet")
	}

	var from, to []byte
	if fromTxNum > 0 {
		ok, blockFrom, err := rawdbv3.TxNums.FindBlockNum(tx, fromTxNum)
		if err != nil {
			return nil, err
		}
		if ok {
			to = hexutility.EncodeTs(blockFrom)
		}
	}

	if toTxNum > 0 {
		ok, blockTo, err := rawdbv3.TxNums.FindBlockNum(tx, toTxNum)
		if err != nil {
			return nil, err
		}
		if ok {
			to = hexutility.EncodeTs(blockTo + 1)
		}
	}

	var blocks iter.KV
	var err error
	if asc {
		blocks, err = tx.RangeAscend(kv.HeaderCanonical, from, to, -1)
		if err != nil {
			return nil, err
		}
	} else {
		blocks, err = tx.RangeDescend(kv.HeaderCanonical, from, to, -1)
		if err != nil {
			return nil, err
		}
	}

	if !blocks.HasNext() {
		return iter.EmptyU64, nil
	}

	it := &CanonicalTxs{blocks: blocks, fromTxNum: fromTxNum, toTxNum: toTxNum, orderAscend: asc, limit: limit}
	if err := it.init(tx); err != nil {
		it.Close() //it's responsibility of constructor (our) to close resource on error
		return nil, err
	}
	return it, nil
}

func (s *CanonicalTxs) init(tx kv.Tx) error {
	if s.fromPrefix == nil { // no initial position
		if s.orderAscend {
			s.nextK, s.nextV, err = s.c.First()
			if err != nil {
				return err
			}
		} else {
			s.nextK, s.nextV, err = s.c.Last()
			if err != nil {
				return err
			}

		}
		return nil
	}

	if s.orderAscend {
		s.nextK, s.nextV, err = s.c.Seek(s.fromPrefix)
		if err != nil {
			return err
		}
		return err
	}

	// to find LAST key with given prefix:
	nextSubtree, ok := kv.NextSubtree(s.fromPrefix)
	if ok {
		s.nextK, s.nextV, err = s.c.SeekExact(nextSubtree)
		if err != nil {
			return err
		}
		s.nextK, s.nextV, err = s.c.Prev()
		if err != nil {
			return err
		}
		if s.nextK != nil { // go to last value of this key
			if casted, ok := s.c.(kv.CursorDupSort); ok {
				s.nextV, err = casted.LastDup()
				if err != nil {
					return err
				}
			}
		}
	} else {
		s.nextK, s.nextV, err = s.c.Last()
		if err != nil {
			return err
		}
		if s.nextK != nil { // go to last value of this key
			if casted, ok := s.c.(kv.CursorDupSort); ok {
				s.nextV, err = casted.LastDup()
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *CanonicalTxs) advance() (err error) {
	if s.orderAscend {
		s.nextK, s.nextV, err = s.c.Next()
		if err != nil {
			return err
		}
	} else {
		s.nextK, s.nextV, err = s.c.Prev()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *CanonicalTxs) HasNext() bool {
	if s.limit == 0 { // limit reached
		return false
	}
	if s.nextK == nil { // EndOfTable
		return false
	}
	if s.toPrefix == nil { // s.nextK == nil check is above
		return true
	}

	//Asc:  [from, to) AND from < to
	//Desc: [from, to) AND from > to
	cmp := bytes.Compare(s.nextK, s.toPrefix)
	return (bool(s.orderAscend) && cmp < 0) || (!bool(s.orderAscend) && cmp > 0)
}

func (s *CanonicalTxs) Next() (k, v []byte, err error) {
	s.limit--
	k, v = s.nextK, s.nextV
	if err = s.advance(); err != nil {
		return nil, nil, err
	}
	return k, v, nil
}

func (s *CanonicalTxs) HasNext() bool {
	return s.blocks.HasNext()
}

func (s *CanonicalTxs) Close() {
	if s == nil {
		return
	}
	if s.c != nil {
		s.c.Close()
		delete(s.tx.streams, s.id)
		s.c = nil
	}
}
