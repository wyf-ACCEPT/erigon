package heimdall

import (
	"github.com/google/btree"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/polygon/bor/valset"
)

type Span struct {
	Id                SpanId              `json:"span_id" yaml:"span_id"`
	StartBlock        uint64              `json:"start_block" yaml:"start_block"`
	EndBlock          uint64              `json:"end_block" yaml:"end_block"`
	ValidatorSet      valset.ValidatorSet `json:"validator_set,omitempty" yaml:"validator_set"`
	SelectedProducers []valset.Validator  `json:"selected_producers,omitempty" yaml:"selected_producers"`
	ChainID           string              `json:"bor_chain_id,omitempty" yaml:"bor_chain_id"`
}

var _ Entity = &Span{}

func (s *Span) RawId() uint64 {
	return uint64(s.Id)
}

func (s *Span) SetRawId(id uint64) {
	panic("unimplemented")
}

func (s *Span) BlockNumRange() ClosedRange {
	return ClosedRange{
		Start: s.StartBlock,
		End:   s.EndBlock,
	}
}

func (s *Span) Producers() []*valset.Validator {
	res := make([]*valset.Validator, len(s.SelectedProducers))
	for i := range s.SelectedProducers {
		res[i] = s.SelectedProducers[i].Copy()
	}

	return res
}

// TODO give this a try!
func (s *Span) ProducersV2() []*valset.Validator {
	producerSet := map[common.Address]struct{}{}
	for _, producer := range s.SelectedProducers {
		producerSet[producer.Address] = struct{}{}
	}

	res := make([]*valset.Validator, 0, len(s.SelectedProducers))
	for _, validator := range s.ValidatorSet.Validators {
		if _, ok := producerSet[validator.Address]; ok {
			res = append(res, validator)
		}
	}

	return res
}

func (s *Span) Less(other btree.Item) bool {
	otherHs := other.(*Span)
	if s.EndBlock == 0 || otherHs.EndBlock == 0 {
		// if endblock is not specified in one of the items, allow search by ID
		return s.Id < otherHs.Id
	}
	return s.EndBlock < otherHs.EndBlock
}

func (s *Span) CmpRange(n uint64) int {
	if n < s.StartBlock {
		return -1
	}

	if n > s.EndBlock {
		return 1
	}

	return 0
}

type SpanResponse struct {
	Height string `json:"height"`
	Result Span   `json:"result"`
}
