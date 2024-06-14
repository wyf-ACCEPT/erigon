package sync

import (
	"context"
	"fmt"
	"time"

	lru "github.com/hashicorp/golang-lru/arc/v2"
	"github.com/ledgerwatch/log/v3"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/polygon/bor"
	"github.com/ledgerwatch/erigon/polygon/bor/borcfg"
	"github.com/ledgerwatch/erigon/polygon/bor/valset"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
)

type HeaderTimeValidator interface {
	ValidateHeaderTime(header *types.Header, now time.Time, parent *types.Header) error
}

type headerTimeValidator struct {
	borConfig           *borcfg.BorConfig
	spans               *SpansCache
	validatorSetFactory func(headerNum uint64) validatorSetInterface
	signaturesCache     *lru.ARCCache[libcommon.Hash, libcommon.Address]
	heimdallClient      heimdall.HeimdallClient
}

func NewHeaderTimeValidator(
	borConfig *borcfg.BorConfig,
	spans *SpansCache,
	validatorSetFactory func(headerNum uint64) validatorSetInterface,
	signaturesCache *lru.ARCCache[libcommon.Hash, libcommon.Address],
) HeaderTimeValidator {
	if signaturesCache == nil {
		var err error
		signaturesCache, err = lru.NewARC[libcommon.Hash, libcommon.Address](InMemorySignatures)
		if err != nil {
			panic(err)
		}
	}

	htv := headerTimeValidator{
		borConfig:           borConfig,
		spans:               spans,
		validatorSetFactory: validatorSetFactory,
		signaturesCache:     signaturesCache,
		heimdallClient:      heimdall.NewHeimdallClient("https://heimdall-api-amoy.polygon.technology", log.New()),
	}

	if validatorSetFactory == nil {
		htv.validatorSetFactory = htv.makeValidatorSet
	}

	return &htv
}

func (htv *headerTimeValidator) makeValidatorSet(headerNum uint64) validatorSetInterface {
	span := htv.spans.SpanAt(headerNum)
	if span == nil {
		return nil
	}
	//vs := valset.ValidatorSet{
	//	Validators: span.Producers(),
	//}
	//proposer := vs.GetProposer()
	//fmt.Printf("initial proposer=%s\n", proposer.Address)
	//vs.UpdateValidatorMap()
	//if err := vs.UpdateTotalVotingPower(); err != nil {
	//	panic(err)
	//}
	//vs.IncrementProposerPriority(1)
	//return &vs
	return valset.NewValidatorSet(span.Producers())
	//return valset.NewValidatorSet(span.ProducersV2())
	//return valset.NewValidatorSet(span.ValidatorSet.Validators)
}

func (htv *headerTimeValidator) ValidateHeaderTime(header *types.Header, now time.Time, parent *types.Header) error {
	return htv.ValidateHeaderTimeV2(header, now, parent)

	headerNum := header.Number.Uint64()
	validatorSet := htv.validatorSetFactory(headerNum)
	if validatorSet == nil {
		return fmt.Errorf("headerTimeValidator.ValidateHeaderTime: no span at %d", headerNum)
	}

	//sprintLength := htv.borConfig.CalculateSprintLength(headerNum)
	//increments := (headerNum / 6400) % sprintLength
	//sprintNum := htv.borConfig.CalculateSprintNumber(headerNum)
	currentSprintNumber := htv.borConfig.CalculateSprintNumber(headerNum)
	fmt.Printf("currentSprintNumber=%d\n", currentSprintNumber)
	firstSprintNumber := htv.borConfig.CalculateSprintNumber(htv.spans.SpanAt(headerNum).StartBlock)
	fmt.Printf("firstSprintNumber=%d\n", firstSprintNumber)
	incrementsNeeded := int(currentSprintNumber - firstSprintNumber)
	//incrementsNeeded = int(htv.borConfig.CalculateSprintLength(headerNum))
	if incrementsNeeded > 0 {
		//fmt.Printf("sprintNum=%d\n", sprintNum)
		//firstSprintNum := htv.borConfig.CalculateSprintNumber(htv.spans.SpanAt(headerNum).StartBlock)
		//fmt.Printf("sprintNum=%d\n", firstSprintNum)
		//times := sprintNum - firstSprintNum
		//fmt.Printf("sprintDiff=%d\n", times)
		//validatorSet.IncrementProposerPriority(int(sprintNum))
		//validatorSet.IncrementProposerPriority(int(sprintNum) + 2)
		//validatorSet.IncrementProposerPriority(int(times) - 2)
		fmt.Printf("incrementsNeeded=%d\n", incrementsNeeded)
		validatorSet.IncrementProposerPriority(incrementsNeeded)
	}

	return bor.ValidateHeaderTime(header, now, parent, validatorSet, htv.borConfig, htv.signaturesCache)
}

func (htv *headerTimeValidator) ValidateHeaderTimeV2(header *types.Header, now time.Time, parent *types.Header) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var validatorSet *valset.ValidatorSet
	spanId := heimdall.SpanIdAt(header.Number.Uint64())
	for id := uint64(0); id <= uint64(spanId); id++ {
		span, err := htv.heimdallClient.FetchSpan(ctx, id)
		if err != nil {
			return err
		}
		if validatorSet == nil {
			validatorSet = valset.NewValidatorSet(span.Producers())
		} else {
			validatorSet = bor.GetUpdatedValidatorSet(validatorSet, span.Producers(), log.New())
		}
		headerNum := min(span.EndBlock, header.Number.Uint64())
		currentSprintNumber := htv.borConfig.CalculateSprintNumber(headerNum)
		firstSprintNumber := htv.borConfig.CalculateSprintNumber(span.StartBlock)
		incrementsNeeded := int(currentSprintNumber-firstSprintNumber) + 1
		validatorSet.IncrementProposerPriority(incrementsNeeded)
	}

	return bor.ValidateHeaderTime(header, now, parent, validatorSet, htv.borConfig, htv.signaturesCache)
}
