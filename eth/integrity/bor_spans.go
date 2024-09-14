package integrity

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/turbo/services"
)

func BorSpansCheck(ctx context.Context, logger log.Logger, blockReader services.FullBlockReader, failFast bool, heimdallUrl string) error {
	logTicker := time.NewTicker(5 * time.Second)
	defer logTicker.Stop()

	heimdallClient := heimdall.NewHeimdallClient(heimdallUrl, logger)
	to := blockReader.LastFrozenSpanId()
	logger.Info("starting bor spans check", "to", to, "heimdallUrl", heimdallUrl)
	var diffs []uint64
	for spanId := uint64(0); spanId <= to; spanId++ {
		select {
		case <-logTicker.C:
			logger.Info("bor spans check periodic progress update", "spanId", spanId, "to", to)
		default: // no-op
		}

		spanBytesFromSnapshots, err := blockReader.Span(ctx, nil /* tx */, spanId)
		if err != nil {
			return err
		}

		spanFromSource, err := heimdallClient.FetchSpan(ctx, spanId)
		if err != nil {
			return err
		}

		spanBytesFromSource, err := json.Marshal(spanFromSource)
		if err != nil {
			return err
		}

		if bytes.Equal(spanBytesFromSnapshots, spanBytesFromSource) {
			continue
		}

		logger.Warn("found span mismatch", "spanId", spanId)
		diffs = append(diffs, spanId)
		if failFast {
			break
		}
	}
	if len(diffs) > 0 {
		err := fmt.Errorf("spans in snapshots differ from source for: %v", diffs)
		logger.Error(err.Error())
		return err
	}

	return nil
}
