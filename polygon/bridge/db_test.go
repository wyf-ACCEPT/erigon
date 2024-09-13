package bridge

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/polygon/polygoncommon"
	"github.com/erigontech/erigon/turbo/testlog"
)

func TestXyz(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	logger := testlog.Logger(t, log.LvlTrace)
	db := polygoncommon.NewDatabase(t.TempDir(), kv.PolygonBridgeDB, databaseTablesCfg, logger, false /* accede */)
	err := db.OpenOnce(ctx)
	require.NoError(t, err)
	store := NewStore(db)
	heimdallClient := heimdall.NewHeimdallClient("https://heimdall-api-amoy.polygon.technology", logger)
	bridge := NewBridge(store, logger, params.AmoyChainConfig.Bor.(*borcfg.BorConfig), heimdallClient, nil)

	event, err := heimdallClient.FetchStateSyncEvent(ctx, 2894)
	require.NoError(t, err)
	err = store.PutEvents(ctx, []*heimdall.EventRecordWithTime{event})
	require.NoError(t, err)
	err = store.PutBlockNumToEventID(ctx, map[uint64]uint64{
		11925392: 2893,
	})
	require.NoError(t, err)

	bridge.reachedTip.Store(true)
	bridge.lastProcessedBlockInfo.Store(&ProcessedBlockInfo{
		BlockNum:  11928816,
		BlockTime: 1726246593,
	})
	err = bridge.ProcessNewBlocks(ctx, []*types.Block{
		types.NewBlockWithHeader(&types.Header{
			Number: big.NewInt(11928832),
			Time:   1726246627,
		}),
	})
	require.NoError(t, err)
}
