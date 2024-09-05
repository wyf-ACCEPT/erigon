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

package committee_subscription

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	sentinel "github.com/erigontech/erigon-lib/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/aggregation"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/network/subnets"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
)

var (
	ErrIgnore                   = errors.New("ignore")
	ErrCommitteeIndexOutOfRange = errors.New("committee index out of range")
	ErrWrongSubnet              = errors.New("attestation is for the wrong subnet")
	ErrNotInPropagationRange    = fmt.Errorf("attestation is not in propagation range. %w", ErrIgnore)
	ErrEpochMismatch            = errors.New("epoch mismatch")
	ErrExactlyOneBitSet         = errors.New("exactly one aggregation bit should be set")
	ErrAggregationBitsMismatch  = errors.New("aggregation bits mismatch committee size")
)

type CommitteeSubscribeMgmt struct {
	indiciesDB   kv.RoDB
	ethClock     eth_clock.EthereumClock
	beaconConfig *clparams.BeaconChainConfig
	netConfig    *clparams.NetworkConfig
	sentinel     sentinel.SentinelClient
	state        *state.CachingBeaconState
	syncedData   *synced_data.SyncedDataManager
	// subscriptions
	aggregationPool    aggregation.AggregationPool
	committeeSubsMutex sync.RWMutex
	committeeSubs      map[uint64]*committeeSub // committeeIndex -> validatorSub
}

func NewCommitteeSubscribeManagement(
	ctx context.Context,
	indiciesDB kv.RoDB,
	beaconConfig *clparams.BeaconChainConfig,
	netConfig *clparams.NetworkConfig,
	ethClock eth_clock.EthereumClock,
	sentinel sentinel.SentinelClient,
	state *state.CachingBeaconState,
	aggregationPool aggregation.AggregationPool,
	syncedData *synced_data.SyncedDataManager,
) *CommitteeSubscribeMgmt {
	c := &CommitteeSubscribeMgmt{
		indiciesDB:      indiciesDB,
		beaconConfig:    beaconConfig,
		netConfig:       netConfig,
		ethClock:        ethClock,
		sentinel:        sentinel,
		state:           state,
		aggregationPool: aggregationPool,
		syncedData:      syncedData,
		committeeSubs:   make(map[uint64]*committeeSub),
	}
	go c.sweepStaleAggregatorSlots(ctx)
	return c
}

type committeeSub struct {
	aggregatorSlots   map[uint64]struct{}
	largestTargetSlot uint64
}

func (c *CommitteeSubscribeMgmt) AddAttestationSubscription(ctx context.Context, p *cltypes.BeaconCommitteeSubscription) error {
	var (
		slot   = p.Slot
		cIndex = p.CommitteeIndex
	)
	headState := c.syncedData.HeadState()
	if headState == nil {
		return errors.New("head state not available")
	}

	log.Debug("Add attestation subscription", "slot", slot, "committeeIndex", cIndex, "isAggregator", p.IsAggregator, "validatorIndex", p.ValidatorIndex)

	// add subscription
	c.committeeSubsMutex.Lock()
	defer c.committeeSubsMutex.Unlock()

	if _, ok := c.committeeSubs[cIndex]; !ok {
		c.committeeSubs[cIndex] = &committeeSub{
			aggregatorSlots:   make(map[uint64]struct{}),
			largestTargetSlot: 0,
		}
	}
	if p.IsAggregator {
		c.committeeSubs[cIndex].aggregatorSlots[slot] = struct{}{}
	}

	// set expiration time to target slot if needed
	curSlot := c.ethClock.GetCurrentSlot()
	if c.committeeSubs[cIndex].largestTargetSlot < slot && curSlot <= slot {
		c.committeeSubs[cIndex].largestTargetSlot = slot
		// set sentinel gossip expiration by subnet id
		commiteePerSlot := headState.CommitteeCount(p.Slot / c.beaconConfig.SlotsPerEpoch)
		subnetId := subnets.ComputeSubnetForAttestation(commiteePerSlot, slot, cIndex, c.beaconConfig.SlotsPerEpoch, c.netConfig.AttestationSubnetCount)
		expiryTime := time.Now().Add(time.Duration((slot-curSlot+1)*c.beaconConfig.SecondsPerSlot) * time.Second)
		request := sentinel.RequestSubscribeExpiry{
			Topic:          gossip.TopicNameBeaconAttestation(subnetId),
			ExpiryUnixSecs: uint64(expiryTime.Unix()),
		}
		if _, err := c.sentinel.SetSubscribeExpiry(ctx, &request); err != nil {
			return err
		}
	}
	return nil
}

func (c *CommitteeSubscribeMgmt) AggregateAttestation(att *solid.Attestation) error {
	committeeIndex := att.AttestantionData().CommitteeIndex()
	c.committeeSubsMutex.RLock()
	defer c.committeeSubsMutex.RUnlock()
	if sub, ok := c.committeeSubs[committeeIndex]; ok {
		curSlot := c.ethClock.GetCurrentSlot()
		if _, exist := sub.aggregatorSlots[curSlot]; exist {
			// aggregate attestation
			if err := c.aggregationPool.AddAttestation(att); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *CommitteeSubscribeMgmt) NeedToAggregateNow(committeeIndex uint64) bool {
	c.committeeSubsMutex.RLock()
	defer c.committeeSubsMutex.RUnlock()
	if sub, ok := c.committeeSubs[committeeIndex]; ok {
		curSlot := c.ethClock.GetCurrentSlot()
		_, exist := sub.aggregatorSlots[curSlot]
		return exist
	}
	return false
}

func (c *CommitteeSubscribeMgmt) sweepStaleAggregatorSlots(ctx context.Context) {
	// check every epoch
	ticker := time.NewTicker(time.Duration(c.beaconConfig.SlotsPerEpoch*c.beaconConfig.SecondsPerSlot) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			curSlot := c.ethClock.GetCurrentSlot()
			c.committeeSubsMutex.Lock()
			for _, sub := range c.committeeSubs {
				for slot := range sub.aggregatorSlots {
					if curSlot > slot {
						delete(sub.aggregatorSlots, slot)
					}
				}
			}
			c.committeeSubsMutex.Unlock()
		}
	}
}
