// Package demand provides core functionality for the AIStore on-demand extended actions.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package demand

import (
	"math"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/housekeep/hk"
)

var (
	// Default demand xaction idle timeout: how long the xaction must live after
	// the end of the last request.
	xactIdleTimeout = 2 * time.Minute
)

type (
	//
	// xaction that self-terminates after staying idle for a while
	// with an added capability to renew itself and ref-count its pending work
	//
	XactDemand interface {
		cmn.Xact
		IdleTimer() <-chan struct{}
		Renew()
		IncPending()
		DecPending()
		SubPending(n int)
	}

	idleInfo struct {
		uuid     string
		dur      time.Duration
		ticks    *cmn.StopCh
		deadline atomic.Int64
	}

	XactDemandBase struct {
		cmn.XactBase
		pending atomic.Int64

		idle idleInfo
	}
)

var (
	_ XactDemand = &XactDemandBase{}
)

//
// XactDemandBase - partially implements XactDemand interface
//

func NewXactDemandBase(kind string, bck cmn.Bck, idleTimes ...time.Duration) *XactDemandBase {
	idleTime := xactIdleTimeout
	if len(idleTimes) != 0 {
		idleTime = idleTimes[0]
	}
	uuid := cmn.GenUUID()
	r := &XactDemandBase{
		XactBase: *cmn.NewXactBaseWithBucket("", kind, bck),
		idle: idleInfo{
			uuid:  uuid,
			dur:   idleTime,
			ticks: cmn.NewStopCh(),
		},
	}
	r.idle.deadline.Store(mono.NanoTime() + int64(idleTime))

	hk.Housekeeper.Register(uuid, func() time.Duration {
		// When deadline is met then we must send a tick (close channel).
		if r.idle.deadline.Load() < mono.NanoTime() {
			r.idle.ticks.Close()
		}
		return idleTime
	})
	return r
}

func (r *XactDemandBase) IdleTimer() <-chan struct{} { return r.idle.ticks.Listen() }

func (r *XactDemandBase) Renew() {
	pending := r.Pending()
	debug.Assert(pending >= 0)
	if pending == 0 {
		// If there are no requests yet and renew was issued then we will wait
		// `r.idle.dur` for some request to come.
		r.startIdleTimer()
	}
}

func (r *XactDemandBase) IncPending() {
	if r.pending.Inc() == 1 {
		// Set deadline to infinity so that we never met it. It will be reset
		// back to `r.idle.dur` when number of pending will drop to 0.
		r.idle.deadline.Store(math.MaxInt64)
	}
}

func (r *XactDemandBase) DecPending() { r.SubPending(1) }

func (r *XactDemandBase) SubPending(n int) {
	if r.pending.Sub(int64(n)) == 0 {
		r.startIdleTimer()
	}
	debug.Assert(r.Pending() >= 0)
}

func (r *XactDemandBase) Pending() int64 { return r.pending.Load() }

func (r *XactDemandBase) Stop() {
	hk.Housekeeper.Unregister(r.idle.uuid)
	r.idle.ticks.Close()
}

func (r *XactDemandBase) startIdleTimer() {
	r.idle.deadline.Store(mono.NanoTime() + int64(r.idle.dur))
}
