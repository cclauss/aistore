// Package demand provides core functionality for the AIStore on-demand extended actions.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package demand

import (
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
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
		IncPending()
		DecPending()
		SubPending(n int)
	}

	idleInfo struct {
		dur   time.Duration
		ticks *cmn.StopCh
		cnt   int
	}

	XactDemandBase struct {
		cmn.XactBase
		pending atomic.Int64
		active  atomic.Int64
		hkname  string
		idle    idleInfo
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
	r := &XactDemandBase{
		XactBase: *cmn.NewXactBaseWithBucket("", kind, bck),
		hkname:   kind + "/" + cmn.GenUUID(),
		idle: idleInfo{
			dur:   idleTime,
			ticks: cmn.NewStopCh(),
		},
	}

	hk.Housekeeper.Register(r.hkname, func() time.Duration {
		active := r.active.Swap(0)
		if r.Pending() > 0 || active > 0 {
			r.idle.cnt = 0
		} else if active == 0 {
			r.idle.cnt++
			if r.idle.cnt == 1 {
				return r.idle.dur / 2 // wait another half interval to eliminate on/off "flickering"
			}
			r.idle.ticks.Close() // idle-ness confirmed - close channel
		}
		return r.idle.dur
	})
	return r
}

func (r *XactDemandBase) IdleTimer() <-chan struct{} { return r.idle.ticks.Listen() }
func (r *XactDemandBase) Pending() int64             { return r.pending.Load() }
func (r *XactDemandBase) IncPending()                { r.pending.Inc(); r.active.Inc() }
func (r *XactDemandBase) DecPending()                { r.SubPending(1) }

func (r *XactDemandBase) SubPending(n int) {
	r.pending.Sub(int64(n))
	debug.Assert(r.Pending() >= 0)
}

func (r *XactDemandBase) Stop() {
	hk.Housekeeper.Unregister(r.hkname)
	r.idle.ticks.Close()
}
