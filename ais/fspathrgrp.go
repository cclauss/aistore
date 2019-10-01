// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
)

type (
	// implements fs.PathRunGroup interface
	fsprungroup struct {
		sync.Mutex
		t       *targetrunner
		runners map[int64]fs.PathRunner // subgroup of the nodeCtx.runners rungroup
		nextid  atomic.Int64
	}
)

func (g *fsprungroup) init(t *targetrunner) {
	g.t = t
	g.runners = make(map[int64]fs.PathRunner, 8)
	g.nextid.Store(time.Now().UTC().UnixNano() & 0xfff)
}

func (g *fsprungroup) UID() int64 { return g.nextid.Add(1) }

func (g *fsprungroup) Reg(r fs.PathRunner) {
	g.Lock()
	_, ok := g.runners[r.ID()]
	cmn.Assert(!ok)
	r.SetID(g.UID())
	g.runners[r.ID()] = r
	g.Unlock()
}

func (g *fsprungroup) Unreg(r fs.PathRunner) {
	g.Lock()
	_, ok := g.runners[r.ID()]
	cmn.Assert(ok)
	delete(g.runners, r.ID())
	g.Unlock()
}

// enableMountpath enables mountpath and notifies necessary runners about the
// change if mountpath actually was disabled.
func (g *fsprungroup) enableMountpath(mpath string) (enabled bool, err error) {
	enabled, err = fs.Mountpaths.Enable(mpath)
	if err != nil || !enabled {
		return
	}

	for _, r := range g.runners {
		r.ReqEnableMountpath(mpath)
	}
	go g.t.rebManager.runLocalReb(false /*skipGlobMisplaced*/)
	g.checkEnable("Enabled", mpath)
	return
}

// disableMountpath disables mountpath and notifies necessary runners about the
// change if mountpath actually was disabled.
func (g *fsprungroup) disableMountpath(mpath string) (disabled bool, err error) {
	disabled, err = fs.Mountpaths.Disable(mpath)
	if !disabled || err != nil {
		return disabled, err
	}

	for _, r := range g.runners {
		r.ReqDisableMountpath(mpath)
	}
	if !g.checkDisable("Disabled") {
		go g.t.rebManager.runLocalReb(false /*skipGlobMisplaced*/)
	}
	return true, nil
}

// addMountpath adds mountpath and notifies necessary runners about the change
// if the mountpath was actually added.
func (g *fsprungroup) addMountpath(mpath string) (err error) {
	if err = fs.Mountpaths.Add(mpath); err != nil {
		return
	}
	if err = fs.Mountpaths.CreateBucketDir(cmn.AIS); err != nil {
		return
	}
	if err = fs.Mountpaths.CreateBucketDir(cmn.Cloud); err != nil {
		return
	}

	for _, r := range g.runners {
		r.ReqAddMountpath(mpath)
	}
	go g.t.rebManager.runLocalReb(false /*skipGlobMisplaced*/)
	g.checkEnable("Added", mpath)
	return
}

// removeMountpath removes mountpath and notifies necessary runners about the
// change if the mountpath was actually removed.
func (g *fsprungroup) removeMountpath(mpath string) (err error) {
	if err = fs.Mountpaths.Remove(mpath); err != nil {
		return
	}
	for _, r := range g.runners {
		r.ReqRemoveMountpath(mpath)
	}
	if !g.checkDisable("Removed") {
		go g.t.rebManager.runLocalReb(false /*skipGlobMisplaced*/)
	}
	return
}

// check for no-mounpaths and UNREGISTER
func (g *fsprungroup) checkDisable(action string) (disabled bool) {
	availablePaths, _ := fs.Mountpaths.Get()
	if len(availablePaths) > 0 {
		return false
	}
	if err := g.t.disable(); err != nil {
		glog.Errorf("%s the last available mountpath, failed to unregister target %s (self), err: %v", action, g.t.si, err)
	} else {
		glog.Errorf("%s the last available mountpath and unregistered target %s (self)", action, g.t.si)
	}
	return true
}

func (g *fsprungroup) checkEnable(action, mpath string) {
	availablePaths, _ := fs.Mountpaths.Get()
	if len(availablePaths) > 1 {
		glog.Infof("%s mountpath %s", action, mpath)
	} else {
		glog.Infof("%s the first mountpath %s", action, mpath)
		if err := g.t.enable(); err != nil {
			glog.Errorf("Failed to re-register %s (self), err: %v", g.t.si, err)
		}
	}
}
