// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport"
	"github.com/klauspost/reedsolomon"
)

// to avoid starving ecencode xaction, allow to run ecencode after every put batch
const putBatchSize = 8

// a mountpath putJogger: processes PUT/DEL requests to one mountpath
type putJogger struct {
	parent *XactPut
	slab   *memsys.Slab
	buffer []byte
	mpath  string

	putCh  chan *Request // top priority operation (object PUT)
	xactCh chan *Request // low priority operation (ec-encode)
	stopCh chan struct{} // jogger management channel: to stop it

	toDisk bool // use files or SGL
}

func (c *putJogger) freeResources() {
	c.slab.Free(c.buffer)
	c.buffer = nil
	c.slab = nil
}

func (c *putJogger) processRequest(req *Request) {
	ecConf := req.LOM.Bprops().EC
	c.parent.stats.updateWaitTime(time.Since(req.tm))
	memRequired := req.LOM.Size() * int64(ecConf.DataSlices+ecConf.ParitySlices) / int64(ecConf.ParitySlices)
	c.toDisk = useDisk(memRequired)
	req.tm = time.Now()
	err := c.ec(req)
	c.parent.DecPending()
	if req.Callback != nil {
		req.Callback(req.LOM, err)
	}
}

func (c *putJogger) run() {
	glog.Infof("Started EC for mountpath: %s, bucket %s", c.mpath, c.parent.bck)
	c.buffer, c.slab = mm.Alloc()
	putsDone := 0

	for {
		// first, process requests with high priority
		select {
		case req := <-c.putCh:
			c.processRequest(req)
			// repeat in case of more objects in the HIGH-priority queue
			putsDone++
			if putsDone < putBatchSize {
				continue
			}
		case <-c.stopCh:
			c.freeResources()
			return
		default:
		}

		putsDone = 0
		// process all other requests
		select {
		case req := <-c.putCh:
			c.processRequest(req)
		case req := <-c.xactCh:
			c.processRequest(req)
		case <-c.stopCh:
			c.freeResources()
			return
		}
	}
}

func (c *putJogger) stop() {
	glog.Infof("Stopping EC for mountpath: %s, bucket %s", c.mpath, c.parent.bck)
	c.stopCh <- struct{}{}
	close(c.stopCh)
}

// starts EC process
func (c *putJogger) ec(req *Request) error {
	var (
		err error
		act = "encoding"
	)

	switch req.Action {
	case ActSplit:
		err = c.encode(req)
		c.parent.stats.updateEncodeTime(time.Since(req.tm), err != nil)
	case ActDelete:
		err = c.cleanup(req)
		act = "cleaning up"
		c.parent.stats.updateDeleteTime(time.Since(req.tm), err != nil)
	default:
		err = fmt.Errorf("invalid EC action for putJogger: %v", req.Action)
	}

	if err != nil {
		glog.Errorf("Error %s object [%s/%s], fqn: %q, err: %v",
			act, req.LOM.Bck(), req.LOM.ObjName, req.LOM.FQN, err)
	}

	if req.ErrCh != nil {
		req.ErrCh <- err
		close(req.ErrCh)
	}
	if err == nil {
		c.parent.stats.updateObjTime(time.Since(req.putTime))
	}
	return err
}

// calculates and stores data and parity slices
func (c *putJogger) encode(req *Request) error {
	if glog.V(4) {
		glog.Infof("Encoding %q...", req.LOM.FQN)
	}
	var (
		cksumValue, cksumType string
		ecConf                = req.LOM.Bprops().EC
	)
	if req.LOM.Cksum() != nil {
		cksumType, cksumValue = req.LOM.Cksum().Get()
	}
	meta := &Metadata{
		Size:      req.LOM.Size(),
		Data:      ecConf.DataSlices,
		Parity:    ecConf.ParitySlices,
		IsCopy:    req.IsCopy,
		ObjCksum:  cksumValue,
		CksumType: cksumType,
	}

	// calculate the number of targets required to encode the object
	// For replicated: ParitySlices + original object
	// For encoded: ParitySlices + DataSlices + original object
	reqTargets := ecConf.ParitySlices + 1
	if !req.IsCopy {
		reqTargets += ecConf.DataSlices
	}
	targetCnt := len(c.parent.smap.Get().Tmap)
	if targetCnt < reqTargets {
		return fmt.Errorf("object %s/%s requires %d targets to encode, only %d found",
			req.LOM.Bck(), req.LOM.ObjName, reqTargets, targetCnt)
	}

	// Save metadata before encoding the object
	ctMeta := cluster.NewCTFromLOM(req.LOM, MetaType)
	metaBuf := bytes.NewReader(meta.Marshal())
	if err := ctMeta.Write(c.parent.t, metaBuf, -1); err != nil {
		return err
	}

	c.parent.ObjectsInc()
	c.parent.BytesAdd(req.LOM.Size())

	// if an object is small just make `parity` copies
	if meta.IsCopy {
		if err := c.createCopies(req, meta); err != nil {
			c.cleanup(req)
		}
		return nil
	}

	// big object is erasure encoded
	if slices, err := c.sendSlices(req, meta); err != nil {
		freeSlices(slices)
		c.cleanup(req)
	}
	return nil
}

func (c *putJogger) ctSendCallback(hdr transport.Header, _ io.ReadCloser, _ unsafe.Pointer, err error) {
	c.parent.t.GetSmallMMSA().Free(hdr.Opaque)
	if err != nil {
		glog.Errorf("failed to send o[%s/%s], err: %v", hdr.Bck, hdr.ObjName, err)
	}
}

// a client has deleted the main object and requested to cleanup all its
// replicas and slices
// Just remove local metafile if it exists and broadcast the request to all
func (c *putJogger) cleanup(req *Request) error {
	fqnMeta, _, err := cluster.HrwFQN(req.LOM.Bck(), MetaType, req.LOM.ObjName)
	if err != nil {
		glog.Errorf("Failed to get path for metadata of %s/%s: %v", req.LOM.Bck(), req.LOM.ObjName, err)
		return nil
	}

	if err := os.RemoveAll(fqnMeta); err != nil {
		// logs the error but move on - notify all other target to do cleanup
		glog.Errorf("Error removing metafile %q", fqnMeta)
	}

	mm := c.parent.t.GetSmallMMSA()
	request := c.parent.newIntraReq(reqDel, nil).NewPack(mm)
	hdr := transport.Header{
		Bck:     req.LOM.Bck().Bck,
		ObjName: req.LOM.ObjName,
		Opaque:  request,
		ObjAttrs: transport.ObjectAttrs{
			Size: 0,
		},
	}
	return c.parent.reqBundle.Send(transport.Obj{Hdr: hdr, Callback: c.ctSendCallback}, nil)
}

// Sends object replicas to targets that must have replicas after the client
// uploads the main replica
func (c *putJogger) createCopies(req *Request, metadata *Metadata) error {
	var (
		copies = req.LOM.Bprops().EC.ParitySlices
	)

	// generate a list of target to send the replica (all excluding this one)
	targets, err := cluster.HrwTargetList(req.LOM.Uname(), c.parent.smap.Get(), copies+1)
	if err != nil {
		return err
	}
	targets = targets[1:]

	// Because object encoding is called after the main replica is saved to
	// disk it needs to read it from the local storage
	fh, err := cmn.NewFileHandle(req.LOM.FQN)
	if err != nil {
		return err
	}

	nodes := make([]string, 0, len(targets))
	for _, tgt := range targets {
		nodes = append(nodes, tgt.ID())
	}

	// broadcast the replica to the targets
	cb := func(hdr transport.Header, reader io.ReadCloser, _ unsafe.Pointer, err error) {
		if err != nil {
			glog.Errorf("Failed to to %v: %v", nodes, err)
		}
	}
	src := &dataSource{
		reader:   fh,
		size:     req.LOM.Size(),
		metadata: metadata,
		reqType:  reqPut,
	}
	err = c.parent.writeRemote(nodes, req.LOM, src, cb)

	return err
}

// Fills slices with calculated checksums, reports errors to errCh
func checksumDataSlices(slices []*slice, wg *sync.WaitGroup, errCh chan error, cksmReaders []io.Reader,
	cksumType string, sliceSize int64) {
	defer wg.Done()
	buf, slab := mm.Alloc(sliceSize)
	defer slab.Free(buf)
	for i, reader := range cksmReaders {
		_, cksum, err := cmn.CopyAndChecksum(ioutil.Discard, reader, buf, cksumType)
		if err != nil {
			errCh <- fmt.Errorf("failure computing checksum of a slice: %s", err)
			return
		}
		slices[i].cksum = cksum.Clone()
	}
}

// TODO -- FIXME: copy/paste vs generateSlicesToDisk

// generateSlicesToMemory gets FQN to the original file and encodes it into EC slices
// * fqn - the path to original object
// * dataSlices - the number of data slices
// * paritySlices - the number of parity slices
// Returns:
// * SGL that hold all the objects data
// * constructed from the main object slices
func generateSlicesToMemory(lom *cluster.LOM, dataSlices, paritySlices int) (cmn.ReadOpenCloser, []*slice, error) {
	var (
		sgl      *memsys.SGL
		totalCnt = paritySlices + dataSlices
		slices   = make([]*slice, totalCnt)
		conf     = lom.CksumConf()
		cksums   []*cmn.CksumHash
	)

	// read the object into memory
	sgl, err := readFile(lom)
	if err != nil {
		return sgl, slices, err
	}
	fileSize := lom.Size()

	sliceSize := SliceSize(fileSize, dataSlices)
	padSize := sliceSize*int64(dataSlices) - fileSize
	initSize := cmn.MinI64(sliceSize, cmn.MiB)

	// make the last slice the same size as the others by padding with 0's
	for padSize > 0 {
		byteCnt := cmn.Min(int(padSize), len(slicePadding))
		padding := slicePadding[:byteCnt]
		if _, err = sgl.Write(padding); err != nil {
			return sgl, slices, err
		}
		padSize -= int64(byteCnt)
	}

	// readers are slices of original object(no memory allocated)
	readers := make([]io.Reader, dataSlices)
	cksmReaders := make([]io.Reader, dataSlices)

	// writers are slices created by EC encoding process(memory is allocated)
	sliceWriters := make([]io.Writer, paritySlices)

	for i := 0; i < dataSlices; i++ {
		reader := memsys.NewSliceReader(sgl, int64(i)*sliceSize, sliceSize)
		slices[i] = &slice{reader: reader}
		readers[i] = reader
		cksmReaders[i] = memsys.NewSliceReader(sgl, int64(i)*sliceSize, sliceSize)
	}

	// We have established readers of data slices, we can already start calculating hashes for them
	// during calculating parity slices and their hashes
	wgCksmReaders := &sync.WaitGroup{}
	wgCksmReaders.Add(1)
	errCksmCh := make(chan error, 1)
	if conf.Type != cmn.ChecksumNone {
		go checksumDataSlices(slices, wgCksmReaders, errCksmCh, cksmReaders, conf.Type, sliceSize)
		cksums = make([]*cmn.CksumHash, paritySlices)
	}
	for i := 0; i < paritySlices; i++ {
		writer := mm.NewSGL(initSize)
		slices[i+dataSlices] = &slice{obj: writer}
		if conf.Type == cmn.ChecksumNone {
			sliceWriters[i] = writer
		} else {
			cksums[i] = cmn.NewCksumHash(conf.Type)
			sliceWriters[i] = cmn.NewWriterMulti(writer, cksums[i].H)
		}
	}

	stream, err := reedsolomon.NewStreamC(dataSlices, paritySlices, true, true)
	if err != nil {
		return sgl, slices, err
	}

	// Calculate parity slices and their checksums
	if err := stream.Encode(readers, sliceWriters); err != nil {
		return sgl, slices, err
	}

	if conf.Type != cmn.ChecksumNone {
		for i := range cksums {
			cksums[i].Finalize()
			slices[i+dataSlices].cksum = cksums[i].Clone()
		}
	}

	wgCksmReaders.Wait()
	close(errCksmCh)
	err = <-errCksmCh

	return sgl, slices, err
}

// TODO -- FIXME: copy/paste vs generateSlicesToMemory

// generateSlicesToDisk gets FQN to the original file and encodes it into EC slices
// * fqn - the path to original object
// * dataSlices - the number of data slices
// * paritySlices - the number of parity slices
// Returns:
// * Main object file handle
// * constructed from the main object slices
func generateSlicesToDisk(lom *cluster.LOM, dataSlices, paritySlices int) (cmn.ReadOpenCloser, []*slice, error) {
	var (
		fh       *cmn.FileHandle
		fqn      = lom.FQN
		totalCnt = paritySlices + dataSlices
		slices   = make([]*slice, totalCnt)
		conf     = lom.CksumConf()
		cksums   []*cmn.CksumHash
	)

	stat, err := os.Stat(fqn)
	if err != nil {
		return fh, slices, err
	}
	fileSize := stat.Size()

	fh, err = cmn.NewFileHandle(fqn)
	if err != nil {
		return fh, slices, err
	}

	sliceSize := SliceSize(fileSize, dataSlices)
	padSize := sliceSize*int64(dataSlices) - fileSize

	// readers are slices of original object(no memory allocated)
	readers := make([]io.Reader, dataSlices)
	cksmReaders := make([]io.Reader, dataSlices)

	// writers are slices created by EC encoding process(memory is allocated)
	// hashes are writers, which calculate hash when their're written to
	// sliceWriters combine writers and hashes to calculate slices and hashes at the same time
	writers := make([]io.Writer, paritySlices)
	sliceWriters := make([]io.Writer, paritySlices)

	sizeLeft := fileSize
	for i := 0; i < dataSlices; i++ {
		var (
			reader     cmn.ReadOpenCloser
			cksmReader cmn.ReadOpenCloser
			err        error
		)
		if sizeLeft < sliceSize {
			reader, err = cmn.NewFileSectionHandle(fh, int64(i)*sliceSize, sizeLeft, padSize)
			cksmReader, _ = cmn.NewFileSectionHandle(fh, int64(i)*sliceSize, sizeLeft, padSize)
		} else {
			reader, err = cmn.NewFileSectionHandle(fh, int64(i)*sliceSize, sliceSize, 0)
			cksmReader, _ = cmn.NewFileSectionHandle(fh, int64(i)*sliceSize, sliceSize, 0)
		}
		if err != nil {
			return fh, slices, err
		}
		slices[i] = &slice{obj: fh, reader: reader}
		readers[i] = reader
		cksmReaders[i] = cksmReader
		sizeLeft -= sliceSize
	}

	// We have established readers of data slices, we can already start calculating hashes for them
	// during calculating parity slices and their hashes
	wgCksmReaders := &sync.WaitGroup{}
	wgCksmReaders.Add(1)
	errChCksm := make(chan error, 1)
	if conf.Type != cmn.ChecksumNone {
		go checksumDataSlices(slices, wgCksmReaders, errChCksm, cksmReaders, conf.Type, sliceSize)
		cksums = make([]*cmn.CksumHash, paritySlices)
	}
	for i := 0; i < paritySlices; i++ {
		workFQN := fs.CSM.GenContentFQN(fqn, fs.WorkfileType, fmt.Sprintf("ec-write-%d", i))
		writer, err := lom.CreateFile(workFQN)
		if err != nil {
			return fh, slices, err
		}
		slices[i+dataSlices] = &slice{writer: writer, workFQN: workFQN}
		writers[i] = writer
		if conf.Type == cmn.ChecksumNone {
			sliceWriters[i] = writer
		} else {
			cksums[i] = cmn.NewCksumHash(conf.Type)
			sliceWriters[i] = cmn.NewWriterMulti(writer, cksums[i].H)
		}
	}

	stream, err := reedsolomon.NewStreamC(dataSlices, paritySlices, true, true)
	if err != nil {
		return fh, slices, err
	}

	// Calculate parity slices and their checksums
	if err := stream.Encode(readers, sliceWriters); err != nil {
		return fh, slices, err
	}

	if conf.Type != cmn.ChecksumNone {
		for i := range cksums {
			cksums[i].Finalize()
			slices[i+dataSlices].cksum = cksums[i].Clone()
		}
	}

	wgCksmReaders.Wait()
	close(errChCksm)
	err = <-errChCksm

	for _, wr := range writers {
		// writer can be only *os.File within this function
		f, ok := wr.(*os.File)
		cmn.Assert(ok)
		debug.AssertNoErr(f.Close())
	}
	return fh, slices, err
}

// copies the constructed EC slices to remote targets
// * req - original request
// * meta - EC metadata
// Returns:
// * list of all slices, sent to targets
func (c *putJogger) sendSlices(req *Request, meta *Metadata) ([]*slice, error) {
	ecConf := req.LOM.Bprops().EC
	totalCnt := ecConf.ParitySlices + ecConf.DataSlices

	// totalCnt+1: first node gets the full object, other totalCnt nodes
	// gets a slice each
	targets, err := cluster.HrwTargetList(req.LOM.Uname(), c.parent.smap.Get(), totalCnt+1)
	if err != nil {
		return nil, err
	}

	// load the data slices from original object and construct parity ones
	var (
		objReader cmn.ReadOpenCloser
		slices    []*slice
	)
	if c.toDisk {
		objReader, slices, err = generateSlicesToDisk(req.LOM, ecConf.DataSlices, ecConf.ParitySlices)
	} else {
		objReader, slices, err = generateSlicesToMemory(req.LOM, ecConf.DataSlices, ecConf.ParitySlices)
	}

	if err != nil {
		freeObject(objReader)
		freeSlices(slices)
		return nil, err
	}

	wg := sync.WaitGroup{}
	ch := make(chan error, totalCnt)
	mainObj := &slice{refCnt: *atomic.NewInt32(int32(ecConf.DataSlices)), obj: objReader}
	sliceSize := SliceSize(req.LOM.Size(), ecConf.DataSlices)

	// transfer a slice to remote target
	// If the slice is data one - no immediate cleanup is required because this
	// slice is just a reader of global SGL for the entire file (that is why a
	// counter is used here)
	copySlice := func(i int) {
		defer wg.Done()

		var data *slice
		if i < ecConf.DataSlices {
			// the slice is just a reader that does not allocate new memory
			data = mainObj
		} else {
			// the slice uses its own SGL, so the counter is 1
			data = &slice{refCnt: *atomic.NewInt32(1), obj: slices[i].obj, workFQN: slices[i].workFQN}
		}

		// In case of data slice, reopen its reader, because it was read
		// to the end by erasure encoding while calculating parity slices
		var (
			reader cmn.ReadOpenCloser
			err    error
		)
		if slices[i].reader != nil {
			reader = slices[i].reader
			switch r := reader.(type) {
			case *memsys.SliceReader:
				_, err = r.Seek(0, io.SeekStart)
			case *memsys.Reader:
				_, err = r.Seek(0, io.SeekStart)
			case *cmn.FileSectionHandle:
				_, err = r.Open()
			default:
				cmn.AssertFmt(false, "unsupported reader type", reader)
			}
		} else {
			if sgl, ok := slices[i].obj.(*memsys.SGL); ok {
				reader = memsys.NewReader(sgl)
			} else if slices[i].workFQN != "" {
				reader, err = cmn.NewFileHandle(slices[i].workFQN)
			} else {
				cmn.AssertFmt(false, "unsupported reader type", slices[i].obj)
			}
		}
		if err != nil {
			ch <- fmt.Errorf("failed to reset reader: %v", err)
			return
		}

		mcopy := *meta
		mcopy.SliceID = i + 1
		mcopy.ObjVersion = req.LOM.Version()
		if mcopy.SliceID != 0 && slices[i].cksum != nil {
			mcopy.CksumType, mcopy.CksumValue = slices[i].cksum.Get()
		}

		src := &dataSource{
			reader:   reader,
			size:     sliceSize,
			obj:      data,
			metadata: &mcopy,
			isSlice:  true,
			reqType:  reqPut,
		}

		// Put in lom actual object's checksum. It will be stored in slice's xattrs on dest target
		lom := *req.LOM
		err = c.parent.writeRemote([]string{targets[i+1].ID()}, &lom, src, nil)
		if err != nil {
			ch <- err
			return
		}
	}

	for i := 0; i < totalCnt; i++ {
		wg.Add(1)
		go copySlice(i)
	}

	wg.Wait()
	close(ch)

	if err, ok := <-ch; ok {
		var s string
		if ecConf.DataSlices > 1 {
			s = "s"
		}
		glog.Errorf("Error while copying %d slice%s (with parity=%d) for %q: %v",
			ecConf.DataSlices, s, ecConf.ParitySlices, req.LOM.FQN, err)
	} else if glog.V(4) {
		glog.Infof("EC created %d slices (with %d parity) for %q: %v",
			ecConf.DataSlices, ecConf.ParitySlices, req.LOM.FQN, err)
	}

	return slices, nil
}
