/*
 * Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"bufio"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/golang/glog"
)

// getAndDrop reads until EOF and uses dummy writer (ReadToNull)
func getAndDrop(w http.ResponseWriter, r *http.Request, redirecturl string) error {
	if glog.V(3) {
		glog.Infof("GET redirect URL %q", redirecturl)
	}
	newr, err := http.Get(redirecturl)
	if err != nil {
		glog.Errorf("Failed to GET redirect URL %q, err: %v", redirecturl, err)
		return err
	}
	defer func() {
		if newr != nil {
			err = newr.Body.Close()
		}
	}()
	bufreader := bufio.NewReader(newr.Body)
	bytes, err := ReadToNull(bufreader)
	if err != nil {
		glog.Errorf("Failed to copy data to http response, URL %q, err: %v", redirecturl, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		stats := getproxystats()
		atomic.AddInt64(&stats.numerr, 1)
		return err
	}
	if glog.V(3) {
		glog.Infof("Received and discarded %q (size %.2f MB)", redirecturl, float64(bytes)/1000/1000)
	}
	return err
}

//===========================================================================
//
// proxy runner
//
//===========================================================================
type proxyrunner struct {
	httprunner
}

// run
func (p *proxyrunner) run() error {
	p.httprunner.init()
	//
	// REST API: register proxy handlers and start listening
	//
	p.httprunner.registerhdlr("/"+Rversion+"/"+Rfiles+"/", p.filehdlr)
	p.httprunner.registerhdlr("/"+Rversion+"/"+Rcluster, p.clusterhdlr)
	p.httprunner.registerhdlr("/"+Rversion+"/"+Rcluster+"/", p.clusterhdlr) // FIXME
	p.httprunner.registerhdlr("/", invalhdlr)
	return p.httprunner.run()
}

// stop gracefully
func (p *proxyrunner) stop(err error) {
	glog.Infof("Stopping %s, err: %v", p.name, err)
	//
	// give targets a limited chance to unregister
	//
	version := ctx.smap.version()
	for i := 0; i < 5; i++ {
		time.Sleep(time.Second)
		v := ctx.smap.version()
		if version != v {
			version = v
			time.Sleep(time.Second)
			continue
		}
		break
	}
	p.httprunner.stop(err)
}

//==============
//
// http handlers
//
//==============

// handler for: "/"+Rversion+"/"+Rfiles+"/"
func (p *proxyrunner) filehdlr(w http.ResponseWriter, r *http.Request) {
	assert(r.Method == http.MethodGet)
	stats := getproxystats()
	atomic.AddInt64(&stats.numget, 1)

	if ctx.smap.count() < 1 {
		s := errmsgRestApi("No registered targets yet", r)
		glog.Errorln(s)
		http.Error(w, s, http.StatusServiceUnavailable)
		atomic.AddInt64(&stats.numerr, 1)
		return
	}
	//
	// parse and validate
	//
	apitems := restApiItems(r.URL.Path, 5)
	if apitems = checkRestAPI(w, r, apitems, 1, Rversion, Rfiles); apitems == nil {
		atomic.AddInt64(&stats.numerr, 1)
		return
	}
	sid := hrwTarget(strings.Join(apitems, "/"))
	si := ctx.smap.get(sid)
	assert(si != nil, "race NIY")

	redirecturl := si.DirectURL + r.URL.Path
	if glog.V(3) {
		glog.Infof("Redirecting %q to %s", r.URL.Path, si.DirectURL)
	}
	if !ctx.config.Proxy.Passthru {
		glog.Infoln("Proxy will invoke the GET (ctx.config.Proxy.Passthru = false)")
		getAndDrop(w, r, redirecturl) // ignore error, proceed to http redirect
	}
	// FIXME: https, HTTP2 here and elsewhere
	http.Redirect(w, r, redirecturl, http.StatusMovedPermanently)
}

// handler for: "/"+Rversion+"/"+Rcluster
func (p *proxyrunner) clusterhdlr(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		p.httpget(w, r)
	case http.MethodPost:
		p.httppost(w, r)
	case http.MethodDelete:
		p.httpdelete(w, r)
	case http.MethodPut:
		p.httpput(w, r)
	default:
		invalhdlr(w, r)
	}
	glog.Flush()
}

// gets target info
func (p *proxyrunner) httpget(w http.ResponseWriter, r *http.Request) {
	stats := getproxystats()
	apitems := restApiItems(r.URL.Path, 5)
	if apitems = checkRestAPI(w, r, apitems, 0, Rversion, Rcluster); apitems == nil {
		atomic.AddInt64(&stats.numerr, 1)
		return
	}
	jsbytes, err := json.Marshal(ctx.smap)
	assert(err == nil)
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsbytes)
}

// registers a new target
func (p *proxyrunner) httppost(w http.ResponseWriter, r *http.Request) {
	stats := getproxystats()
	apitems := restApiItems(r.URL.Path, 5)
	if apitems = checkRestAPI(w, r, apitems, 0, Rversion, Rcluster); apitems == nil {
		atomic.AddInt64(&stats.numerr, 1)
		return
	}
	var si ServerInfo
	b, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err == nil {
		err = json.Unmarshal(b, &si)
	}
	if err != nil {
		glog.Errorf("Failed to json-unmarshal %s request, err: %v [%v]", r.Method, err, string(b))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		atomic.AddInt64(&stats.numerr, 1)
		return
	}
	atomic.AddInt64(&stats.numpost, 1)
	if ctx.smap.get(si.DaemonID) != nil {
		glog.Errorf("Duplicate target {%s}", si.DaemonID)
	}
	ctx.smap.add(&si)
	if glog.V(3) {
		glog.Infof("Registered target {%s} (count %d)", si.DaemonID, ctx.smap.count())
	}
}

// unregisters a target
func (p *proxyrunner) httpdelete(w http.ResponseWriter, r *http.Request) {
	stats := getproxystats()
	apitems := restApiItems(r.URL.Path, 5)
	if apitems = checkRestAPI(w, r, apitems, 2, Rversion, Rcluster); apitems == nil {
		atomic.AddInt64(&stats.numerr, 1)
		return
	}
	if apitems[0] != Rdaemon {
		glog.Errorf("Invalid API element: %s (expecting %s)", apitems[0], Rdaemon)
		invalhdlr(w, r)
		return
	}
	sid := apitems[1]
	atomic.AddInt64(&stats.numdelete, 1)
	if ctx.smap.get(sid) == nil {
		glog.Errorf("Unknown target {%s}", sid)
		return
	}
	ctx.smap.del(sid)
	if glog.V(3) {
		glog.Infof("Unregistered target {%s} (count %d)", sid, ctx.smap.count())
	}
}

func (p *proxyrunner) httpput(w http.ResponseWriter, r *http.Request) {
	assert(r.Method == http.MethodPut) // TODO: add GET for the stats, and more
	stats := getproxystats()
	//
	// parse and validate REST API
	//
	apitems := restApiItems(r.URL.Path, 5)
	if apitems = checkRestAPI(w, r, apitems, 0, Rversion, Rcluster); apitems == nil {
		atomic.AddInt64(&stats.numerr, 1)
		invalhdlr(w, r)
		return
	}
	var msg ControlMsg
	b, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err == nil {
		err = json.Unmarshal(b, &msg)
	}
	if err != nil {
		glog.Errorf("Failed to json-unmarshal %s request, err: %v [%v]", r.Method, err, string(b))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		atomic.AddInt64(&stats.numerr, 1)
		return
	}
	if msg.Action != shutdown {
		glog.Errorf("Unexpected control message [%+v]", msg)
		invalhdlr(w, r)
		return
	}
	glog.Infoln("Proxy-controlled cluster shutdown...")
	jsbytes, err := json.Marshal(msg) // same message -> this target
	if err != nil {
		glog.Errorf("Unexpected failure to json-marshal %+v, err: %v", msg, err)
		invalhdlr(w, r)
		return
	}
	for _, si := range ctx.smap.Smap {
		url := si.DirectURL + "/" + Rversion + "/" + Rdaemon
		p.call(url, http.MethodPut, jsbytes)
	}
	time.Sleep(time.Second)
	syscall.Kill(syscall.Getpid(), syscall.SIGINT)
}
