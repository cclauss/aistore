// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"io"
	"net/http"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/objwalk/walkinfo"
	"github.com/NVIDIA/aistore/query"
	"github.com/NVIDIA/aistore/xaction"
)

// There are 3 methods exposed by targets:
// * Peek(n): get next n objects from a target query, but keep the results in memory.
//   Subsequent Peek(n) request returns the same objects.
// * Discard(n): forget first n elements from a target query.
// * Next(n): Peek(n) + Discard(n)

func (t *targetrunner) queryHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		t.httpqueryget(w, r)
	case http.MethodPost:
		t.httpquerypost(w, r)
	case http.MethodPut:
		t.httpqueryput(w, r)
	default:
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /query path")
	}
}

func (t *targetrunner) httpquerypost(w http.ResponseWriter, r *http.Request) {
	if _, err := t.checkRESTItems(w, r, 0, false, cmn.Version, cmn.Query, cmn.Init); err != nil {
		return
	}

	handle := r.Header.Get(cmn.HeaderHandle) // TODO: should it be from header or from body?
	msg := &query.InitMsg{}
	if err := cmn.ReadJSON(w, r, msg); err != nil {
		return
	}

	q, err := query.NewQueryFromMsg(&msg.QueryMsg)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}

	wi := walkinfo.NewDefaultWalkInfo(t, msg.QueryMsg.From.Bck.Name)
	wi.SetObjectFilter(q.Filter())
	if _, err = xaction.Registry.RenewObjectsListingXact(t, q, wi, handle); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
}

func (t *targetrunner) httpqueryget(w http.ResponseWriter, r *http.Request) {
	var (
		entries []*cmn.BucketEntry
	)

	apiItems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Query)
	if err != nil {
		return
	}

	msg := &query.NextMsg{}
	if err := cmn.ReadJSON(w, r, msg); err != nil {
		return
	}
	if msg.Handle == "" {
		t.invalmsghdlr(w, r, "handle cannot be empty", http.StatusBadRequest)
		return
	}
	resultSet := query.Registry.Get(msg.Handle)
	if resultSet == nil {
		t.queryDoesntExist(w, r, msg.Handle)
		return
	}

	switch apiItems[0] {
	case cmn.Next:
		entries, err = resultSet.NextN(msg.Size)
	case cmn.Peek:
		entries, err = resultSet.PeekN(msg.Size)
	default:
		t.invalmsghdlrf(w, r, "invalid %s/%s/%s", cmn.Version, cmn.Query, apiItems[0])
		return
	}

	if err != nil && err != io.EOF {
		t.invalmsghdlr(w, r, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(cmn.MustMarshal(cmn.BucketList{Entries: entries}))
}

// v1/query/discard/handle/value
func (t *targetrunner) httpqueryput(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Query, cmn.Discard)
	if err != nil {
		return
	}

	handle, value := apiItems[0], apiItems[1]
	resultSet := query.Registry.Get(handle)
	if resultSet == nil {
		t.queryDoesntExist(w, r, handle)
		return
	}

	resultSet.DiscardUntil(value)
}

func (t *targetrunner) queryDoesntExist(w http.ResponseWriter, r *http.Request, handle string) {
	t.invalmsghdlrsilent(w, r, t.Snode().String()+" handle "+handle+" not found", http.StatusNotFound)
}
