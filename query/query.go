// Package query provides interface to iterate over objects with additional filtering
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package query

import (
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

type (
	ObjectsSource struct {
		// regexp *regexp.Regexp // support in the future
		Pt *cmn.ParsedTemplate
	}

	BucketSource struct {
		// regexp *regexp.Regexp // support in the future
		Bck *cmn.Bck
	}

	ObjectsQuery struct {
		ObjectsSource *ObjectsSource
		BckSource     *BucketSource
		filter        cluster.ObjectFilter
	}
)

func NewQuery(source *ObjectsSource, bckSource *BucketSource, filter cluster.ObjectFilter) *ObjectsQuery {
	return &ObjectsQuery{
		ObjectsSource: source,
		BckSource:     bckSource,
		filter:        filter,
	}
}

func (q *ObjectsQuery) Filter() cluster.ObjectFilter {
	if q.filter != nil {
		return q.filter
	}
	return func(*cluster.LOM) bool { return true }
}

func TemplateObjSource(pt *cmn.ParsedTemplate) *ObjectsSource {
	return &ObjectsSource{Pt: pt}
}

func AllObjSource() *ObjectsSource {
	return &ObjectsSource{}
}

func BckSource(bck cmn.Bck) *BucketSource {
	return &BucketSource{Bck: &bck}
}

func NewQueryFromMsg(msg *DefMsg) (q *ObjectsQuery, err error) {
	q = &ObjectsQuery{}
	if msg.OuterSelect.Template != "" {
		pt, err := cmn.ParseBashTemplate(msg.OuterSelect.Template)
		if err != nil {
			return nil, err
		}

		q.ObjectsSource = TemplateObjSource(&pt)
	} else {
		q.ObjectsSource = AllObjSource()
	}
	q.BckSource = BckSource(msg.From.Bck)
	q.filter, err = ObjFilterFromMsg(msg.Where.Filter)
	if err != nil {
		return nil, err
	}
	return q, nil
}
