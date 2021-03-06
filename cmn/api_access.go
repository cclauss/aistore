// Package provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	// object
	AccessGET = 1 << iota
	AccessObjHEAD
	AccessPUT
	AccessAPPEND
	AccessDownload
	AccessObjDELETE
	AccessObjRENAME
	AccessPROMOTE
	// bucket
	AccessBckHEAD
	AccessObjLIST
	AccessBckRENAME
	AccessPATCH
	AccessMAKENCOPIES
	AccessEC
	AccessSYNC
	AccessBckDELETE
	// cluster
	AccessBckCreate
	AccessBckLIST
	AccessADMIN
	// must be the last one
	AccessMax

	// Permissions
	allowAllAccess       = ^uint64(0)
	allowReadOnlyAccess  = AccessGET | AccessObjHEAD | AccessBckHEAD | AccessObjLIST
	allowReadWriteAccess = allowReadOnlyAccess |
		AccessPUT | AccessAPPEND | AccessDownload | AccessObjDELETE | AccessObjRENAME
	allowClusterAccess = allowAllAccess & (AccessBckCreate - 1)

	// Permission Operations
	AllowAccess = "allow"
	DenyAccess  = "deny"
)

type AccessAttrs uint64

// access => operation
var accessOp = map[int]string{
	// object
	AccessGET:       "GET",
	AccessObjHEAD:   "HEAD-OBJECT",
	AccessPUT:       "PUT",
	AccessAPPEND:    "APPEND",
	AccessDownload:  "DOWNLOAD",
	AccessObjDELETE: "DELETE-OBJECT",
	AccessObjRENAME: "RENAME-OBJECT",
	AccessPROMOTE:   "PROMOTE",
	// bucket
	AccessBckHEAD:     "HEAD-BUCKET",
	AccessObjLIST:     "LIST-OBJECTS",
	AccessBckRENAME:   "RENAME-BUCKET",
	AccessPATCH:       "PATCH",
	AccessMAKENCOPIES: "MAKE-NCOPIES",
	AccessEC:          "EC",
	AccessSYNC:        "SYNC-BUCKET",
	AccessBckDELETE:   "DELETE-BUCKET",
	// cluster
	AccessBckCreate: "CREATE-BUCKET",
	AccessADMIN:     "ADMIN",
}

func NoAccess() AccessAttrs                      { return 0 }
func AllAccess() AccessAttrs                     { return AccessAttrs(allowAllAccess) }
func ReadOnlyAccess() AccessAttrs                { return allowReadOnlyAccess }
func ReadWriteAccess() AccessAttrs               { return allowReadWriteAccess }
func (a AccessAttrs) Has(perms AccessAttrs) bool { return a&perms == perms }
func (a AccessAttrs) String() string             { return strconv.FormatUint(uint64(a), 10) }
func (a AccessAttrs) Describe() string {
	if a == 0 {
		return "No access"
	}
	accList := make([]string, 0, 24)
	if a.Has(AccessGET) {
		accList = append(accList, accessOp[AccessGET])
	}
	if a.Has(AccessObjHEAD) {
		accList = append(accList, accessOp[AccessObjHEAD])
	}
	if a.Has(AccessPUT) {
		accList = append(accList, accessOp[AccessPUT])
	}
	if a.Has(AccessAPPEND) {
		accList = append(accList, accessOp[AccessAPPEND])
	}
	if a.Has(AccessDownload) {
		accList = append(accList, accessOp[AccessDownload])
	}
	if a.Has(AccessObjDELETE) {
		accList = append(accList, accessOp[AccessObjDELETE])
	}
	if a.Has(AccessObjRENAME) {
		accList = append(accList, accessOp[AccessObjRENAME])
	}
	if a.Has(AccessPROMOTE) {
		accList = append(accList, accessOp[AccessPROMOTE])
	}
	//
	if a.Has(AccessBckHEAD) {
		accList = append(accList, accessOp[AccessBckHEAD])
	}
	if a.Has(AccessObjLIST) {
		accList = append(accList, accessOp[AccessObjLIST])
	}
	if a.Has(AccessBckRENAME) {
		accList = append(accList, accessOp[AccessBckRENAME])
	}
	if a.Has(AccessPATCH) {
		accList = append(accList, accessOp[AccessPATCH])
	}
	if a.Has(AccessMAKENCOPIES) {
		accList = append(accList, accessOp[AccessMAKENCOPIES])
	}
	if a.Has(AccessSYNC) {
		accList = append(accList, accessOp[AccessSYNC])
	}
	if a.Has(AccessBckDELETE) {
		accList = append(accList, accessOp[AccessBckDELETE])
	}
	return strings.Join(accList, ",")
}

func AccessOp(access int) string {
	if s, ok := accessOp[access]; ok {
		return s
	}
	return "<unknown access>"
}

func ModifyAccess(aattr uint64, action string, bits uint64) (uint64, error) {
	if action == AllowAccess {
		return aattr | bits, nil
	}
	if action != DenyAccess {
		return 0, fmt.Errorf("unknown make-access action %q", action)
	}
	return aattr & (allowAllAccess ^ bits), nil
}
