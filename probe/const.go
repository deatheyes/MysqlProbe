package probe

import (
	"time"
)

// mysql
const (
	iOK          byte = 0x00
	iLocalInFile byte = 0xfb
	iEOF         byte = 0xfe
	iERR         byte = 0xff
)

const (
	comQuit byte = iota + 1
	comInitDB
	comQuery
	comFieldList
	comCreateDB
	comDropDB
	comRefresh
	comShutdown
	comStatistics
	comProcessInfo
	comConnect
	comProcessKill
	comDebug
	comPing
	comTime
	comDelayedInsert
	comChangeUser
	comBinlogDump
	comTableDump
	comConnectOut
	comRegisterSlave
	comStmtPrepare
	comStmtExecute
	comStmtSendLongData
	comStmtClose
	comStmtReset
	comSetOption
	comStmtFetch
)

// probe
const (
	inputQueueLength = 2000            // stream input queue length
	streamExpiration = 5 * time.Second // empty stream expiration
)
