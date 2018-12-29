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

// query type
const (
	queryNormal byte = iota + 1
	queryPrepare
	queryExecute
)

// capability flags
const (
	clientLongPassword uint32 = 1 << iota
	clientFoundRows
	clientLongFlag
	clientConnectWithDB
	clientNoSchema
	clientCompress
	clientODBC
	clientLocalFiles
	clientIgnoreSpace
	clientProtocol41
	clientInteractive
	clientSSL
	clientIgnoreSIGPIPE
	clientTransactions
	clientReserved
	clientSecureConn
	clientMultiStatements
	clientMultiResults
	clientPSMultiResults
	clientPluginAuth
	clientConnectAttrs
	clientPluginAuthLenEncClientData
	clientCanHandleExpiredPasswords
	clientSessionTrack
	clientDeprecateEOF
)

// probe
const (
	inputQueueLength = 2000              // stream input queue length
	streamExpiration = 150 * time.Second // empty stream expiration
	cleanDeviation   = 10                // clean deviation in case of IO congestion
	unknowDbName     = "unknown"         // unkonwn db name
)

// assembly
const (
	mysqlReqSeq         = 0     // mysql request sequence
	mysqlRspSeq         = 1     // mysql response sequence
	maxSpan     float32 = 60000 // max response latency（ms）in case of package jam
)

// cache
const lruCacheSize = 500 // lru cache size of prepare command
