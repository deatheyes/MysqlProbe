package util

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	// mysql dialect
	_ "github.com/go-sql-driver/mysql"
	"github.com/golang/glog"
)

const (
	mysqlShowProcessList = "show processlist"
	mysql                = "mysql"
	localhost            = "localhost"
	infoExpiration       = 10 * time.Second // connection info expiration
)

// ConnectionWatcher get the connection info by db client and refresh periodically
type ConnectionWatcher struct {
	infoMap    map[string]*DBConnectionInfo // connection info from db
	uname      string                       // db user name
	passward   string                       // db passward
	sock       string                       // db domain socket path
	dbname     string                       // db name
	lastupdate time.Time                    // update time
	lastbytes  []byte                       // byte for comparision
	sync.RWMutex
}

// NewConnectionWatcher create a instance of connection watcher
func NewConnectionWatcher(uname, passward, sock, dbname string) *ConnectionWatcher {
	w := &ConnectionWatcher{
		infoMap:  make(map[string]*DBConnectionInfo),
		uname:    uname,
		passward: passward,
		sock:     sock,
		dbname:   dbname,
	}
	w.init()
	return w
}

func (w *ConnectionWatcher) update() {
	m, err := GetMysqlConnectionInfo(w.uname, w.passward, w.sock, w.dbname)
	if err != nil {
		glog.Warningf("[watcher] get connection info failed: %v", err)
	}

	var buffer []byte
	for _, v := range m {
		buffer = append(buffer, v.key()[:]...)
	}

	if bytes.Equal(buffer, w.lastbytes) {
		if len(buffer) == 0 {
			glog.Warning("[watcher] get empty connection info")
		} else {
			glog.V(6).Info("[watcher] connection info not change")
		}
	} else {
		w.Lock()
		w.infoMap = m
		w.Unlock()
		w.lastbytes = buffer
		glog.V(6).Info("[watcher] connection info update done")
	}
}

// Init run the update process
func (w *ConnectionWatcher) init() {
	// init connection info first
	w.update()

	go func() {
		ticker := time.NewTicker(infoExpiration)
		for {
			<-ticker.C
			w.update()
		}
	}()
}

// Get return the connection info by key
func (w *ConnectionWatcher) Get(key string) *DBConnectionInfo {
	w.RLock()
	defer w.RUnlock()

	return w.infoMap[key]
}

// DBConnectionInfo retrieves the db connection info
type DBConnectionInfo struct {
	ID, User, Host, DB, Cmd, Time, State, Info, Sent, Examined []byte
}

// Key generate the bytes for comparison
func (i *DBConnectionInfo) key() (buffer []byte) {
	buffer = append(buffer, i.ID[:]...)
	buffer = append(buffer, i.User[:]...)
	buffer = append(buffer, i.Host[:]...)
	buffer = append(buffer, i.DB[:]...)
	return
}

// GetMysqlConnectionInfo return all db connection info
func GetMysqlConnectionInfo(user string, password string, sock string, dbname string) (map[string]*DBConnectionInfo, error) {
	str := fmt.Sprintf("%s:%s@unix(%s)/%s", user, password, sock, dbname)
	db, err := sql.Open(mysql, str)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query(mysqlShowProcessList)
	if err != nil {
		return nil, err
	}

	version := 0
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	if len(cols) == 10 {
		version = 5
	} else if len(cols) == 8 {
		version = 8
	}

	if version == 0 {
		return nil, errors.New("unknown mysql version")
	}

	ret := make(map[string]*DBConnectionInfo)
	for rows.Next() {
		data := &DBConnectionInfo{}
		if version == 8 {
			if err := rows.Scan(&data.ID, &data.User, &data.Host, &data.DB, &data.Cmd, &data.Time, &data.State, &data.Info); err != nil {
				return nil, err
			}
		} else {
			if err := rows.Scan(&data.ID, &data.User, &data.Host, &data.DB, &data.Cmd, &data.Time, &data.State, &data.Info, &data.Sent, &data.Examined); err != nil {
				return nil, err
			}
		}
		if len(data.Host) == 0 || string(data.Host) == localhost || len(data.DB) == 0 {
			continue
		}
		glog.V(5).Infof("[watcher] connection %s db %s", data.Host, data.DB)
		ret[string(data.Host)] = data
	}
	return ret, nil
}
