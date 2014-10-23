package gomaprtables

// #cgo CFLAGS: -I. -I/opt/mapr/include
// #cgo LDFLAGS: -L/opt/mapr/lib -L/usr/lib/jvm/java-1.7.0/jre/lib/amd64/server -lMapRClient -ljvm
/*
#include <stdlib.h>
#include <hbase/hbase.h>
*/
import "C"
//import "unsafe"
import "fmt"
import "strings"

//Connection represents a connection to HBase/MapR
type Connection struct {
  hb       C.hb_connection_t
  CLDBList []string
  a        *AdminClient
  c        *Client
}

// NewConnection takes a list of CLDB servers "<server1[:port]>,..." and connects to HBase/MapR
func NewConnection(cldbs []string) (*Connection, error) {
  conn := Connection{}
  conn.CLDBList = cldbs

  cs := C.CString(strings.Join(cldbs, ","))

  e := C.hb_connection_create(cs, nil, &conn.hb)
  if e != 0 {
    return nil, fmt.Errorf("Could not connect to cluster %v: err=%d\n", cldbs, e)
  }

  return &conn, nil
}

// Close cleans up all associated structures from the Connection
func (conn *Connection) Close() error {
  if conn.a != nil {
    if err := conn.a.Close(); err != nil {
      return err
    }
  }
  if conn.c != nil {
    if err := conn.c.Close(); err != nil {
      return err
    }
  }
  e := C.hb_connection_destroy(conn.hb)
  if e != 0 {
    return fmt.Errorf("connection_destroy: %d\n", e)
  }
  return nil
}

// Helper functions

func (conn *Connection) ensureAdminClient() error {
  if conn.a == nil {
    var err error
    conn.a, err = conn.NewAdminClient()
    if err != nil {
      return fmt.Errorf("Admin client: %v\n", err)
    }
  }
  return nil
}

//CreateTable creates the given table, doing all the dirty work for you
func (conn *Connection) CreateTable(tableName string, columnFamilies [][]byte, deleteIfExist bool) error {
  if err := conn.ensureAdminClient(); err != nil {
    return err
  }

  families := []*ColDesc{}
  for i, family := range columnFamilies {
    if cd, err := NewColDesc([]byte(family)); err == nil {
      families = append(families, cd)
    } else {
      return fmt.Errorf("new col desc %d: %v\n", i, err)
    }
  }

  if deleteIfExist {
    if err := conn.a.IsTableExist(nil, tableName); err == nil {
      if err := conn.a.DeleteTable(nil, tableName); err != nil {
        return fmt.Errorf("Deleting table: %v\n", err)
      }
    }
  }

  if err := conn.a.CreateTable(nil, tableName, families); err != nil {
    return fmt.Errorf("create table: %v\n", err)
  }
  return nil
}

func (conn *Connection) ensureClient() error {
  if conn.c == nil {
    var err error
    conn.c, err = conn.NewClient()
    if err != nil {
      return fmt.Errorf("Client: %v\n", err)
    }
  }
  return nil
}

//Put adds a row to a table, doing all the dirty work for you
func (conn *Connection) Put(tableName string, rowKey []byte, cells []Cell, cb *chan CallbackResult) error {
  if err := conn.ensureClient(); err != nil {
    return err
  }
  if err := conn.c.Put(nil, tableName, true, rowKey, cells, cb); err != nil {
    return err
  }
  return nil
}

//Get retrieves a row from a table, doing all the dirty work for you
func (conn *Connection) Get(tableName string, rowKey []byte) (*Result, error) {
  if err := conn.ensureClient(); err != nil {
    return nil, err
  }

  cb := make(chan CallbackResult)

  if err := conn.c.Get(nil, tableName, rowKey, &cb); err != nil {
    return nil, err
  }

  result := <-cb
  if result.Err != nil {
    return nil, result.Err
  }

  return result.Results[0], nil
}

//Scan retrieves several rows from a table, doing all the dirty work for you
func (conn *Connection) Scan(tableName string) ([]*Result, error) {
  if err := conn.ensureClient(); err != nil {
    return nil, err
  }

  cb := make(chan CallbackResult)

  if err := conn.c.Scan(nil, tableName, nil, nil, 1, &cb); err != nil {
    return nil, err
  }

  results := []*Result{}
  for result := range cb {
    if result.Err != nil {
      return nil, result.Err
    }
    if len(result.Results) > 0 {
      results = append(results, result.Results...)
    } else {
      break
    }
  }
  return results, nil
}
