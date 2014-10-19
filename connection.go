package gohbase

// #cgo CFLAGS: -I. -I/opt/mapr/include
// #cgo LDFLAGS: -L/opt/mapr/lib -L/usr/lib/jvm/java-1.7.0/jre/lib/amd64/server -lMapRClient -ljvm
/*
#include <stdlib.h>
#include <hbase/hbase.h>
*/
import "C"
import "unsafe"
import "fmt"
import "strings"

type Conn struct {
  hb       C.hb_connection_t
  CLDBList []string
  a        *AdminClient
  c        *Client
}

// NewConn takes a list of CLDB servers "<server1[:port]>,..."
func NewConn(cldbs []string) (*Conn, error) {
  conn := Conn{}
  conn.CLDBList = cldbs

  cs := C.CString(strings.Join(cldbs, ","))
  defer C.free(unsafe.Pointer(cs))

  e := C.hb_connection_create(cs, nil, &conn.hb)
  if e != 0 {
    return nil, fmt.Errorf("Could not connect to cluster %v: err=%d\n", cldbs, e)
  }

  return &conn, nil
}

func (c *Conn) Close() error {
  if c.a != nil {
    if err := c.a.Close(); err != nil {
      return err
    }
  }
  if c.c != nil {
    if err := c.c.Close(); err != nil {
      return err
    }
  }
  e := C.hb_connection_destroy(c.hb)
  if e != 0 {
    return fmt.Errorf("connection_destroy: %d\n", e)
  }
  return nil
}

// Helper functions
func (conn *Conn) EnsureAdminClient() error {
  if conn.a == nil {
    var err error
    conn.a, err = conn.NewAdminClient()
    if err != nil {
      return fmt.Errorf("Admin client: %v\n", err)
    }
  }
  return nil
}

func (conn *Conn) CreateTable(tableName string, columnFamilies [][]byte, deleteIfExist bool) error {
  if err := conn.EnsureAdminClient(); err != nil {
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

func (conn *Conn) EnsureClient() error {
  if conn.c == nil {
    var err error
    conn.c, err = conn.NewClient()
    if err != nil {
      return fmt.Errorf("Client: %v\n", err)
    }
  }
  return nil
}

func (conn *Conn) Put(tableName string, rowKey []byte, cells []Cell, cb chan CallbackResult) error {
  if err := conn.EnsureClient(); err != nil {
    return err
  }
  if err := conn.c.Put(nil, tableName, true, rowKey, cells, cb); err != nil {
    return err
  }
  return nil
}

func (conn *Conn) Get(tableName string, rowKey []byte, cb chan CallbackResult) error {
  if err := conn.EnsureClient(); err != nil {
    return err
  }
  if err := conn.c.Get(nil, tableName, rowKey, cb); err != nil {
    return err
  }
  return nil
}

func (conn *Conn) Scan(tableName string, cb chan CallbackResult) error {
  if err := conn.EnsureClient(); err != nil {
    return err
  }
  if err := conn.c.Scan(nil, tableName, nil, nil, 1, cb); err != nil {
    return err
  }
  return nil
}
