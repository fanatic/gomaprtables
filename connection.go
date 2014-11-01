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

// NewConnectionAsUser takes a list of CLDB servers "<server1[:port]>,..." and connects
// to HBase/MapR as a specific user via impersonation
func NewConnectionAsUser(cldbs []string, user string) (*Connection, error) {
  conn := Connection{}
  conn.CLDBList = cldbs

  cs := C.CString(strings.Join(cldbs, ","))
  cu := C.CString(user)

  e := C.hb_connection_create_as_user(cs, nil, cu, &conn.hb)
  if e != 0 {
    return nil, fmt.Errorf("Could not connect to cluster %v: %v\n", cldbs, Errno(e))
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
    return Errno(e)
  }
  return nil
}
