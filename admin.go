package gomaprtables

/*
#include <stdlib.h>
#include <hbase/hbase.h>

void admin_dc_cb(int32_t err, hb_admin_t admin, void *extra);
*/
import "C"
import "unsafe"

// AdminClient represents a client for manipulating tables
type AdminClient struct {
  admin C.hb_admin_t
}

// NewAdminClient returns an AdminClient
func (conn *Connection) NewAdminClient() (*AdminClient, error) {
  a := AdminClient{}
  e := C.hb_admin_create(conn.hb, &a.admin)
  if e != 0 {
    return nil, Errno(e)
  }
  return &a, nil
}

//export adminCloseCallback
func adminCloseCallback(err C.int32_t, admin C.hb_admin_t, extra unsafe.Pointer) {
  cb := (*chan C.int32_t)(extra)
  *cb <- err
}

//IsTableExist checks if a table exists.  Returns nil if table exists.
func (a *AdminClient) IsTableExist(nameSpace *string, tableName string) error {
  var ns *C.char
  if nameSpace != nil {
    ns = C.CString(*nameSpace)
    defer C.free(unsafe.Pointer(ns))
  }

  tn := C.CString(tableName)
  defer C.free(unsafe.Pointer(tn))

  e := C.hb_admin_table_exists(a.admin, ns, tn)
  if e != 0 {
    return Errno(e)
  }
  return nil
}

//IsTableEnabled checks if a table is enabled.  Returns true if table is enabled.
func (a *AdminClient) IsTableEnabled(nameSpace *string, tableName string) (bool, error) {
  var ns *C.char
  if nameSpace != nil {
    ns = C.CString(*nameSpace)
    defer C.free(unsafe.Pointer(ns))
  }

  tn := C.CString(tableName)
  defer C.free(unsafe.Pointer(tn))

  e := C.hb_admin_table_enabled(a.admin, ns, tn)
  if e != 0 {
    if e == C.HBASE_TABLE_DISABLED {
      return false, nil
    }
    return false, Errno(e)
  }
  return true, nil
}

// DisableTable disables an HBase table
func (a *AdminClient) DisableTable(nameSpace *string, tableName string) error {
  var ns *C.char
  if nameSpace != nil {
    ns = C.CString(*nameSpace)
    defer C.free(unsafe.Pointer(ns))
  }

  tn := C.CString(tableName)
  defer C.free(unsafe.Pointer(tn))

  e := C.hb_admin_table_disable(a.admin, ns, tn)
  if e != 0 {
    return Errno(e)
  }
  return nil
}

// EnableTable enables an HBase table
func (a *AdminClient) EnableTable(nameSpace *string, tableName string) error {
  var ns *C.char
  if nameSpace != nil {
    ns = C.CString(*nameSpace)
    defer C.free(unsafe.Pointer(ns))
  }

  tn := C.CString(tableName)
  defer C.free(unsafe.Pointer(tn))

  e := C.hb_admin_table_enable(a.admin, ns, tn)
  if e != 0 {
    return Errno(e)
  }
  return nil
}

// CreateTable creates an HBase table
func (a *AdminClient) CreateTable(nameSpace *string, tableName string, families []*ColDesc) error {
  var ns *C.char
  if nameSpace != nil {
    ns = C.CString(*nameSpace)
    defer C.free(unsafe.Pointer(ns))
  }

  tn := C.CString(tableName)
  defer C.free(unsafe.Pointer(tn))

  cFamilies := make([]C.hb_columndesc, len(families))
  for i, fam := range families {
    var err error
    cFamilies[i], err = fam.c()
    if err != nil {
      return err
    }
  }

  e := C.hb_admin_table_create(a.admin, ns, tn, (*C.hb_columndesc)(unsafe.Pointer(&cFamilies[0])), C.size_t(len(families)))
  if e != 0 {
    return Errno(e)
  }

  for _, cFam := range cFamilies {
    destroy(cFam)
  }
  return nil
}

// DeleteTable deletes an HBase table, and disables the table if not already disabled
func (a *AdminClient) DeleteTable(nameSpace *string, tableName string) error {
  var ns *C.char
  if nameSpace != nil {
    ns = C.CString(*nameSpace)
    defer C.free(unsafe.Pointer(ns))
  }

  tn := C.CString(tableName)
  defer C.free(unsafe.Pointer(tn))

  e := C.hb_admin_table_delete(a.admin, ns, tn)
  if e != 0 {
    return Errno(e)
  }
  return nil
}

// Close cleans up all associated structures from AdminClient and waits before returning
func (a *AdminClient) Close() error {
  errCB := make(chan C.int32_t)
  e := C.hb_admin_destroy(a.admin, (C.hb_admin_disconnection_cb)(C.admin_dc_cb), (unsafe.Pointer)(&errCB))
  if e != 0 {
    return Errno(e)
  }
  // Wait around for the callback
  e = <-errCB
  if e != 0 {
    return Errno(e)
  }
  return nil
}
