package gohbase

/*
#include <stdlib.h>
#include <string.h>
#include <hbase/hbase.h>

// Admin dc callback
void admin_dc_cb(int32_t err, hb_admin_t admin, void *extra)
{
  printf("admin_dc_cb: err = %d\n", err);
}
*/
import "C"
import "unsafe"

type AdminClient struct {
  admin C.hb_admin_t
}

func (c *Conn) NewAdminClient() (*AdminClient, error) {
  a := AdminClient{}
  e := C.hb_admin_create(c.hb, &a.admin)
  if e != 0 {
    return nil, Errno(e)
  }
  return &a, nil
}

func (a *AdminClient) Close() error {
  e := C.hb_admin_destroy(a.admin, (C.hb_admin_disconnection_cb)(C.admin_dc_cb), nil)
  if e != 0 {
    return Errno(e)
  }
  return nil
}

// Unimplemented: Callback in golang
// https://code.google.com/p/go-wiki/wiki/cgo

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

// Unimplemented: table disable
// Unimplemented: table enable

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
    cFamilies[i] = fam.C()
  }

  e := C.hb_admin_table_create(a.admin, ns, tn, (*C.hb_columndesc)(unsafe.Pointer(&cFamilies[0])), C.size_t(len(families)))
  if e != 0 {
    return Errno(e)
  }
  return nil
}

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
