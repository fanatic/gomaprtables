package gohbase

/*
#include <stdlib.h>
#include <string.h>
#include <hbase/hbase.h>

void admin_dc_cb(int32_t err, hb_admin_t admin, void *extra);
*/
import "C"
import "unsafe"

type AdminClient struct {
  admin C.hb_admin_t
  errCB chan C.int32_t
}

func (c *Conn) NewAdminClient() (*AdminClient, error) {
  a := AdminClient{}
  e := C.hb_admin_create(c.hb, &a.admin)
  if e != 0 {
    return nil, Errno(e)
  }
  a.errCB = make(chan C.int32_t)
  return &a, nil
}

func (a *AdminClient) Close() error {
  e := C.hb_admin_destroy(a.admin, (C.hb_admin_disconnection_cb)(C.admin_dc_cb), (unsafe.Pointer)(&a.errCB))
  if e != 0 {
    return Errno(e)
  }
  // Wait around for the callback
  e = <-a.errCB
  if e != 0 {
    return Errno(e)
  }
  return nil
}

//export adminCloseCallback
func adminCloseCallback(err C.int32_t, admin C.hb_admin_t, extra unsafe.Pointer) {
  *((*chan C.int32_t)(extra)) <- err
}

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
