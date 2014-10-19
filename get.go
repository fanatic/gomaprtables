package gohbase

// #cgo CFLAGS: -I. -I/opt/mapr/include
// #cgo LDFLAGS: -L/opt/mapr/lib -L/usr/lib/jvm/java-1.7.0/jre/lib/amd64/server -lMapRClient -ljvm
/*
#include <stdlib.h>
#include <string.h>
#include <hbase/hbase.h>
#include <pthread.h>

void get_send_cb(int32_t err, hb_client_t client, hb_get_t get, hb_result_t result, void *extra);
*/
import "C"
import "unsafe"
import "fmt"

//Unimplemented: Get Column
//Unimplemented: Get with filter
//Unimplemented: Get timestamp
//Unimplemented: Get with time range
func (cl *Client) Get(nameSpace *string, tableName string, rowKey []byte, cb chan CallbackResult) error {
  var get C.hb_get_t
  e := C.hb_get_create((*C.byte_t)(unsafe.Pointer(&rowKey[0])), (C.size_t)(len(rowKey)), &get)
  if e != 0 {
    fmt.Printf("get_create: %d\n", e)
    return Errno(e)
  }

  if nameSpace != nil {
    ns := C.CString(*nameSpace)
    defer C.free(unsafe.Pointer(ns))
    e = C.hb_get_set_namespace(get, ns, C.strlen(ns))
    if e != 0 {
      return Errno(e)
    }
  }

  tn := C.CString(tableName)
  defer C.free(unsafe.Pointer(tn))
  e = C.hb_get_set_table(get, tn, C.strlen(tn))
  if e != 0 {
    fmt.Printf("set_table: %d\n", e)
    return Errno(e)
  }

  e = C.hb_get_send(cl.client, get, (C.hb_get_cb)(C.get_send_cb), (unsafe.Pointer)(&cb))
  if e != 0 {
    fmt.Printf("get_send: %d\n", e)
    return Errno(e)
  }
  return nil
}

//export getCallback
func getCallback(e C.int32_t, client C.hb_client_t, get C.hb_get_t, result C.hb_result_t, extra unsafe.Pointer) {
  var err error
  if e != 0 {
    err = Errno(e)
  }

  C.hb_get_destroy(get)
  *((*chan CallbackResult)(extra)) <- CallbackResult{[]*Result{NewResult(result)}, err}
}
