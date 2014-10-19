package gomaprtables

/*
#include <stdlib.h>
#include <string.h>
#include <hbase/hbase.h>

void put_cb(int err, hb_client_t client, hb_mutation_t mutation,
            hb_result_t result, void *extra);
*/
import "C"
import "unsafe"

//Unimplemented: durability,
func (cl *Client) Put(nameSpace *string, tableName string, bufferable bool, rowKey []byte, cells []Cell, cb chan CallbackResult) error {
  var put C.hb_put_t

  e := C.hb_put_create((*C.byte_t)(unsafe.Pointer(&rowKey[0])), (C.size_t)(len(rowKey)), &put)
  if e != 0 {
    return Errno(e)
  }

  if nameSpace != nil {
    ns := C.CString(*nameSpace)
    defer C.free(unsafe.Pointer(ns))
    e = C.hb_mutation_set_namespace((C.hb_mutation_t)(put), ns, C.strlen(ns))
    if e != 0 {
      return Errno(e)
    }
  }

  tn := C.CString(tableName)
  defer C.free(unsafe.Pointer(tn))
  e = C.hb_mutation_set_table((C.hb_mutation_t)(put), tn, C.strlen(tn))
  if e != 0 {
    return Errno(e)
  }

  e = C.hb_mutation_set_bufferable((C.hb_mutation_t)(put), (C._Bool)(bufferable))
  if e != 0 {
    return Errno(e)
  }

  for _, cell := range cells {
    e = C.hb_put_add_cell(put, cell.CCell())
    if e != 0 {
      return Errno(e)
    }
  }

  e = C.hb_mutation_send(cl.client, (C.hb_mutation_t)(put), (C.hb_mutation_cb)(C.put_cb), (unsafe.Pointer)(&cb))
  if e != 0 {
    return Errno(e)
  }
  return nil
}

//export putCallback
func putCallback(e C.int32_t, client C.hb_client_t, mutation C.hb_mutation_t, result C.hb_result_t, extra unsafe.Pointer) {
  var err error
  if e != 0 {
    err = Errno(e)
  }

  C.hb_mutation_destroy(mutation)
  *((*chan CallbackResult)(extra)) <- CallbackResult{[]*Result{NewResult(result)}, err}
}

//Unimplemented: delete
//Unimplemented: increment
//Unimplemented: append
