package gomaprtables

/*
#include <stdlib.h>
#include <string.h>
#include <hbase/hbase.h>

void sn_cb(int32_t err, hb_scanner_t scan, hb_result_t *results, size_t numResults, void *extra);
void sn_destroy_cb(int32_t err, hb_scanner_t scanner, void *extra);
*/
import "C"
import "unsafe"
import "fmt"

//Unimplemented: Scan with filter
//Unimplemented: Scan with limit

//Scan queues a request to retrieve multiple rows.  The result will be placed on the cb channel.
func (cl *Client) Scan(nameSpace *string, tableName string, startRow, endRow []byte, numVersions int, cb chan CallbackResult) error {
  var scan C.hb_scanner_t
  e := C.hb_scanner_create(cl.client, &scan)
  if e != 0 {
    return Errno(e)
  }

  if nameSpace != nil {
    ns := C.CString(*nameSpace)
    defer C.free(unsafe.Pointer(ns))
    e = C.hb_scanner_set_namespace(scan, ns, C.strlen(ns))
    if e != 0 {
      return Errno(e)
    }
  }

  tn := C.CString(tableName)
  defer C.free(unsafe.Pointer(tn))
  e = C.hb_scanner_set_table(scan, tn, C.strlen(tn))
  if e != 0 {
    fmt.Printf("set_table: %d\n", e)
    return Errno(e)
  }

  if startRow != nil {
    e = C.hb_scanner_set_start_row(scan, (*C.byte_t)(unsafe.Pointer(&startRow[0])), (C.size_t)(len(startRow)))
    if e != 0 {
      return Errno(e)
    }
  }
  if endRow != nil {
    e = C.hb_scanner_set_end_row(scan, (*C.byte_t)(unsafe.Pointer(&endRow[0])), (C.size_t)(len(endRow)))
    if e != 0 {
      return Errno(e)
    }
  }

  e = C.hb_scanner_set_num_max_rows(scan, 1)
  if e != 0 {
    return Errno(e)
  }

  e = C.hb_scanner_set_num_versions(scan, (C.int8_t)(numVersions))
  if e != 0 {
    return Errno(e)
  }

  e = C.hb_scanner_next(scan, (C.hb_scanner_cb)(C.sn_cb), (unsafe.Pointer)(&cb))
  if e != 0 {
    return Errno(e)
  }
  return nil
}

//export scanNextCallback
func scanNextCallback(e C.int32_t, scan C.hb_scanner_t, results *C.hb_result_t, numResults C.size_t, extra unsafe.Pointer) {
  var err error
  if e != 0 {
    err = Errno(e)
  }

  resultSet := make([]*Result, 0, int(numResults))
  cb := *((*chan CallbackResult)(extra))

  if numResults > 0 {

    cResults := (*[1 << 30]C.hb_result_t)(unsafe.Pointer(results))[:numResults:numResults]
    for _, cResult := range cResults {
      resultSet = append(resultSet, newResult(cResult))
    }

    e = C.hb_scanner_next(scan, (C.hb_scanner_cb)(C.sn_cb), (unsafe.Pointer)(&cb))
    if e != 0 {
      err = Errno(e)
    }
  } else {
    errCB := make(chan C.int32_t)
    C.hb_scanner_destroy(scan, (C.hb_scanner_destroy_cb)(C.sn_destroy_cb), (unsafe.Pointer)(&errCB))
    // Wait around for the callback
    e = <-errCB
    if e != 0 {
      err = Errno(e)
    }
  }

  cb <- CallbackResult{resultSet, err}
}

//export scanDestroyCallback
func scanDestroyCallback(err C.int32_t, scan C.hb_scanner_t, extra unsafe.Pointer) {
  *((*chan C.int32_t)(extra)) <- err
}
