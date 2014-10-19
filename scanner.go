package gohbase

/*
#include <stdlib.h>
#include <string.h>
#include <hbase/hbase.h>
#include <pthread.h>

// Scan callback
void sn_cb(int32_t err, hb_scanner_t scan, hb_result_t *results, size_t numResults, void *extra)
{
  printf("  -> Scanner next callback()\n");
  printf("  -> CB: err: %d, numResults: %d\n", err, (int)numResults);

  if (numResults > 0) {
		uint32_t r;
    for (r = 0; r < numResults; ++r) {
      read_result(results[r]);
    }
    hb_scanner_next(scan, sn_cb, NULL);
  }
}
*/
import "C"
import "unsafe"
import "fmt"

//Unimplemented: Scan with filter
//Unimplemented: Scan with limit
func (cl *Client) Scan(nameSpace *string, tableName string, startRow, endRow []byte, numVersions int) error {
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

  e = C.hb_scanner_set_num_versions(scan, (C.int8_t)(numVersions))
  if e != 0 {
    return Errno(e)
  }

  e = C.hb_scanner_next(scan, (C.hb_scanner_cb)(C.sn_cb), nil)
  if e != 0 {
    return Errno(e)
  }
  return nil
}
