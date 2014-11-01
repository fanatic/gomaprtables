package gomaprtables

/*
#include <stdlib.h>
#include <string.h>
#include <hbase/hbase.h>

void get_send_cb(int32_t err, hb_client_t client, hb_get_t get, hb_result_t result, void *extra);
*/
import "C"
import "unsafe"

//Get queues a request to retrieve a row.  The result will be placed on the cb channel.
func (cl *Client) Get(nameSpace *string, tableName string, rowKey []byte, columns *[]Column, filter *string, numVersions *int, timestamp *int64, timestampRange *TimeRange, cb *chan CallbackResult) error {
  var get C.hb_get_t
  e := C.hb_get_create(cBytes(rowKey), cLen(rowKey), &get)
  if e != 0 {
    return Errno(e)
  }

  if nameSpace != nil {
    ns := C.CString(*nameSpace)
    e = C.hb_get_set_namespace(get, ns, C.strlen(ns))
    if e != 0 {
      C.hb_get_destroy(get)
      return Errno(e)
    }
  }

  tn := C.CString(tableName)
  e = C.hb_get_set_table(get, tn, C.strlen(tn))
  if e != 0 {
    C.hb_get_destroy(get)
    return Errno(e)
  }

  if columns != nil {
    for _, column := range *columns {
      if column.Qualifier == nil {
        e = C.hb_get_add_column(get, cBytes(column.Family), cLen(column.Family), nil, 0)
        if e != 0 {
          C.hb_get_destroy(get)
          return Errno(e)
        }
      } else {
        e = C.hb_get_add_column(get, cBytes(column.Family), cLen(column.Family), cBytes(*column.Qualifier), cLen(*column.Qualifier))
        if e != 0 {
          C.hb_get_destroy(get)
          return Errno(e)
        }
      }
    }
  }

  if filter != nil {
    cf := C.CString(*filter)
    e = C.hb_get_set_filter(get, cf)
    if e != 0 {
      C.hb_get_destroy(get)
      return Errno(e)
    }
  }

  if numVersions != nil {
    e = C.hb_get_set_num_versions(get, C.int32_t(*numVersions))
    if e != 0 {
      C.hb_get_destroy(get)
      return Errno(e)
    }
  }

  if timestamp != nil {
    e = C.hb_get_set_timestamp(get, C.int64_t(*timestamp))
    if e != 0 {
      C.hb_get_destroy(get)
      return Errno(e)
    }
  }

  if timestampRange != nil {
    e = C.hb_get_set_timerange(get, C.int64_t(timestampRange.MinTimestamp), C.int64_t(timestampRange.MaxTimestamp))
    if e != 0 {
      C.hb_get_destroy(get)
      return Errno(e)
    }
  }

  e = C.hb_get_send(cl.client, get, (C.hb_get_cb)(C.get_send_cb), (unsafe.Pointer)(cb))
  if e != 0 {
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

  cb := (*chan CallbackResult)(extra)
  *cb <- CallbackResult{[]*Result{newResult(result)}, err}

  C.hb_get_destroy(get)
}
