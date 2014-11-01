package gomaprtables

/*
#include <stdlib.h>
#include <string.h>
#include <hbase/hbase.h>

void cl_dsc_cb(int32_t err, hb_client_t client, void *extra);
void cl_flush_cb(int32_t err, hb_client_t client, void *extra);
*/
import "C"
import "unsafe"

// Client represents a client for manipulating rows and cells within a table
type Client struct {
  client C.hb_client_t
}

// NewClient returns a Client
func (conn *Connection) NewClient() (*Client, error) {
  cl := Client{}
  e := C.hb_client_create(conn.hb, &cl.client)
  if e != 0 {
    return nil, Errno(e)
  }
  return &cl, nil
}

//export clientFlushCallback
func clientFlushCallback(err C.int32_t, client C.hb_client_t, extra unsafe.Pointer) {
  cb := (*chan C.int32_t)(extra)
  *cb <- err
}

// Flush any buffered client-side write operations to HBase.  Waits until everything
// that has been buffered at the time of the call has been flushed.
// Note: This doesn't guarantee that ALL outstanding RPCs have completed
// BUG: This seems to hang forever :(
func (cl *Client) Flush() error {
  errCB := make(chan C.int32_t)
  e := C.hb_client_flush(cl.client, (C.hb_client_flush_cb)(C.cl_flush_cb), (unsafe.Pointer)(&errCB))
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

//export clientCloseCallback
func clientCloseCallback(err C.int32_t, client C.hb_client_t, extra unsafe.Pointer) {
  cb := (*chan C.int32_t)(extra)
  *cb <- err
}

// Close cleans up all associated structures from Client and waits before returning
func (cl *Client) Close() error {
  errCB := make(chan C.int32_t)
  e := C.hb_client_destroy(cl.client, (C.hb_client_disconnection_cb)(C.cl_dsc_cb), (unsafe.Pointer)(&errCB))
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
