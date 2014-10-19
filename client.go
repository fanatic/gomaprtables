package gomaprtables

/*
#include <stdlib.h>
#include <string.h>
#include <hbase/hbase.h>

void cl_dsc_cb(int32_t err, hb_client_t client, void *extra);
*/
import "C"
import "unsafe"

type Client struct {
  client C.hb_client_t
  errCB  chan C.int32_t
}

func (c *Conn) NewClient() (*Client, error) {
  cl := Client{}
  e := C.hb_client_create(c.hb, &cl.client)
  if e != 0 {
    return nil, Errno(e)
  }
  cl.errCB = make(chan C.int32_t)
  return &cl, nil
}

func (cl *Client) Close() error {
  e := C.hb_client_destroy(cl.client, (C.hb_client_disconnection_cb)(C.cl_dsc_cb), (unsafe.Pointer)(&cl.errCB))
  if e != 0 {
    return Errno(e)
  }
  // Wait around for the callback
  e = <-cl.errCB
  if e != 0 {
    return Errno(e)
  }
  return nil
}

//export clientCloseCallback
func clientCloseCallback(err C.int32_t, client C.hb_client_t, extra unsafe.Pointer) {
  *((*chan C.int32_t)(extra)) <- err
}

// Unimplemented: Flush()
