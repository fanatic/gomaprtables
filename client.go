package gohbase

/*
#include <stdlib.h>
#include <string.h>
#include <hbase/hbase.h>
#include <pthread.h>

// Client dsc callback
void cl_dsc_cb(int32_t err, hb_client_t client, void *extra)
{
  printf("  -> Client disconnection callback called %p\n", extra);
}
*/
import "C"

type Client struct {
  client C.hb_client_t
}

func (c *Conn) NewClient() (*Client, error) {
  cl := Client{}
  e := C.hb_client_create(c.hb, &cl.client)
  if e != 0 {
    return nil, Errno(e)
  }
  return &cl, nil
}

func (cl *Client) Close() error {
  e := C.hb_client_destroy(cl.client, (C.hb_client_disconnection_cb)(C.cl_dsc_cb), nil)
  if e != 0 {
    return Errno(e)
  }
  return nil
}

// Unimplemented: Flush()
