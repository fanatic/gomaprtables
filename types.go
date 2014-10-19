package gohbase

/*
#include <stdlib.h>
#include <string.h>
#include <hbase/hbase.h>
*/
import "C"
import "unsafe"

type Cell struct {
  Row       []byte
  Family    []byte
  Qualifier []byte
  Value     []byte
  Timestamp *int
}

func (c Cell) CCell() *C.hb_cell_t {
  cellPtr := C.hb_cell_t{}

  cellPtr.row = (*C.byte_t)(unsafe.Pointer(&c.Row[0]))
  cellPtr.row_len = (C.size_t)(len(c.Row))

  cellPtr.family = (*C.byte_t)(unsafe.Pointer(&c.Family[0]))
  cellPtr.family_len = (C.size_t)(len(c.Family))

  cellPtr.qualifier = (*C.byte_t)(unsafe.Pointer(&c.Qualifier[0]))
  cellPtr.qualifier_len = (C.size_t)(len(c.Qualifier))

  cellPtr.value = (*C.byte_t)(unsafe.Pointer(&c.Value[0]))
  cellPtr.value_len = (C.size_t)(len(c.Value))

  if c.Timestamp != nil {
    cellPtr.ts = (C.int64_t)(*c.Timestamp)
  }

  return &cellPtr
}
