package gomaprtables

/*
#include <stdlib.h>
#include <hbase/hbase.h>
*/
import "C"
import "unsafe"
import "time"

type Cell struct {
  Row       []byte
  Family    []byte
  Qualifier []byte
  Value     []byte
  Timestamp *time.Time
}

func NewCell(cCell *C.hb_cell_t) *Cell {
  c := Cell{
    Row:       C.GoBytes(unsafe.Pointer(cCell.row), C.int(cCell.row_len)),
    Family:    C.GoBytes(unsafe.Pointer(cCell.family), C.int(cCell.family_len)),
    Qualifier: C.GoBytes(unsafe.Pointer(cCell.qualifier), C.int(cCell.qualifier_len)),
    Value:     C.GoBytes(unsafe.Pointer(cCell.value), C.int(cCell.value_len)),
  }
  ts := time.Unix(int64(cCell.ts), 0)
  c.Timestamp = &ts
  return &c
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
    cellPtr.ts = (C.int64_t)(c.Timestamp.Unix())
  }

  return &cellPtr
}
