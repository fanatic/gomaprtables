package gomaprtables

/*
#include <stdlib.h>
#include <hbase/hbase.h>
*/
import "C"
import "unsafe"
import "time"

//Cell represents a HBase cell - identified by a row, column family,
//column qualifier, value, and timestamp
type Cell struct {
  Row       []byte
  Family    []byte
  Qualifier []byte
  Value     []byte
  Timestamp *time.Time
}

type Column struct {
  Family    []byte
  Qualifier *[]byte /* Optional */
  Timestamp *int64  /* Optional, used in Delete only */
}

type TimeRange struct {
  MinTimestamp int64
  MaxTimestamp int64
}

const LATESTTIMESTAMP int64 = C.HBASE_LATEST_TIMESTAMP

//NewCell is used internally to create a Cell from the C object
func newCell(cCell *C.hb_cell_t) *Cell {
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

//CCell is used internally to create a C object from a Cell
func (c Cell) cCell() *C.hb_cell_t {
  cellPtr := C.hb_cell_t{}

  cellPtr.row = cBytes(&c.Row)
  cellPtr.row_len = cLen(c.Row)

  cellPtr.family = cBytes(&c.Family)
  cellPtr.family_len = cLen(c.Family)

  cellPtr.qualifier = cBytes(&c.Qualifier)
  cellPtr.qualifier_len = cLen(c.Qualifier)

  cellPtr.value = cBytes(&c.Value)
  cellPtr.value_len = cLen(c.Value)

  if c.Timestamp != nil {
    cellPtr.ts = (C.int64_t)(c.Timestamp.Unix())
  }

  return &cellPtr
}
