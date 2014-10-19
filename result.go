package gomaprtables

/*
#include <stdlib.h>
#include <hbase/hbase.h>
*/
import "C"
import "unsafe"
import "fmt"

//Result represents a HBase response
type Result struct {
  TableName string
  NameSpace *string
  RowKey    []byte
  NumCells  int
  Cells     []*Cell
  hb_result C.hb_result_t
}

//newResult is used internally to create a Result from the C object
func newResult(result C.hb_result_t) *Result {
  if result == nil {
    return nil
  }
  r := Result{hb_result: result}

  var tn *C.char
  var tnLen C.size_t
  C.hb_result_get_table(result, &tn, &tnLen)
  r.TableName = C.GoStringN(tn, C.int(tnLen))

  //Throws error in LibHdfsApi when namespace is NULL
  var ns *C.char
  var nsLen C.size_t
  C.hb_result_get_namespace(result, &ns, &nsLen)
  if ns != nil {
    ns2 := C.GoStringN(ns, C.int(nsLen))
    r.NameSpace = &ns2
  }

  var rk *C.byte_t
  var rkLen C.size_t
  C.hb_result_get_key(result, &rk, &rkLen)
  r.RowKey = C.GoBytes(unsafe.Pointer(rk), C.int(rkLen))

  var cCount C.size_t
  C.hb_result_get_cell_count(result, &cCount)
  r.NumCells = int(cCount)
  if r.NumCells > 0 {
    var cells **C.hb_cell_t
    var cellsLen C.size_t
    C.hb_result_get_cells(result, &cells, &cellsLen)

    r.Cells = make([]*Cell, 0, cellsLen)
    cCells := (*[1 << 30]*C.hb_cell_t)(unsafe.Pointer(cells))[:cellsLen:cellsLen]
    for _, cCell := range cCells {
      r.Cells = append(r.Cells, newCell(cCell))
    }
  }

  // Clean up the hb_result now that it's been copied off
  C.hb_result_destroy(result)
  return &r
}

//PrintResult prints out the result representation to stdout
func (r *Result) PrintResult() {
  if r != nil {
    fmt.Printf("  Table: %s  NameSpace: %v CellCount: %d RowKey: %q\n", r.TableName, r.NameSpace, r.NumCells, string(r.RowKey))
    for i, cell := range r.Cells {
      fmt.Printf("    cell[%d] [R]: %q, [F:Q]: %q:%q, [V]: %q, [TS]: %s\n", i, cell.Row, cell.Family, cell.Qualifier, cell.Value, cell.Timestamp)
    }
  }
}
