package gomaprtables

/*
#include <stdlib.h>
#include <string.h>
#include <hbase/hbase.h>
*/
import "C"
import "unsafe"

//ColDesc represents a column family descriptor
type ColDesc struct {
  colDesc C.hb_columndesc
}

//NewColDesc creates a column family descriptor
func NewColDesc(family []byte) (*ColDesc, error) {
  cd := ColDesc{}

  fam := cBytes(family)
  //defer C.free(unsafe.Pointer(fam))
  e := C.hb_coldesc_create((*C.byte_t)(unsafe.Pointer(fam)), C.size_t(len(family)), &cd.colDesc)
  if e != 0 {
    return nil, Errno(e)
  }
  return &cd, nil
}

//c returns a C-representation of a column descriptor for internal use
func (cd *ColDesc) c() C.hb_columndesc {
  return cd.colDesc
}

//Unimplemented: destroy
//Unimplemented: SetMaxVersions
//Unimplemented: SetMinVersions
//Unimplemented: SetTTL
//Unimplemented: SetInmemory
