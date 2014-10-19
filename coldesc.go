package gomaprtables

/*
#include <stdlib.h>
#include <string.h>
#include <hbase/hbase.h>
*/
import "C"
import "unsafe"

type ColDesc struct {
  colDesc C.hb_columndesc
}

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

func (cd *ColDesc) C() C.hb_columndesc {
  return cd.colDesc
}

//Unimplemented: destroy
//Unimplemented: SetMaxVersions
//Unimplemented: SetMinVersions
//Unimplemented: SetTTL
//Unimplemented: SetInmemory
