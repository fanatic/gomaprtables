package gomaprtables

/*
#include <stdlib.h>
#include <string.h>
#include <hbase/hbase.h>
*/
import "C"
import "unsafe"
import "reflect"

// cBytes returns a pointer to the first byte in b.
func cBytes(b *[]byte) *C.byte_t {
  return (*C.byte_t)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(b)).Data))
  //return (*C.byte_t)(unsafe.Pointer(&b[0]))
}

func cLen(b []byte) C.size_t {
  return (C.size_t)(len(b))
}
