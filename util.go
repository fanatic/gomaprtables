package gomaprtables

import "unsafe"
import "reflect"

// cBytes returns a pointer to the first byte in b.
func cBytes(b []byte) unsafe.Pointer {
  return unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&b)).Data)
}
