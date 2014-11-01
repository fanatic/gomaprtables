package gomaprtables

/*
#include <stdlib.h>
#include <string.h>
#include <hbase/hbase.h>
*/
import "C"

//ColDesc represents a column family descriptor
type ColDesc struct {
  Name        []byte /* Column family name */
  MaxVersions *int   /* Maximum number of cell versions to be retained. Defaults to 3. */
  MinVersions *int   /* Maximum number of cell versions to be retained. Defaults to 0. */
  TTL         *int   /* Time-to-live of cell contents, in seconds. Defaults to forever */
  InMemory    *bool  /* If true, all values are kept in HRegionServer cache.  Defaults to false */
}

//c returns a C-representation of a column descriptor for internal use
func (cd *ColDesc) c() (C.hb_columndesc, error) {
  var colDesc C.hb_columndesc

  e := C.hb_coldesc_create(cBytes(cd.Name), cLen(cd.Name), &colDesc)
  if e != 0 {
    return nil, Errno(e)
  }

  if cd.MaxVersions != nil {
    e = C.hb_coldesc_set_maxversions(colDesc, C.int32_t(*cd.MaxVersions))
    if e != 0 {
      C.hb_coldesc_destroy(colDesc)
      return nil, Errno(e)
    }
  }

  if cd.MinVersions != nil {
    e = C.hb_coldesc_set_minversions(colDesc, C.int32_t(*cd.MinVersions))
    if e != 0 {
      C.hb_coldesc_destroy(colDesc)
      return nil, Errno(e)
    }
  }

  if cd.TTL != nil {
    e = C.hb_coldesc_set_ttl(colDesc, C.int32_t(*cd.TTL))
    if e != 0 {
      C.hb_coldesc_destroy(colDesc)
      return nil, Errno(e)
    }
  }
  return colDesc, nil
}

func destroy(colDesc C.hb_columndesc) error {
  e := C.hb_coldesc_destroy(colDesc)
  if e != 0 {
    return Errno(e)
  }
  return nil
}
