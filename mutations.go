package gohbase

// #cgo CFLAGS: -I. -I/opt/mapr/include
// #cgo LDFLAGS: -L/opt/mapr/lib -L/usr/lib/jvm/java-1.7.0/jre/lib/amd64/server -lMapRClient -ljvm
/*
#include <stdlib.h>
#include <string.h>
#include <hbase/hbase.h>
#include <pthread.h>

// Put callback
pthread_mutex_t put_mut = PTHREAD_MUTEX_INITIALIZER;
uint64_t count = 0;
void read_result(hb_result_t result)
{
  int e = 0;

  if (!result) {
    printf("NULL Result\n");
    return;
  }

  const char *tableName;
  size_t tableLen = 0;
  e = hb_result_get_table(result, &tableName, &tableLen);
  printf("    get_table: %s(err=%d)\n", tableName, e);

  size_t cellCount = 0;
  e = hb_result_get_cell_count(result, &cellCount);
  printf("    get_cell_count: %d(err=%d)\n", (int)cellCount, e);

  // Getting all cells
  size_t i;
  for (i = 0; i < cellCount; ++i) {
    const hb_cell_t *cell;
    e = hb_result_get_cell_at(result, i, &cell);
    printf("    cell[%d]: Row: %s, [F:Q]: %s:%s, Value: %s\n", (int)i, cell->row,
 cell->family, cell->qualifier, cell->value);
  }

  const char *t;
  const char *n;
  size_t len;
  e = hb_result_get_table(result, &t, &len);
  e = hb_result_get_namespace(result, &n, &len);

  printf("    Result table=%s, NameSpace=%s\n", t, n);
}


void put_cb(int err, hb_client_t client, hb_mutation_t mutation,
            hb_result_t result, void *extra)
{
  printf("PUT cb called [Client: %p, Mutation: %p] err = %d\n", (void *)client, (void *)mutation, err);
  printf("Result: %p\n", result);
  hb_mutation_destroy(mutation);
  pthread_mutex_lock(&put_mut);
  count ++;
  pthread_mutex_unlock(&put_mut);
  read_result(result);
}
*/
import "C"
import "unsafe"

//Unimplemented: durability,
func (cl *Client) Put(nameSpace *string, tableName string, bufferable bool, rowKey []byte, cells []Cell) error {
  var put C.hb_put_t

  e := C.hb_put_create((*C.byte_t)(unsafe.Pointer(&rowKey[0])), (C.size_t)(len(rowKey)), &put)
  if e != 0 {
    return Errno(e)
  }

  if nameSpace != nil {
    ns := C.CString(*nameSpace)
    defer C.free(unsafe.Pointer(ns))
    e = C.hb_mutation_set_namespace((C.hb_mutation_t)(put), ns, C.strlen(ns))
    if e != 0 {
      return Errno(e)
    }
  }

  tn := C.CString(tableName)
  defer C.free(unsafe.Pointer(tn))
  e = C.hb_mutation_set_table((C.hb_mutation_t)(put), tn, C.strlen(tn))
  if e != 0 {
    return Errno(e)
  }

  e = C.hb_mutation_set_bufferable((C.hb_mutation_t)(put), (C._Bool)(bufferable))
  if e != 0 {
    return Errno(e)
  }

  for _, cell := range cells {
    e = C.hb_put_add_cell(put, cell.CCell())
    if e != 0 {
      return Errno(e)
    }
  }

  e = C.hb_mutation_send(cl.client, (C.hb_mutation_t)(put), (C.hb_mutation_cb)(C.put_cb), nil)
  if e != 0 {
    return Errno(e)
  }
  return nil
}

//Unimplemented: delete
//Unimplemented: increment
//Unimplemented: append
