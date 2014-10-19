package gohbase

// #cgo CFLAGS: -I. -I/opt/mapr/include
// #cgo LDFLAGS: -L/opt/mapr/lib -L/usr/lib/jvm/java-1.7.0/jre/lib/amd64/server -lMapRClient -ljvm
/*
#include <stdlib.h>
#include <string.h>
#include <hbase/hbase.h>
#include <pthread.h>

// Admin dc callback
void admin_dc_cb(int32_t err, hb_admin_t admin, void *extra)
{
  printf("admin_dc_cb: err = %d\n", err);
}

// Client dsc callback
void cl_dsc_cb(int32_t err, hb_client_t client, void *extra)
{
  printf("  -> Client disconnection callback called %p\n", extra);
}

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

// Get callback
void get_send_cb(int32_t err, hb_client_t client, hb_get_t get, hb_result_t result, void *extra)
{
  printf("  get_send_cb: err=%d\n", err);
  read_result(result);
  hb_get_destroy(get);
}

// Scan callback
void sn_cb(int32_t err, hb_scanner_t scan, hb_result_t *results, size_t numResults, void *extra)
{
  printf("  -> Scanner next callback()\n");
  printf("  -> CB: err: %d, numResults: %d\n", err, (int)numResults);

  if (numResults > 0) {
		uint32_t r;
    for (r = 0; r < numResults; ++r) {
      read_result(results[r]);
    }
    hb_scanner_next(scan, sn_cb, NULL);
  }
}
*/
/*
import "C"
import "time"
import "unsafe"
import "fmt"

type Errno int

func (e Errno) Error() string {
  s := errText[e]
  if s == "" {
    return fmt.Sprintf("errno %d", int(e))
  }
  return s
}
var errText = map[Errno]string{}

var tableName = C.CString("/tables/jptest")

func main() {
	var conn C.hb_connection_t
	cldbs := C.CString("192.168.2.107")
	defer C.free(unsafe.Pointer(cldbs))
	e := C.hb_connection_create(cldbs, nil, &conn)
	if e != 0 {
		fmt.Printf("Could not connect to cluster %s: err=%d\n", C.GoString(cldbs), e)
		return
	}

	err := createTable(conn)
	if err != nil {
		fmt.Printf("createTable: %v\n", err)
		return
	}

	err = put(conn, 10)
	if err != nil {
		fmt.Printf("put: %v\n", err)
		return
	}

	err = get(conn, "row-1")
	if err != nil {
		fmt.Printf("get: %v\n", err)
		return
	}

	err = scan(conn)
	if err != nil {
		fmt.Printf("scan: %v\n", err)
		return
	}

	e = C.hb_connection_destroy(conn);
	if e != 0 {
		fmt.Printf("connection_destroy: %d\n", err);
		return
	}

}

func createTable(conn C.hb_connection_t) error {
	var admin C.hb_admin_t
	err := C.hb_admin_create(conn, &admin)
	if err != 0 {
		return Errno(err)
	}

	families := make([]C.hb_columndesc, 3)

	id := C.CString("Id")
	defer C.free(unsafe.Pointer(id))
	err = C.hb_coldesc_create((*C.byte_t)(unsafe.Pointer(id)), 2, &families[0])
	if err != 0 {
		return Errno(err)
	}

	name := C.CString("Name")
	defer C.free(unsafe.Pointer(name))
	err = C.hb_coldesc_create((*C.byte_t)(unsafe.Pointer(name)), 4, &families[1])
	if err != 0 {
		return Errno(err)
	}

	fam := C.CString("Family3")
	defer C.free(unsafe.Pointer(fam))
	err = C.hb_coldesc_create((*C.byte_t)(unsafe.Pointer(fam)), 7, &families[2])
	if err != 0 {
		return Errno(err)
	}

	err = C.hb_admin_table_exists(admin, nil, tableName)
	if err == 0 {
		fmt.Printf("Table User exists\n")
		err = C.hb_admin_table_delete(admin, nil, tableName)
		if err != 0 {
			return Errno(err)
		}
	} else {
		fmt.Printf("Table User does not exist: %d\n", err)
	}

	fmt.Printf("Creating table %s ...\n", C.GoString(tableName))
	err = C.hb_admin_table_create(admin, nil, tableName, (*C.hb_columndesc)(unsafe.Pointer(&families[0])), 2)
	if err != 0 {
		return Errno(err)
	}

	fmt.Printf("Destroying admin connection ...\n")
	err = C.hb_admin_destroy(admin, (C.hb_admin_disconnection_cb)(C.admin_dc_cb), nil)
	if err != 0 {
		return Errno(err)
	}

  return nil
}

func put(conn C.hb_connection_t, numRows int) error {
	var client C.hb_client_t
	err := C.hb_client_create(conn, &client)
	if err != 0 {
		fmt.Printf("client_create: %d\n", err);
		return Errno(err)
	}
	for i:=0; i<numRows; i++ {
		err := onePut(client, i)
		if err != nil {
			return err
		}
	}

	fmt.Printf("Waiting for all callbacks to return ...\n");
	for locCount :=0;; {
	  time.Sleep(1 * time.Second)
	  C.pthread_mutex_lock(&C.put_mut)
	  locCount = int(C.count)
	  C.pthread_mutex_unlock(&C.put_mut)
		if locCount < numRows {
			break;
		}
	}

	fmt.Printf("Received %lu callbacks\n", numRows);

	fmt.Printf("Destroying client connection ...\n")
	err = C.hb_client_destroy(client, (C.hb_client_disconnection_cb)(C.cl_dsc_cb), nil)
	if err != 0 {
		fmt.Printf("client_destroy: %d\n", err);
		return Errno(err)
	}
	return nil
}

func onePut(client C.hb_client_t, id int) error {
	var put C.hb_put_t

	b := fmt.Sprintf("row-%d", id)
	buffer := C.CString(b)
	defer C.free(unsafe.Pointer(buffer))

	err := C.hb_put_create((*C.byte_t)(unsafe.Pointer(buffer)), C.strlen(buffer), &put)
	if err != 0 {
		fmt.Printf("PUT: put_create: %d\n", err)
		return Errno(err)
	}

	var cell *C.hb_cell_t

	createDummyCell(&cell, b, "Name", "First", fmt.Sprintf("first-%d", id))
	err = C.hb_put_add_cell(put, cell)
	if err != 0 {
		fmt.Printf("add_cell: %d\n", err)
		return Errno(err)
	}

	createDummyCell(&cell, b, "Id", "i", fmt.Sprintf("id-%d", id))
	err = C.hb_put_add_cell(put, cell)
	if err != 0 {
		fmt.Printf("add_cell: %d\n", err)
		return Errno(err)
	}

	err = C.hb_mutation_set_table((C.hb_mutation_t)(put), tableName, C.strlen(tableName))
	if err != 0 {
		fmt.Printf("set_table: %d\n", err)
		return Errno(err)
	}

	fmt.Printf("Put send [Client: %v, Mutation: %v, Id: %d]\n", client, put, id)
	err = C.hb_mutation_send(client, (C.hb_mutation_t)(put), (C.hb_mutation_cb)(C.put_cb), nil)
	if err != 0 {
		fmt.Printf("mutation_send: %d\n", err)
		return Errno(err)
	}

	return nil
}

func createDummyCell(cell **C.hb_cell_t, r, f, q, v string) {
  cellPtr := C.hb_cell_t{}

  cr := C.CString(r)
	//defer C.free(unsafe.Pointer(cr))
	cellPtr.row = (*C.byte_t)(unsafe.Pointer(cr))
	cellPtr.row_len = C.strlen(cr)

  cf := C.CString(f)
	//defer C.free(unsafe.Pointer(cf))
	cellPtr.family = (*C.byte_t)(unsafe.Pointer(cf))
	cellPtr.family_len = C.strlen(cf)

  cq := C.CString(q)
	//defer C.free(unsafe.Pointer(cq))
	cellPtr.qualifier = (*C.byte_t)(unsafe.Pointer(cq))
	cellPtr.qualifier_len = C.strlen(cq)

  cv := C.CString(v)
	//defer C.free(unsafe.Pointer(cv))
	cellPtr.value = (*C.byte_t)(unsafe.Pointer(cv))
	cellPtr.value_len = C.strlen(cv)

	*cell = &cellPtr
}

func get(conn C.hb_connection_t, key string) error {
	var client C.hb_client_t
	e := C.hb_client_create(conn, &client)
	if e != 0 {
		fmt.Printf("client_create: %d\n", e);
		return Errno(e)
	}

  ck := C.CString(key)
	defer C.free(unsafe.Pointer(ck))

	var get C.hb_get_t
	e = C.hb_get_create((*C.byte_t)(unsafe.Pointer(ck)), C.strlen(ck), &get)
	if e != 0 {
		fmt.Printf("get_create: %d\n", e);
		return Errno(e)
	}

	e = C.hb_get_set_table(get, tableName, C.strlen(tableName))
	if e != 0 {
		fmt.Printf("set_table: %d\n", e)
		return Errno(e)
	}

	e = C.hb_get_send(client, get, (C.hb_get_cb)(C.get_send_cb), nil)
	if e != 0 {
		fmt.Printf("get_send: %d\n", e)
		return Errno(e)
	}

	fmt.Printf("Destroying client connection ...\n")
	e = C.hb_client_destroy(client, (C.hb_client_disconnection_cb)(C.cl_dsc_cb), nil)
	if e != 0 {
		fmt.Printf("client_destroy: %d\n", e)
		return Errno(e)
	}
	time.Sleep(4*time.Second)
	return nil
}

func scan(conn C.hb_connection_t) error {
	var client C.hb_client_t
	e := C.hb_client_create(conn, &client)
	if e != 0 {
		fmt.Printf("client_create: %d\n", e);
		return Errno(e)
	}

	var scan C.hb_scanner_t
	e = C.hb_scanner_create(client, &scan)
	if e != 0 {
		fmt.Printf("scanner_create: %d\n", e);
		return Errno(e)
	}

	e = C.hb_scanner_set_table(scan, tableName, C.strlen(tableName))
	if e != 0 {
		fmt.Printf("set_table: %d\n", e)
		return Errno(e)
	}

	e = C.hb_scanner_set_num_versions(scan, 2)
	if e != 0 {
		fmt.Printf("set_num_versions: %d\n", e)
		return Errno(e)
	}

	e = C.hb_scanner_next(scan, (C.hb_scanner_cb)(C.sn_cb), nil)
	if e != 0 {
		fmt.Printf("scanner_next: %d\n", e)
		return Errno(e)
	}

	time.Sleep(4*time.Second)

	fmt.Printf("Destroying client connection ...\n")
	e = C.hb_client_destroy(client, (C.hb_client_disconnection_cb)(C.cl_dsc_cb), nil)
	if e != 0 {
		fmt.Printf("client_destroy: %d\n", e)
		return Errno(e)
	}
	time.Sleep(4*time.Second)
	return nil
}
*/
