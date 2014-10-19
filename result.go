package gohbase

/*
#include <stdlib.h>
#include <string.h>
#include <hbase/hbase.h>
#include <pthread.h>

// Put callback
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
*/
import "C"

type Result struct {
  hb_result C.hb_result_t
}

func NewResult(r C.hb_result_t) *Result {
  return &Result{hb_result: r}
}

func (r *Result) PrintResult() {
  if r.hb_result != nil {
    C.read_result(r.hb_result)
  }
}
