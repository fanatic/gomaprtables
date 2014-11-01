package gomaprtables

/*
#include <stdlib.h>
#include <string.h>
#include <hbase/hbase.h>

void mutation_cb(int err, hb_client_t client, hb_mutation_t mutation,
            hb_result_t result, void *extra);
*/
import "C"
import "unsafe"

type Durability int

const (
  USEDEFAULT Durability = C.DURABILITY_USE_DEFAULT /* Use column family's default setting */
  SKIPWAL    Durability = C.DURABILITY_SKIP_WAL    /* Do not write the Mutation to the WAL */
  ASYNCWAL   Durability = C.DURABILITY_ASYNC_WAL   /* Write the Mutation to the WAL asynchronously */
  SYNCWAL    Durability = C.DURABILITY_SYNC_WAL    /* Write the Mutation to the WAL synchronously */
  FSYNCWAL   Durability = C.DURABILITY_FSYNC_WAL   /* Write the Mutation to the WAL synchronously and force to disk */
)

//Put queues a request to insert a row.  The result will be placed on the cb channel.
func (cl *Client) Put(nameSpace *string, tableName string, bufferable *bool, durability *Durability, rowKey []byte, cells []Cell, cb *chan CallbackResult) error {
  var put C.hb_put_t

  e := C.hb_put_create(cBytes(&rowKey), cLen(rowKey), &put)
  if e != 0 {
    return Errno(e)
  }

  if nameSpace != nil {
    ns := C.CString(*nameSpace)
    e = C.hb_mutation_set_namespace((C.hb_mutation_t)(put), ns, C.strlen(ns))
    if e != 0 {
      C.hb_mutation_destroy((C.hb_mutation_t)(put))
      return Errno(e)
    }
  }

  tn := C.CString(tableName)
  e = C.hb_mutation_set_table((C.hb_mutation_t)(put), tn, C.strlen(tn))
  if e != 0 {
    C.hb_mutation_destroy((C.hb_mutation_t)(put))
    return Errno(e)
  }

  if bufferable != nil {
    e = C.hb_mutation_set_bufferable((C.hb_mutation_t)(put), (C._Bool)(*bufferable))
    if e != 0 {
      C.hb_mutation_destroy((C.hb_mutation_t)(put))
      return Errno(e)
    }
  }

  if durability != nil {
    e = C.hb_mutation_set_durability((C.hb_mutation_t)(put), C.hb_durability_t(*durability))
    if e != 0 {
      C.hb_mutation_destroy((C.hb_mutation_t)(put))
      return Errno(e)
    }
  }

  for _, cell := range cells {
    e = C.hb_put_add_cell(put, cell.cCell())
    if e != 0 {
      C.hb_mutation_destroy((C.hb_mutation_t)(put))
      return Errno(e)
    }
  }

  e = C.hb_mutation_send(cl.client, (C.hb_mutation_t)(put), (C.hb_mutation_cb)(C.mutation_cb), (unsafe.Pointer)(cb))
  if e != 0 {
    C.hb_mutation_destroy((C.hb_mutation_t)(put))
    return Errno(e)
  }
  return nil
}

//DeleteRow queues a request to delete a row.  The result will be placed on the cb channel.
func (cl *Client) DeleteRow(nameSpace *string, tableName string, bufferable *bool, durability *Durability, rowKey []byte, timestamp *int64, cb *chan CallbackResult) error {
  var del C.hb_delete_t

  e := C.hb_delete_create(cBytes(&rowKey), cLen(rowKey), &del)
  if e != 0 {
    return Errno(e)
  }

  if nameSpace != nil {
    ns := C.CString(*nameSpace)
    e = C.hb_mutation_set_namespace((C.hb_mutation_t)(del), ns, C.strlen(ns))
    if e != 0 {
      C.hb_mutation_destroy((C.hb_mutation_t)(del))
      return Errno(e)
    }
  }

  tn := C.CString(tableName)
  e = C.hb_mutation_set_table((C.hb_mutation_t)(del), tn, C.strlen(tn))
  if e != 0 {
    C.hb_mutation_destroy((C.hb_mutation_t)(del))
    return Errno(e)
  }

  if bufferable != nil {
    e = C.hb_mutation_set_bufferable((C.hb_mutation_t)(del), (C._Bool)(*bufferable))
    if e != 0 {
      C.hb_mutation_destroy((C.hb_mutation_t)(del))
      return Errno(e)
    }
  }

  if durability != nil {
    e = C.hb_mutation_set_durability((C.hb_mutation_t)(del), C.hb_durability_t(*durability))
    if e != 0 {
      C.hb_mutation_destroy((C.hb_mutation_t)(del))
      return Errno(e)
    }
  }

  if timestamp != nil {
    e = C.hb_delete_set_timestamp(del, C.int64_t(*timestamp))
    if e != 0 {
      C.hb_mutation_destroy((C.hb_mutation_t)(del))
      return Errno(e)
    }
  }

  e = C.hb_mutation_send(cl.client, (C.hb_mutation_t)(del), (C.hb_mutation_cb)(C.mutation_cb), (unsafe.Pointer)(cb))
  if e != 0 {
    C.hb_mutation_destroy((C.hb_mutation_t)(del))
    return Errno(e)
  }
  return nil
}

//DeleteColumn queues a request to delete a column.  The result will be placed on the cb channel.
func (cl *Client) DeleteColumns(nameSpace *string, tableName string, bufferable *bool, durability *Durability, rowKey []byte, columns []Column, cb *chan CallbackResult) error {
  var del C.hb_delete_t

  e := C.hb_delete_create(cBytes(&rowKey), cLen(rowKey), &del)
  if e != 0 {
    return Errno(e)
  }

  if nameSpace != nil {
    ns := C.CString(*nameSpace)
    e = C.hb_mutation_set_namespace((C.hb_mutation_t)(del), ns, C.strlen(ns))
    if e != 0 {
      C.hb_mutation_destroy((C.hb_mutation_t)(del))
      return Errno(e)
    }
  }

  tn := C.CString(tableName)
  e = C.hb_mutation_set_table((C.hb_mutation_t)(del), tn, C.strlen(tn))
  if e != 0 {
    C.hb_mutation_destroy((C.hb_mutation_t)(del))
    return Errno(e)
  }

  if bufferable != nil {
    e = C.hb_mutation_set_bufferable((C.hb_mutation_t)(del), (C._Bool)(*bufferable))
    if e != 0 {
      C.hb_mutation_destroy((C.hb_mutation_t)(del))
      return Errno(e)
    }
  }

  if durability != nil {
    e = C.hb_mutation_set_durability((C.hb_mutation_t)(del), C.hb_durability_t(*durability))
    if e != 0 {
      C.hb_mutation_destroy((C.hb_mutation_t)(del))
      return Errno(e)
    }
  }

  for _, column := range columns {
    var ts C.int64_t
    if column.Timestamp != nil {
      ts = C.int64_t(*column.Timestamp)
    }

    if column.Qualifier == nil {
      e = C.hb_delete_add_column(del, cBytes(&column.Family), cLen(column.Family), nil, 0, ts)
      if e != 0 {
        C.hb_mutation_destroy((C.hb_mutation_t)(del))
        return Errno(e)
      }
    } else {
      e = C.hb_delete_add_column(del, cBytes(&column.Family), cLen(column.Family), cBytes(&*column.Qualifier), cLen(*column.Qualifier), ts)
      if e != 0 {
        C.hb_mutation_destroy((C.hb_mutation_t)(del))
        return Errno(e)
      }
    }
  }

  e = C.hb_mutation_send(cl.client, (C.hb_mutation_t)(del), (C.hb_mutation_cb)(C.mutation_cb), (unsafe.Pointer)(cb))
  if e != 0 {
    C.hb_mutation_destroy((C.hb_mutation_t)(del))
    return Errno(e)
  }
  return nil
}

type Increment struct {
  Cell   *Cell
  Amount int64
}

//Increment queues a request to increment a cell.  The result will be placed on the cb channel.
func (cl *Client) Increment(nameSpace *string, tableName string, rowKey []byte, durability *Durability, cells []Increment, cb *chan CallbackResult) error {
  var incr C.hb_increment_t

  e := C.hb_increment_create(cBytes(&rowKey), cLen(rowKey), &incr)
  if e != 0 {
    return Errno(e)
  }

  if nameSpace != nil {
    ns := C.CString(*nameSpace)
    e = C.hb_mutation_set_namespace((C.hb_mutation_t)(incr), ns, C.strlen(ns))
    if e != 0 {
      C.hb_mutation_destroy((C.hb_mutation_t)(incr))
      return Errno(e)
    }
  }

  tn := C.CString(tableName)
  e = C.hb_mutation_set_table((C.hb_mutation_t)(incr), tn, C.strlen(tn))
  if e != 0 {
    C.hb_mutation_destroy((C.hb_mutation_t)(incr))
    return Errno(e)
  }

  if durability != nil {
    e = C.hb_mutation_set_durability((C.hb_mutation_t)(incr), C.hb_durability_t(*durability))
    if e != 0 {
      C.hb_mutation_destroy((C.hb_mutation_t)(incr))
      return Errno(e)
    }
  }

  for _, cell := range cells {
    e = C.hb_increment_add_column((C.hb_increment_t)(incr), cell.Cell.cCell(), C.int64_t(cell.Amount))
    if e != 0 {
      C.hb_mutation_destroy((C.hb_mutation_t)(incr))
      return Errno(e)
    }
  }

  e = C.hb_mutation_send(cl.client, (C.hb_mutation_t)(incr), (C.hb_mutation_cb)(C.mutation_cb), (unsafe.Pointer)(cb))
  if e != 0 {
    C.hb_mutation_destroy((C.hb_mutation_t)(incr))
    return Errno(e)
  }
  return nil
}

//Append queues a request to append a cell.  The result will be placed on the cb channel.
func (cl *Client) Append(nameSpace *string, tableName string, rowKey []byte, durability *Durability, cells []Cell, cb *chan CallbackResult) error {
  var app C.hb_append_t

  e := C.hb_append_create(cBytes(&rowKey), cLen(rowKey), &app)
  if e != 0 {
    return Errno(e)
  }

  if nameSpace != nil {
    ns := C.CString(*nameSpace)
    e = C.hb_mutation_set_namespace((C.hb_mutation_t)(app), ns, C.strlen(ns))
    if e != 0 {
      C.hb_mutation_destroy((C.hb_mutation_t)(app))
      return Errno(e)
    }
  }

  tn := C.CString(tableName)
  e = C.hb_mutation_set_table((C.hb_mutation_t)(app), tn, C.strlen(tn))
  if e != 0 {
    C.hb_mutation_destroy((C.hb_mutation_t)(app))
    return Errno(e)
  }

  if durability != nil {
    e = C.hb_mutation_set_durability((C.hb_mutation_t)(app), C.hb_durability_t(*durability))
    if e != 0 {
      C.hb_mutation_destroy((C.hb_mutation_t)(app))
      return Errno(e)
    }
  }

  for _, cell := range cells {
    e = C.hb_append_add_column(app, cell.cCell())
    if e != 0 {
      C.hb_mutation_destroy((C.hb_mutation_t)(app))
      return Errno(e)
    }
  }

  e = C.hb_mutation_send(cl.client, (C.hb_mutation_t)(app), (C.hb_mutation_cb)(C.mutation_cb), (unsafe.Pointer)(cb))
  if e != 0 {
    C.hb_mutation_destroy((C.hb_mutation_t)(app))
    return Errno(e)
  }
  return nil
}

//export mutationCallback
func mutationCallback(e C.int32_t, client C.hb_client_t, mutation C.hb_mutation_t, result C.hb_result_t, extra unsafe.Pointer) {
  var err error
  if e != 0 {
    err = Errno(e)
  }

  cb := (*chan CallbackResult)(extra)
  *cb <- CallbackResult{[]*Result{newResult(result)}, err}
  C.hb_mutation_destroy(mutation)
}
