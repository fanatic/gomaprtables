package gomaprtables

import (
  "fmt"
)

func (conn *Connection) ensureAdminClient() error {
  if conn.a == nil {
    var err error
    conn.a, err = conn.NewAdminClient()
    if err != nil {
      return fmt.Errorf("Admin client: %v\n", err)
    }
  }
  return nil
}

//CreateTable creates the given table, doing all the dirty work for you
func (conn *Connection) CreateTable(tableName string, columnFamilies [][]byte, deleteIfExist bool) error {
  if err := conn.ensureAdminClient(); err != nil {
    return err
  }

  families := []*ColDesc{}
  for _, family := range columnFamilies {
    families = append(families, &ColDesc{Name: []byte(family)})
  }

  if deleteIfExist {
    if err := conn.a.IsTableExist(nil, tableName); err == nil {
      if err := conn.a.DeleteTable(nil, tableName); err != nil {
        return fmt.Errorf("Deleting table: %v\n", err)
      }
    }
  }

  if err := conn.a.CreateTable(nil, tableName, families); err != nil {
    return fmt.Errorf("create table: %v\n", err)
  }
  return nil
}

func (conn *Connection) ensureClient() error {
  if conn.c == nil {
    var err error
    conn.c, err = conn.NewClient()
    if err != nil {
      return fmt.Errorf("Client: %v\n", err)
    }
  }
  return nil
}

//Put adds a row to a table, doing all the dirty work for you
func (conn *Connection) Put(tableName string, rowKey []byte, cells []Cell, cb *chan CallbackResult) error {
  if err := conn.ensureClient(); err != nil {
    return err
  }
  if err := conn.c.Put(nil, tableName, nil, nil, rowKey, cells, cb); err != nil {
    return err
  }
  return nil
}

//Flush flushes pending puts, deletes, increments, and appends
func (conn *Connection) Flush() error {
  if err := conn.ensureClient(); err != nil {
    return err
  }
  if err := conn.c.Flush(); err != nil {
    return err
  }
  return nil
}

//Get retrieves a row from a table, doing all the dirty work for you
func (conn *Connection) Get(tableName string, rowKey []byte) (*Result, error) {
  if err := conn.ensureClient(); err != nil {
    return nil, err
  }

  cb := make(chan CallbackResult)

  if err := conn.c.Get(nil, tableName, rowKey, nil, nil, nil, nil, nil, &cb); err != nil {
    return nil, err
  }

  result := <-cb
  if result.Err != nil {
    return nil, result.Err
  }

  return result.Results[0], nil
}

//Scan retrieves several rows from a table, doing all the dirty work for you
func (conn *Connection) Scan(tableName string) ([]*Result, error) {
  if err := conn.ensureClient(); err != nil {
    return nil, err
  }

  cb := make(chan CallbackResult)

  if err := conn.c.Scan(nil, tableName, nil, nil, nil, nil, nil, &cb); err != nil {
    return nil, err
  }

  results := []*Result{}
  for result := range cb {
    if result.Err != nil {
      return nil, result.Err
    }
    if len(result.Results) > 0 {
      results = append(results, result.Results...)
    } else {
      break
    }
  }
  return results, nil
}
