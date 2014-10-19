package main

import (
  "fmt"
  "github.com/fanatic/gohbase"
)

func main() {
  // Connect!
  conn, err := gohbase.NewConn([]string{"192.168.2.107"})
  if err != nil {
    fmt.Printf("Connection error: %v\n", err)
    return
  }

  // Create Table (replace if exists)!
  colFamilies := [][]byte{[]byte("Id"), []byte("Name"), []byte("Family3")}
  if err := conn.CreateTable("/tables/jptest", colFamilies, true); err != nil {
    fmt.Printf("createTable: %v\n", err)
    return
  }

  // Put 10 Rows!
  err = put(conn, "/tables/jptest", 10)
  if err != nil {
    fmt.Printf("put: %v\n", err)
    return
  }

  // Get 1st Row!
  cb := make(chan gohbase.CallbackResult)
  err = conn.Get("/tables/jptest", []byte("row-1"), cb)
  if err != nil {
    fmt.Printf("get: %v\n", err)
    return
  }
  result := <-cb
  if result.Err != nil {
    fmt.Printf("get: %v\n", result.Err)
    return
  }
  result.PrintAllResults()

  // Get All Rows!
  cb2 := make(chan gohbase.CallbackResult)
  err = conn.Scan("/tables/jptest", cb2)
  if err != nil {
    fmt.Printf("scan: %v\n", err)
    return
  }
  for result := range cb2 {
    if result.Err != nil {
      fmt.Printf("scan: %v\n", result.Err)
      return
    }
    if len(result.Results) > 0 {
      result.PrintAllResults()
    } else {
      break
    }
  }

  // Clean up!
  if err := conn.Close(); err != nil {
    fmt.Printf("close connection: %v\n", err)
  }
}

func put(conn *gohbase.Conn, tableName string, numRows int) error {
  cb := make(chan gohbase.CallbackResult)

  for i := 0; i < numRows; i++ {
    row := []byte(fmt.Sprintf("row-%d", i))

    cells := []gohbase.Cell{
      gohbase.Cell{row, []byte("Name"), []byte("First"), []byte(fmt.Sprintf("first-%d", i)), nil},
      gohbase.Cell{row, []byte("Id"), []byte("i"), []byte(fmt.Sprintf("id-%d", i)), nil},
    }

    fmt.Printf("Put send [Id: %d]\n", i)
    if err := conn.Put(tableName, row, cells, cb); err != nil {
      return err
    }
  }

  for i := 0; i < numRows; i++ {
    result := <-cb
    if result.Err != nil {
      return result.Err
    }
    //fmt.Printf("Put Result [Id: X] %+v\n", result.Result)
    result.PrintAllResults()
  }
  return nil
}
