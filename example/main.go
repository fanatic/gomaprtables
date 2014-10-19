package main

import (
  "fmt"
  "github.com/fanatic/gomaprtables"
)

const tableName = "/tables/jptest"

func main() {
  // Connect!
  conn, err := gomaprtables.NewConnection([]string{"192.168.2.107"})
  if err != nil {
    fmt.Printf("Connection error: %v\n", err)
    return
  }

  // Create Table (replace if exists)!
  colFamilies := [][]byte{[]byte("Id"), []byte("Name"), []byte("Family3")}
  if err := conn.CreateTable(tableName, colFamilies, true); err != nil {
    fmt.Printf("createTable: %v\n", err)
    return
  }

  // Put 10 Rows!
  err = put(conn, tableName, 10)
  if err != nil {
    fmt.Printf("put: %v\n", err)
    return
  }

  // Get 1st Row! (syncronously)
  result, err := conn.Get(tableName, []byte("row-1"))
  if err != nil {
    fmt.Printf("get: %v\n", err)
    return
  }
  result.PrintResult()

  // Get All Rows!
  results, err := conn.Scan(tableName)
  if err != nil {
    fmt.Printf("scan: %v\n", err)
    return
  }
  for _, result := range results {
    result.PrintResult()
  }

  // Clean up!
  if err := conn.Close(); err != nil {
    fmt.Printf("close connection: %v\n", err)
  }
}

func put(conn *gomaprtables.Connection, tableName string, numRows int) error {
  cb := make(chan gomaprtables.CallbackResult)

  for i := 0; i < numRows; i++ {
    row := []byte(fmt.Sprintf("row-%d", i))

    cells := []gomaprtables.Cell{
      gomaprtables.Cell{row, []byte("Name"), []byte("First"), []byte(fmt.Sprintf("first-%d", i)), nil},
      gomaprtables.Cell{row, []byte("Id"), []byte("i"), []byte(fmt.Sprintf("id-%d", i)), nil},
    }

    if err := conn.Put(tableName, row, cells, cb); err != nil {
      return err
    }
  }

  for i := 0; i < numRows; i++ {
    result := <-cb
    if result.Err != nil {
      return result.Err
    }
    result.PrintAllResults()
  }
  return nil
}
