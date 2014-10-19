package main

import (
  "fmt"
  "github.com/fanatic/gohbase"
  "time"
)

func main() {
  conn, err := gohbase.NewConn([]string{"192.168.2.107"})
  if err != nil {
    fmt.Printf("Connection error: %v\n", err)
    return
  }

  colFamilies := [][]byte{[]byte("Id"), []byte("Name"), []byte("Family3")}
  if err := conn.CreateTable("/tables/jptest", colFamilies, true); err != nil {
    fmt.Printf("createTable: %v\n", err)
    return
  }

  err = put(conn, "/tables/jptest", 10)
  if err != nil {
    fmt.Printf("put: %v\n", err)
    return
  }

  err = conn.Get("/tables/jptest", []byte("row-1"))
  if err != nil {
    fmt.Printf("get: %v\n", err)
    return
  }
  time.Sleep(3 * time.Second)

  err = conn.Scan("/tables/jptest")
  if err != nil {
    fmt.Printf("scan: %v\n", err)
    return
  }
  time.Sleep(3 * time.Second)

  if err := conn.Close(); err != nil {
    fmt.Printf("close connection: %v\n", err)
  }
}

func put(conn *gohbase.Conn, tableName string, numRows int) error {
  for i := 0; i < numRows; i++ {
    row := []byte(fmt.Sprintf("row-%d", i))

    cells := []gohbase.Cell{
      gohbase.Cell{row, []byte("Name"), []byte("First"), []byte(fmt.Sprintf("first-%d", i)), nil},
      gohbase.Cell{row, []byte("Id"), []byte("i"), []byte(fmt.Sprintf("id-%d", i)), nil},
    }

    fmt.Printf("Put send [Id: %d]\n", i)
    if err := conn.Put(tableName, row, cells); err != nil {
      return err
    }
  }
  time.Sleep(3 * time.Second)
  return nil
}
