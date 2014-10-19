package benchmarks

import (
  "fmt"
  "github.com/fanatic/gomaprtables"
)

var conn *gomaprtables.Connection

const tableName = "/tables/jptest"

func init() {
  var err error

  // Connect
  conn, err = gomaprtables.NewConnection([]string{"192.168.2.107"})
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
}
