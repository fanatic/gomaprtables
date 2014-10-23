package benchmarks

import (
  "fmt"
  "github.com/fanatic/gomaprtables"
  "os"
  "strings"
)

var conn *gomaprtables.Connection

var tableName string

func init() {
  var err error

  tableName = "/tables/jptest"
  if os.Getenv("TABLENAME") != "" {
    tableName = os.Getenv("TABLENAME")
  }
  cldbs := "192.168.2.107"
  if os.Getenv("CLDBS") != "" {
    cldbs = os.Getenv("CLDBS")
  }

  // Connect
  conn, err = gomaprtables.NewConnection(strings.Split(cldbs, ","))
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
