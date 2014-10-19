package benchmarks

import (
  "fmt"
  "github.com/fanatic/gomaprtables"
  "testing"
)

func benchmarkPut(numRows int, b *testing.B) {
  for n := 0; n < b.N; n++ {
    cb := make(chan gomaprtables.CallbackResult)

    for i := 0; i < numRows; i++ {
      row := []byte(fmt.Sprintf("row-%d", i))

      cells := []gomaprtables.Cell{
        gomaprtables.Cell{row, []byte("Name"), []byte("First"), []byte(fmt.Sprintf("first-%d", i)), nil},
        gomaprtables.Cell{row, []byte("Id"), []byte("i"), []byte(fmt.Sprintf("id-%d", i)), nil},
      }

      conn.Put(tableName, row, cells, cb)
      //fmt.Printf("Put sent\n")
    }

    // Wait for receipt for now
    for i := 0; i < numRows; i++ {
      <-cb
    }

  }
}

func BenchmarkPut1(b *testing.B)   { benchmarkPut(1, b) }
func BenchmarkPut10(b *testing.B)  { benchmarkPut(10, b) }
func BenchmarkPut100(b *testing.B) { benchmarkPut(100, b) }

//func BenchmarkPut1000(b *testing.B)   { benchmarkPut(1000, b) }
//func BenchmarkPut10000(b *testing.B)  { benchmarkPut(10000, b) }
//func BenchmarkPut100000(b *testing.B) { benchmarkPut(100000, b) }
