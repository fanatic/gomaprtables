package benchmarks

/*
import (
  "fmt"
  "github.com/fanatic/gomaprtables"
  "github.com/dchest/uniuri"
  "testing"
)

func benchmarkPut(numRows, rowSize int, b *testing.B) {
  bigValue := uniuri.NewLen(rowSize/2)
  for n := 0; n < b.N; n++ {
    cb := make(chan gomaprtables.CallbackResult)

    for i := 0; i < numRows; i++ {
      row := []byte(fmt.Sprintf("row-%d-%d", i, rowSize))

      cells := []gomaprtables.Cell{
        gomaprtables.Cell{row, []byte("Name"), []byte("First"), []byte(bigValue), nil},
        gomaprtables.Cell{row, []byte("Id"), []byte("i"), []byte(bigValue), nil},
      }

      conn.Put(tableName, row, cells, cb)
    }

    // Wait for receipt for now
    for i := 0; i < numRows; i++ {
      <-cb
    }
  }
}

func BenchmarkPut100(b *testing.B)   { benchmarkPut(500, 100, b) }
*/
