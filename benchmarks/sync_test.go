package benchmarks

import (
  "fmt"
  "github.com/fanatic/gomaprtables"
  "github.com/dchest/uniuri"
  "testing"
)

func benchmarkPut2(numRows, rowSize int, b *testing.B) {
  bigValue := uniuri.NewLen(rowSize/2)
  cb := make(chan gomaprtables.CallbackResult)
  for n := 0; n < b.N; n++ {

    for i := 0; i < numRows; i++ {
      row := []byte(fmt.Sprintf("row-%d-%d", rowSize, i))

      cells := []gomaprtables.Cell{
        gomaprtables.Cell{row, []byte("Name"), []byte("First"), []byte(bigValue), nil},
        gomaprtables.Cell{row, []byte("Id"), []byte("i"), []byte(bigValue), nil},
      }

      conn.Put(tableName, row, cells, &cb)
    }

    // Wait for receipt for now
    for i := 0; i < numRows; i++ {
      <-cb
    }
  }
}

func benchmarkGet2(numRows, rowSize int, b *testing.B) {
  for n := 0; n < b.N; n++ {
    key := fmt.Sprintf("row-%d-%d", rowSize, n%numRows)
    conn.Get(tableName, []byte(key))
  }
}

func BenchmarkPut_100(b *testing.B)   { benchmarkPut2(500, 100, b) }
func BenchmarkGet_100(b *testing.B)   { benchmarkGet2(500, 100, b) }
func BenchmarkPut_1K(b *testing.B)   { benchmarkPut2(500, 1*1024, b) }
func BenchmarkGet_1K(b *testing.B)   { benchmarkGet2(500, 1*1024, b) }
func BenchmarkPut_5K(b *testing.B)   { benchmarkPut2(500, 5*1024, b) }
func BenchmarkGet_5K(b *testing.B)   { benchmarkGet2(500, 5*1024, b) }
func BenchmarkPut_10K(b *testing.B)   { benchmarkPut2(500, 10*1024, b) }
func BenchmarkGet_10K(b *testing.B)   { benchmarkGet2(500, 10*1024, b) }
func BenchmarkPut_20K(b *testing.B)   { benchmarkPut2(500, 20*1024, b) }
func BenchmarkGet_20K(b *testing.B)   { benchmarkGet2(500, 20*1024, b) }
func BenchmarkPut_50K(b *testing.B)   { benchmarkPut2(500, 50*1024, b) }
func BenchmarkGet_50K(b *testing.B)   { benchmarkGet2(500, 50*1024, b) }
func BenchmarkPut_100K(b *testing.B)   { benchmarkPut2(500, 100*1024, b) }
func BenchmarkGet_100K(b *testing.B)   { benchmarkGet2(500, 100*1024, b) }
