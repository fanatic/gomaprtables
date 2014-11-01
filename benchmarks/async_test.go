package benchmarks

import (
  "fmt"
  "github.com/fanatic/gomaprtables"
  "testing"
)

func benchmarkScan(numRows, rowSize int, b *testing.B) {
  client, _ := conn.NewClient()
  for n := 0; n < b.N; n++ {
    start := fmt.Sprintf("row-%d", rowSize)
    end := fmt.Sprintf("row-%d", rowSize+1)
    cb := make(chan gomaprtables.CallbackResult)
    if err := client.Scan(nil, tableName, []byte(start), []byte(end), nil, nil, nil, &cb); err != nil {
      fmt.Printf("Error: %v\n", err)
    }
    count := 0
    for result := range cb {
      if result.Err != nil {
        fmt.Printf("Error: %v\n", result.Err)
      }
      if len(result.Results) > 0 {
        count += len(result.Results)
      } else {
        break
      }
    }

  }
}

func BenchmarkPutAsync_100(b *testing.B)  { benchmarkPut2(500, 100, b) }
func BenchmarkGetAsync_100(b *testing.B)  { benchmarkScan(500, 100, b) }
func BenchmarkPutAsync_1K(b *testing.B)   { benchmarkPut2(500, 1*1024, b) }
func BenchmarkGetAsync_1K(b *testing.B)   { benchmarkScan(500, 1*1024, b) }
func BenchmarkPutAsync_5K(b *testing.B)   { benchmarkPut2(500, 5*1024, b) }
func BenchmarkGetAsync_5K(b *testing.B)   { benchmarkScan(500, 5*1024, b) }
func BenchmarkPutAsync_10K(b *testing.B)  { benchmarkPut2(500, 10*1024, b) }
func BenchmarkGetAsync_10K(b *testing.B)  { benchmarkScan(500, 10*1024, b) }
func BenchmarkPutAsync_20K(b *testing.B)  { benchmarkPut2(500, 20*1024, b) }
func BenchmarkGetAsync_20K(b *testing.B)  { benchmarkScan(500, 20*1024, b) }
func BenchmarkPutAsync_50K(b *testing.B)  { benchmarkPut2(500, 50*1024, b) }
func BenchmarkGetAsync_50K(b *testing.B)  { benchmarkScan(500, 50*1024, b) }
func BenchmarkPutAsync_100K(b *testing.B) { benchmarkPut2(500, 100*1024, b) }
func BenchmarkGetAsync_100K(b *testing.B) { benchmarkScan(500, 100*1024, b) }
