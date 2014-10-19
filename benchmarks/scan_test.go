package benchmarks

import (
  "testing"
)

func BenchmarkScan100000(b *testing.B) {
  for n := 0; n < b.N; n++ {
    conn.Scan(tableName)
  }
}
