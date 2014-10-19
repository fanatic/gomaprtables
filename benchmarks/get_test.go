package benchmarks

import (
  "testing"
)

func BenchmarkGet(b *testing.B) {
  for n := 0; n < b.N; n++ {
    conn.Get(tableName, []byte("row-1"))
  }
}
