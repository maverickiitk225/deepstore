package engine

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkPutConcurrent measures durable Put throughput with c concurrent writers.
// Each Put returns only after its record is fsynced, so this is bound by fsync cost.
func BenchmarkPutConcurrent(b *testing.B) {
	for _, c := range []int{1, 4, 16, 64, 256} {
		b.Run(fmt.Sprintf("writers=%d", c), func(b *testing.B) {
			e, err := Open(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			defer e.Close()
			e.SetSnapshotEvery(0)

			var next atomic.Int64
			lat := make([][]time.Duration, c)
			var wg sync.WaitGroup
			wg.Add(c)
			b.ResetTimer()
			for w := range c {
				go func(w int) {
					defer wg.Done()
					for {
						i := next.Add(1)
						if i > int64(b.N) {
							return
						}
						start := time.Now()
						if _, err := e.Put(fmt.Sprintf("k%d", i), "value", uint64(i), 1); err != nil {
							b.Error(err)
							return
						}
						lat[w] = append(lat[w], time.Since(start))
					}
				}(w)
			}
			wg.Wait()
			b.StopTimer()

			all := slices.Concat(lat...)
			slices.Sort(all)
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "writes/s")
			b.ReportMetric(float64(all[len(all)/2].Microseconds()), "p50-µs")
			b.ReportMetric(float64(all[len(all)*99/100].Microseconds()), "p99-µs")
		})
	}
}
