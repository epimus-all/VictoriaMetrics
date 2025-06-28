package storage

import (
	"fmt"
	"testing"
)

func BenchmarkBlockStreamWriterBlocksWorstCase(b *testing.B) {
	benchmarkBlockStreamWriter(b, benchBlocksWorstCase, len(benchRawRowsWorstCase), false)
}

func BenchmarkBlockStreamWriterBlocksBestCase(b *testing.B) {
	benchmarkBlockStreamWriter(b, benchBlocksBestCase, len(benchRawRowsBestCase), false)
}

func BenchmarkBlockStreamWriterRowsWorstCase(b *testing.B) {
	benchmarkBlockStreamWriter(b, benchBlocksWorstCase, len(benchRawRowsWorstCase), true)
}

func BenchmarkBlockStreamWriterRowsBestCase(b *testing.B) {
	benchmarkBlockStreamWriter(b, benchBlocksBestCase, len(benchRawRowsBestCase), true)
}

func BenchmarkBlockStreamWriterRowsBestCases(b *testing.B) {
	var bb = Block{
		values:     []int64{11, 12, 13, 14, 15},
		timestamps: []int64{1750065000000, 1750065000001, 1750065060000, 1750065180000, 1750065240000},
	}
	bb.adjustValues()

}

func benchmarkBlockStreamWriter(b *testing.B, ebs []Block, rowsCount int, writeRows bool) {
	b.ReportAllocs()
	b.SetBytes(int64(rowsCount))
	b.RunParallel(func(pb *testing.PB) {
		var rowsMerged uint64
		var bsw blockStreamWriter
		var mp inmemoryPart
		var ph partHeader
		var ebsCopy []Block
		for i := range ebs {
			var ebCopy Block
			ebCopy.CopyFrom(&ebs[i])
			ebsCopy = append(ebsCopy, ebCopy)
		}
		loopCount := 0

		for pb.Next() {
			if writeRows {
				for i := range ebsCopy {
					eb := &ebsCopy[i]
					if err := eb.UnmarshalData(); err != nil {
						panic(fmt.Errorf("cannot unmarshal block %d on loop %d: %w", i, loopCount, err))
					}
				}
			}

			bsw.MustInitFromInmemoryPart(&mp, -5)
			for i := range ebsCopy {
				bsw.WriteExternalBlock(nil, &ebsCopy[i], &ph, &rowsMerged, partInmemory)
			}
			bsw.MustClose()
			mp.Reset()
			loopCount++
		}
	})
}

var benchBlocksWorstCase = newBenchBlocks(benchRawRowsWorstCase)
var benchBlocksBestCase = newBenchBlocks(benchRawRowsBestCase)

func newBenchBlocks(rows []rawRow) []Block {
	var ebs []Block

	mp := newTestInmemoryPart(rows)
	var bsr blockStreamReader
	bsr.MustInitFromInmemoryPart(mp)
	for bsr.NextBlock() {
		var eb Block
		eb.CopyFrom(&bsr.Block)
		ebs = append(ebs, eb)
	}
	if err := bsr.Error(); err != nil {
		panic(fmt.Errorf("unexpected error when reading inmemoryPart: %w", err))
	}
	return ebs
}
