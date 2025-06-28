package storage

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/atomicutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/filestream"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"path/filepath"
	"strings"
	"sync"
)

// blockStreamWriter represents block stream writer.
type blockStreamWriter struct {
	compressLevel int

	timestampsWriter filestream.WriteCloser
	valuesWriter     filestream.WriteCloser
	indexWriter      filestream.WriteCloser
	metaindexWriter  filestream.WriteCloser

	mr metaindexRow

	timestampsBlockOffset uint64
	valuesBlockOffset     uint64
	indexBlockOffset      uint64

	indexData           []byte
	compressedIndexData []byte

	metaindexData           []byte
	compressedMetaindexData []byte

	// prevTimestamps* is used as an optimization for reducing disk space usage
	// when serially written blocks have identical timestamps.
	// This is usually the case when adjacent blocks contain metrics scraped from the same target,
	// since such metrics have identical timestamps.
	prevTimestampsData        []byte
	prevTimestampsBlockOffset uint64
}

// Init initializes bsw with the given writers.
func (bsw *blockStreamWriter) reset() {
	bsw.compressLevel = 0

	bsw.timestampsWriter = nil
	bsw.valuesWriter = nil
	bsw.indexWriter = nil
	bsw.metaindexWriter = nil

	bsw.mr.Reset()

	bsw.timestampsBlockOffset = 0
	bsw.valuesBlockOffset = 0
	bsw.indexBlockOffset = 0

	bsw.indexData = bsw.indexData[:0]
	bsw.compressedIndexData = bsw.compressedIndexData[:0]

	bsw.metaindexData = bsw.metaindexData[:0]
	bsw.compressedMetaindexData = bsw.compressedMetaindexData[:0]

	bsw.prevTimestampsData = bsw.prevTimestampsData[:0]
	bsw.prevTimestampsBlockOffset = 0
}

// MustInitFromInmemoryPart initializes bsw from inmemory part.
func (bsw *blockStreamWriter) MustInitFromInmemoryPart(mp *inmemoryPart, compressLevel int) {
	bsw.reset()

	bsw.compressLevel = compressLevel
	bsw.timestampsWriter = &mp.timestampsData
	bsw.valuesWriter = &mp.valuesData
	bsw.indexWriter = &mp.indexData
	bsw.metaindexWriter = &mp.metaindexData
}

// MustInitFromFilePart initializes bsw from a file-based part on the given path.
//
// The bsw doesn't pollute OS page cache if nocache is set.
func (bsw *blockStreamWriter) MustInitFromFilePart(path string, nocache bool, compressLevel int) {
	path = filepath.Clean(path)

	// Create the directory
	fs.MustMkdirFailIfExist(path)

	// Create part files in the directory.
	timestampsPath := filepath.Join(path, timestampsFilename)
	timestampsFile := filestream.MustCreate(timestampsPath, nocache)

	valuesPath := filepath.Join(path, valuesFilename)
	valuesFile := filestream.MustCreate(valuesPath, nocache)

	indexPath := filepath.Join(path, indexFilename)
	indexFile := filestream.MustCreate(indexPath, nocache)

	// Always cache metaindex file in OS page cache, since it is immediately
	// read after the merge.
	metaindexPath := filepath.Join(path, metaindexFilename)
	metaindexFile := filestream.MustCreate(metaindexPath, false)

	bsw.reset()
	bsw.compressLevel = compressLevel

	bsw.timestampsWriter = timestampsFile
	bsw.valuesWriter = valuesFile
	bsw.indexWriter = indexFile
	bsw.metaindexWriter = metaindexFile
}

// MustClose closes the bsw.
//
// It closes *Writer files passed to Init*.
func (bsw *blockStreamWriter) MustClose() {
	// Flush remaining data.
	bsw.flushIndexData()

	// Write metaindex data.
	bsw.compressedMetaindexData = encoding.CompressZSTDLevel(bsw.compressedMetaindexData[:0], bsw.metaindexData, bsw.compressLevel)
	fs.MustWriteData(bsw.metaindexWriter, bsw.compressedMetaindexData)

	// Close writers.
	bsw.timestampsWriter.MustClose()
	bsw.valuesWriter.MustClose()
	bsw.indexWriter.MustClose()
	bsw.metaindexWriter.MustClose()

	bsw.reset()
}

func (bsw *blockStreamWriter) Finish() error {
	mergeId := bsw.getMergeId()
	if mergeId == "" {
		return nil
	}
	m, ok := globalUpdateIdMap[mergeId]
	if !ok {
		return nil
	}
	for shardingKey, uploadId := range m {
		//objectName := mergeId + "-values.bin"
		objectName := getObjectName(shardingKey, mergeId)
		buffer := globalBufferMap[mergeId][shardingKey]
		i := globalPartNumberMap[mergeId][shardingKey]
		uploadParts := globalUploadPartMap[mergeId][shardingKey]
		if buffer.Len() > 0 {
			uploadPart, err := ossWriteData(objectName, uploadId, *i, buffer.Bytes())
			if err != nil {
				return err
			}
			uploadParts = append(uploadParts, uploadPart)
		}
		if uploadParts == nil {
			continue
		}
		err := CompleteMultipartUpload(uploadId, objectName, uploadParts)
		if err != nil {
			return err
		}
	}
	delete(globalUpdateIdMap, mergeId)
	delete(globalPartNumberMap, mergeId)
	delete(globalOffsetMap, mergeId)
	delete(globalUploadPartMap, mergeId)
	delete(globalBufferMap, mergeId)
	return nil
}

func getObjectName(shardingKey string, mergeId string) string {
	return shardingKey + "/" + mergeId + "-values.bin"
}

var (
	globalUpdateIdMap   = make(map[string]map[string]string)
	globalPartNumberMap = make(map[string]map[string]*int32)
	globalOffsetMap     = make(map[string]map[string]*uint64)
	globalUploadPartMap = make(map[string]map[string][]oss.UploadPart)
	globalBufferMap     = make(map[string]map[string]*bytes.Buffer)
)

// WriteExternalBlock writes b to bsw and updates ph and rowsMerged.
func (bsw *blockStreamWriter) WriteExternalBlock(s *Storage, b *Block, ph *partHeader, rowsMerged *uint64, dstPartType partType) {
	mergeId := bsw.getMergeId()
	isOss := dstPartType != partInmemory && isObjectStorageAvailable(s, b) && len(mergeId) > 0
	if isOss {
		b.adjustValues()
		if len(b.values) == 0 {
			return
		}
	}
	*rowsMerged += uint64(b.rowsCount())
	b.deduplicateSamplesDuringMerge()
	headerData, timestampsData, valuesData := b.MarshalData(bsw.timestampsBlockOffset, bsw.valuesBlockOffset)

	usePrevTimestamps := len(bsw.prevTimestampsData) > 0 && bytes.Equal(timestampsData, bsw.prevTimestampsData)
	if usePrevTimestamps {
		// The current timestamps block equals to the previous timestamps block.
		// Update headerData so it points to the previous timestamps block. This saves disk space.
		headerData, timestampsData, valuesData = b.MarshalData(bsw.prevTimestampsBlockOffset, bsw.valuesBlockOffset)
		timestampsBlocksMerged.Add(1)
		timestampsBytesSaved.Add(uint64(len(timestampsData)))
	}

	if len(bsw.indexData)+len(headerData) > maxBlockSize {
		bsw.flushIndexData()
	}
	bsw.indexData = append(bsw.indexData, headerData...)
	bsw.mr.RegisterBlockHeader(&b.bh)

	if isOss {
		shardingKey := getShardingKey(b)
		objectName := getObjectName(shardingKey, mergeId)
		var updateIdMap = globalUpdateIdMap[mergeId]
		if updateIdMap == nil {
			updateIdMap = make(map[string]string)
			globalUpdateIdMap[mergeId] = updateIdMap
		}
		var partNumberMap = globalPartNumberMap[mergeId]
		if partNumberMap == nil {
			partNumberMap = make(map[string]*int32)
			globalPartNumberMap[mergeId] = partNumberMap
		}
		var offsetMap = globalOffsetMap[mergeId]
		if offsetMap == nil {
			offsetMap = make(map[string]*uint64)
			globalOffsetMap[mergeId] = offsetMap
		}
		var uploadPartMap = globalUploadPartMap[mergeId]
		if uploadPartMap == nil {
			uploadPartMap = make(map[string][]oss.UploadPart)
			globalUploadPartMap[mergeId] = uploadPartMap
		}
		var bufferMap = globalBufferMap[mergeId]
		if bufferMap == nil {
			bufferMap = make(map[string]*bytes.Buffer)
			globalBufferMap[mergeId] = bufferMap
		}

		uploadId, ok := updateIdMap[shardingKey]
		if !ok {
			u, err := InitiateMultipartUpload(objectName)
			if err != nil {
				fmt.Errorf("InitiateMultipartUpload error: %v", err)
			}
			updateIdMap[shardingKey] = u
			uploadId = u
		}
		i, ok := partNumberMap[shardingKey]
		if !ok {
			one := int32(1)
			i = &one
		}
		offset, ok := offsetMap[shardingKey]
		if !ok {
			zero := uint64(0)
			offset = &zero
		}
		buffers, ok := bufferMap[shardingKey]
		if !ok {
			buffers = &bytes.Buffer{}
			bufferMap[shardingKey] = buffers
		}

		if len(b.valuesData) > 0 {
			buffers.Write(b.valuesData)
			//bufferMap[shardingKey] = buffers
			for {
				if buffers.Len() > 100*1024 {
					bs := make([]byte, 100*1024)
					buffers.Read(bs)

					p, err := ossWriteData(objectName, uploadId, *i, bs)
					*i++
					partNumberMap[shardingKey] = i
					if err != nil {
						fmt.Errorf("ossWriteData error: %v", err)
					} else {
						parts, ok := uploadPartMap[shardingKey]
						if !ok {
							parts = make([]oss.UploadPart, 0)
							uploadPartMap[shardingKey] = parts
						}
						parts = append(parts, p)
						uploadPartMap[shardingKey] = parts
					}
				} else {
					break
				}
			}
		}
		logger.Infof("block write to buffers, metricId = %d, offset = %d, size = %d, content = %s, count = %d", b.bh.TSID.MetricID, *offset, len(valuesData), base64.StdEncoding.EncodeToString(valuesData), b.bh.RowsCount)
		partNumberMap[shardingKey] = i
		ph.IsObjectStorage = true
		bsw.valuesBlockOffset = *offset
		*offset += uint64(len(valuesData))
		offsetMap[shardingKey] = offset
	} else {
		if !usePrevTimestamps {
			bsw.prevTimestampsData = append(bsw.prevTimestampsData[:0], timestampsData...)
			bsw.prevTimestampsBlockOffset = bsw.timestampsBlockOffset
			fs.MustWriteData(bsw.timestampsWriter, timestampsData)
			bsw.timestampsBlockOffset += uint64(len(timestampsData))
		}
		fs.MustWriteData(bsw.valuesWriter, valuesData)
		bsw.valuesBlockOffset += uint64(len(valuesData))
		ph.IsObjectStorage = false
	}
	updatePartHeader(b, ph)
}

func (bsw *blockStreamWriter) getMergeId() string {
	writer, ok := bsw.indexWriter.(*filestream.Writer)
	if !ok {
		return ""
	}
	writer.Path()
	split := strings.Split(writer.Path(), "/")
	return split[len(split)-2]
}

func (b *Block) adjustValues() {
	if len(b.values) == 0 || len(b.timestamps) == 0 {
		return
	}
	timestamps := b.timestamps
	values := b.values
	vs := make([]int64, 0)
	ts := make([]int64, 0)
	lastTimeStamps := timestamps[len(timestamps)-1]
	value := values[0]
	timestamp := timestamps[0]
	times := (lastTimeStamps-timestamp)/ObjectStepDuring + 1
	vs = append(vs, value)
	ts = append(ts, timestamp)

	step := 1
	for i := 1; i < int(times); i++ {
		minTimestamp := timestamp + int64((i-1)*ObjectStepDuring)
		maxTimestamp := timestamp + int64(i*ObjectStepDuring)
		temp := int64(1<<63 - 2) //NaN
		for j := step; j < len(values); j++ {
			if timestamps[j] > maxTimestamp {
				break
			} else if timestamps[j] <= minTimestamp {
				continue
			}
			temp = values[j]
			step = j
		}
		vs = append(vs, temp)
		ts = append(ts, maxTimestamp)
	}
	b.values = vs
	b.timestamps = ts
}

var (
	timestampsBlocksMerged atomicutil.Uint64
	timestampsBytesSaved   atomicutil.Uint64
)

func updatePartHeader(b *Block, ph *partHeader) {
	ph.BlocksCount++
	ph.RowsCount += uint64(b.bh.RowsCount)
	if b.bh.MinTimestamp < ph.MinTimestamp {
		ph.MinTimestamp = b.bh.MinTimestamp
	}
	if b.bh.MaxTimestamp > ph.MaxTimestamp {
		ph.MaxTimestamp = b.bh.MaxTimestamp
	}
}

func (bsw *blockStreamWriter) flushIndexData() {
	if len(bsw.indexData) == 0 {
		return
	}

	// Write compressed index block to index data.
	bsw.compressedIndexData = encoding.CompressZSTDLevel(bsw.compressedIndexData[:0], bsw.indexData, bsw.compressLevel)
	indexBlockSize := len(bsw.compressedIndexData)
	if uint64(indexBlockSize) >= 1<<32 {
		logger.Panicf("BUG: indexBlock size must fit uint32; got %d", indexBlockSize)
	}
	fs.MustWriteData(bsw.indexWriter, bsw.compressedIndexData)

	// Write metaindex row to metaindex data.
	bsw.mr.IndexBlockOffset = bsw.indexBlockOffset
	bsw.mr.IndexBlockSize = uint32(indexBlockSize)
	bsw.metaindexData = bsw.mr.Marshal(bsw.metaindexData)

	// Update offsets.
	bsw.indexBlockOffset += uint64(indexBlockSize)

	bsw.indexData = bsw.indexData[:0]
	bsw.mr.Reset()
}

func getBlockStreamWriter() *blockStreamWriter {
	v := bswPool.Get()
	if v == nil {
		return &blockStreamWriter{}
	}
	return v.(*blockStreamWriter)
}

func putBlockStreamWriter(bsw *blockStreamWriter) {
	bsw.reset()
	bswPool.Put(bsw)
}

var bswPool sync.Pool
