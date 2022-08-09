// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package array

import (
	"errors"
	"fmt"
	"math"
	"sync/atomic"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/internal/debug"
)

// type aliases to preserve functionality and avoid breaking consumers
// by the shift to arrow.Table, arrow.Column and arrow.Chunked over array each.
type (
	// Table aliases arrow.Table
	//
	// Deprecated: this alias will be removed in v8
	Table = arrow.Table
	// Column aliases arrow.Column
	//
	// Deprecated: this alias will be removed in v8
	Column = arrow.Column
	// Chunked aliases arrow.Chunked
	//
	// Deprecated: this alias will be removed in v8
	Chunked = arrow.Chunked
)

var (
	// NewColumn aliases the arrow.NewColumn function to avoid breaking consumers.
	//
	// Deprecated: this alias will be removed in v8
	NewColumn = arrow.NewColumn
	// NewChunked aliases the arrow.NewChunked function to avoid breaking consumers.
	//
	// Deprecated: this alias will be removed in v8
	NewChunked = arrow.NewChunked
)

// NewColumnSlice returns a new zero-copy slice of the column with the indicated
// indices i and j, corresponding to the column's array[i:j].
// The returned column must be Release()'d after use.
//
// NewColSlice panics if the slice is outside the valid range of the column's array.
// NewColSlice panics if j < i.
func NewColumnSlice(col *arrow.Column, i, j int64) *arrow.Column {
	slice := NewChunkedSlice(col.Data(), i, j)
	defer slice.Release()
	return arrow.NewColumn(col.Field(), slice)
}

// NewChunkedSlice constructs a zero-copy slice of the chunked array with the indicated
// indices i and j, corresponding to array[i:j].
// The returned chunked array must be Release()'d after use.
//
// NewSlice panics if the slice is outside the valid range of the input array.
// NewSlice panics if j < i.
func NewChunkedSlice(a *arrow.Chunked, i, j int64) *Chunked {
	if j > int64(a.Len()) || i > j || i > int64(a.Len()) {
		panic("arrow/array: index out of range")
	}

	var (
		cur    = 0
		beg    = i
		sz     = j - i
		chunks = make([]arrow.Array, 0, len(a.Chunks()))
	)

	for cur < len(a.Chunks()) && beg >= int64(a.Chunks()[cur].Len()) {
		beg -= int64(a.Chunks()[cur].Len())
		cur++
	}

	for cur < len(a.Chunks()) && sz > 0 {
		arr := a.Chunks()[cur]
		end := beg + sz
		if end > int64(arr.Len()) {
			end = int64(arr.Len())
		}
		chunks = append(chunks, NewSlice(arr, beg, end))
		sz -= int64(arr.Len()) - beg
		beg = 0
		cur++
	}
	chunks = chunks[:len(chunks):len(chunks)]
	defer func() {
		for _, chunk := range chunks {
			chunk.Release()
		}
	}()

	return NewChunked(a.DataType(), chunks)
}

// simpleTable is a basic, non-lazy in-memory table.
type simpleTable struct {
	refCount int64

	rows int64
	cols []Column

	schema *arrow.Schema
}

// NewTable returns a new basic, non-lazy in-memory table.
// If rows is negative, the number of rows will be inferred from the height
// of the columns.
//
// NewTable panics if the columns and schema are inconsistent.
// NewTable panics if rows is larger than the height of the columns.
func NewTable(schema *arrow.Schema, cols []Column, rows int64) *simpleTable {
	tbl := simpleTable{
		refCount: 1,
		rows:     rows,
		cols:     cols,
		schema:   schema,
	}

	if tbl.rows < 0 {
		switch len(tbl.cols) {
		case 0:
			tbl.rows = 0
		default:
			tbl.rows = int64(tbl.cols[0].Len())
		}
	}

	// validate the table and its constituents.
	// note we retain the columns after having validated the table
	// in case the validation fails and panics (and would otherwise leak
	// a ref-count on the columns.)
	tbl.validate()

	for i := range tbl.cols {
		tbl.cols[i].Retain()
	}

	return &tbl
}

// NewTableFromRecords returns a new basic, non-lazy in-memory table.
//
// NewTableFromRecords panics if the records and schema are inconsistent.
func NewTableFromRecords(schema *arrow.Schema, recs []Record) *simpleTable {
	arrs := make([]arrow.Array, len(recs))
	cols := make([]Column, len(schema.Fields()))

	defer func(cols []Column) {
		for i := range cols {
			cols[i].Release()
		}
	}(cols)

	for i := range cols {
		field := schema.Field(i)
		for j, rec := range recs {
			arrs[j] = rec.Column(i)
		}
		chunk := arrow.NewChunked(field.Type, arrs)
		cols[i] = *arrow.NewColumn(field, chunk)
		chunk.Release()
	}

	return NewTable(schema, cols, -1)
}

func (tbl *simpleTable) Schema() *arrow.Schema { return tbl.schema }
func (tbl *simpleTable) NumRows() int64        { return tbl.rows }
func (tbl *simpleTable) NumCols() int64        { return int64(len(tbl.cols)) }
func (tbl *simpleTable) Column(i int) *Column  { return &tbl.cols[i] }

func (tbl *simpleTable) validate() {
	if len(tbl.cols) != len(tbl.schema.Fields()) {
		panic(errors.New("arrow/array: table schema mismatch"))
	}
	for i, col := range tbl.cols {
		if !col.Field().Equal(tbl.schema.Field(i)) {
			panic(fmt.Errorf("arrow/array: column field %q is inconsistent with schema", col.Name()))
		}

		if int64(col.Len()) < tbl.rows {
			panic(fmt.Errorf("arrow/array: column %q expected length >= %d but got length %d", col.Name(), tbl.rows, col.Len()))
		}
	}
}

// Retain increases the reference count by 1.
// Retain may be called simultaneously from multiple goroutines.
func (tbl *simpleTable) Retain() {
	atomic.AddInt64(&tbl.refCount, 1)
}

// Release decreases the reference count by 1.
// When the reference count goes to zero, the memory is freed.
// Release may be called simultaneously from multiple goroutines.
func (tbl *simpleTable) Release() {
	debug.Assert(atomic.LoadInt64(&tbl.refCount) > 0, "too many releases")

	if atomic.AddInt64(&tbl.refCount, -1) == 0 {
		for i := range tbl.cols {
			tbl.cols[i].Release()
		}
		tbl.cols = nil
	}
}

// TableReader is a Record iterator over a (possibly chunked) Table
type TableReader struct {
	refCount int64

	tbl   Table
	cur   int64  // current row
	max   int64  // total number of rows
	rec   Record // current Record
	chksz int64  // chunk size

	chunks  []*Chunked
	slots   []int   // chunk indices
	offsets []int64 // chunk offsets
}

// NewTableReader returns a new TableReader to iterate over the (possibly chunked) Table.
// if chunkSize is <= 0, the biggest possible chunk will be selected.
func NewTableReader(tbl Table, chunkSize int64) *TableReader {
	ncols := tbl.NumCols()
	tr := &TableReader{
		refCount: 1,
		tbl:      tbl,
		cur:      0,
		max:      int64(tbl.NumRows()),
		chksz:    chunkSize,
		chunks:   make([]*Chunked, ncols),
		slots:    make([]int, ncols),
		offsets:  make([]int64, ncols),
	}
	tr.tbl.Retain()

	if tr.chksz <= 0 {
		tr.chksz = math.MaxInt64
	}

	for i := range tr.chunks {
		col := tr.tbl.Column(i)
		tr.chunks[i] = col.Data()
		tr.chunks[i].Retain()
	}
	return tr
}

func (tr *TableReader) Schema() *arrow.Schema { return tr.tbl.Schema() }
func (tr *TableReader) Record() Record        { return tr.rec }

func (tr *TableReader) Next() bool {
	if tr.cur >= tr.max {
		return false
	}

	if tr.rec != nil {
		tr.rec.Release()
	}

	// determine the minimum contiguous slice across all columns
	chunksz := imin64(tr.max, tr.chksz)
	chunks := make([]arrow.Array, len(tr.chunks))
	for i := range chunks {
		j := tr.slots[i]
		chunk := tr.chunks[i].Chunk(j)
		remain := int64(chunk.Len()) - tr.offsets[i]
		if remain < chunksz {
			chunksz = remain
		}

		chunks[i] = chunk
	}

	// slice the chunks, advance each chunk slot as appropriate.
	batch := make([]arrow.Array, len(tr.chunks))
	for i, chunk := range chunks {
		var slice arrow.Array
		offset := tr.offsets[i]
		switch int64(chunk.Len()) - offset {
		case chunksz:
			tr.slots[i]++
			tr.offsets[i] = 0
			if offset > 0 {
				// need to slice
				slice = NewSlice(chunk, offset, offset+chunksz)
			} else {
				// no need to slice
				slice = chunk
				slice.Retain()
			}
		default:
			tr.offsets[i] += chunksz
			slice = NewSlice(chunk, offset, offset+chunksz)
		}
		batch[i] = slice
	}

	tr.cur += chunksz
	tr.rec = NewRecord(tr.tbl.Schema(), batch, chunksz)

	for _, arr := range batch {
		arr.Release()
	}

	return true
}

// Retain increases the reference count by 1.
// Retain may be called simultaneously from multiple goroutines.
func (tr *TableReader) Retain() {
	atomic.AddInt64(&tr.refCount, 1)
}

// Release decreases the reference count by 1.
// When the reference count goes to zero, the memory is freed.
// Release may be called simultaneously from multiple goroutines.
func (tr *TableReader) Release() {
	debug.Assert(atomic.LoadInt64(&tr.refCount) > 0, "too many releases")

	if atomic.AddInt64(&tr.refCount, -1) == 0 {
		tr.tbl.Release()
		for _, chk := range tr.chunks {
			chk.Release()
		}
		if tr.rec != nil {
			tr.rec.Release()
		}
		tr.tbl = nil
		tr.chunks = nil
		tr.slots = nil
		tr.offsets = nil
	}
}

func imin64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

var (
	_ Table        = (*simpleTable)(nil)
	_ RecordReader = (*TableReader)(nil)
)
