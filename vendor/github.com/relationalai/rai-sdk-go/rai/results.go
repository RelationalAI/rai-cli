// Copyright 2022 RelationalAI, Inc.

package rai

// Support for accessing transaction results.

// Transaction results consist of the transaction resource, relations and
// metadata describing those relations. The relations may be output relations
// and/or relations describing problems with the transaction. The metadata is
// encoded using protobuf, and all relations are encoded using Apache Arrow.
//
// RelationalAI represents relations in a low-level physical format known as a
// Partition, where constant values are lifted into the metadata, and the
// relation data is partitioned by the resulting unique metadata signatures.
// For example:
//
//     def output = 1, :foo; 2, :bar; 3, :baz
//
//  results in 3 partitions, each with a unique metadata signature, and in this
//  example, a single column with a single row of data:
//
//     sig: (Int64, :foo), data: [[1]]
//     sig: (Int64, :bar), data: [[2]]
//     sig: (Int64, :baz), data: [[3]]
//
// This representation eliminats the duplication of constant values.
//
// This file provides accessors for the raw partition data, accessors for
// projections of that data back to its relational form (with constants
// restored to value space), and operations for combining and projecting
// relations.

import (
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/array"
	"github.com/apache/arrow/go/v7/arrow/float16"
	"github.com/shopspring/decimal"
)

type floatTypes interface {
	float16.Num | float32 | float64
}

type intTypes interface {
	int8 | int16 | int32 | int64 | uint8 | uint16 | uint32 | uint64
}

type PrimitiveTypes interface {
	bool | floatTypes | intTypes | string
}

type SimpleTypes interface {
	PrimitiveTypes | *big.Int | *big.Rat | decimal.Decimal | time.Time
}

// Column provides access to a single column of data.
type Column interface {
	NumRows() int
	String(int) string
	Type() any // reflect.Type | MixedType
	Value(int) any
}

// DataColumn is a Column with a typed accessor.
type DataColumn[T any] interface {
	Column
	Item(int) T
}

// SimpleColumn is a DataColumn with a typed placement accessor for simple
// types.
type SimpleColumn[T SimpleTypes] interface {
	DataColumn[T]
	GetItem(int, *T)
}

// TabularColumn is a DataColumn with a typed placement accessor for composite
// types.
type TabularColumn[T any] interface {
	DataColumn[[]T]
	GetItem(int, []T)
	NumCols() int
	Strings(int) []string
}

// Tabular is a generic interface to a sequence of columns of data with a
// type siganture.
type Tabular interface {
	Column
	Column(int) Column
	Columns() []Column
	GetRow(int, []any)
	NumCols() int
	Row(int) []any
	Signature() Signature
	Strings(int) []string
}

type Relation interface {
	Tabular
	Showable
	Slice(int, ...int) Relation
}

func asString(v any) string {
	switch vv := v.(type) {
	case rune:
		return fmt.Sprintf("'%c'", vv)
	case string:
		return fmt.Sprintf("\"%s\"", vv)
	case time.Time:
		return vv.Format(time.RFC3339)
	default:
		return fmt.Sprintf("%v", vv)
	}
}

// Answers if the given signature has a prefix that matches the given terms,
// where '_' is a single term wildcard.
func matchPrefix(sig []any, terms ...string) bool {
	n := len(terms)
	if len(sig) < n {
		return false
	}
	for i, term := range terms {
		s, ok := sig[i].(string)
		if !ok {
			return false
		}
		if term == "_" {
			continue
		}
		if s != term {
			return false
		}
	}
	return true
}

// Represents a column of primitive values.
type primitiveColumn[T PrimitiveTypes] struct {
	data []T
}

func newPrimitiveColumn[T PrimitiveTypes](d []T) SimpleColumn[T] {
	return primitiveColumn[T]{d}
}

func (c primitiveColumn[T]) GetItem(rnum int, out *T) {
	*out = c.data[rnum]
}

func (c primitiveColumn[T]) Item(rnum int) T {
	return c.data[rnum]
}

func (c primitiveColumn[T]) NumRows() int {
	return len(c.data)
}

func (c primitiveColumn[T]) String(rnum int) string {
	return fmt.Sprintf("%v", c.data[rnum])
}

func (c primitiveColumn[T]) Type() any {
	return typeOf[T]()
}

func (c primitiveColumn[T]) Value(rnum int) any {
	return c.data[rnum]
}

// Sadly, the `array.Boolean` type does not have a `Values` accessor.
type boolColumn struct {
	data *array.Boolean
}

func newBoolColumn(data *array.Boolean) SimpleColumn[bool] {
	return boolColumn{data}
}

func (c boolColumn) GetItem(rnum int, out *bool) {
	*out = c.data.Value(rnum)
}

func (c boolColumn) Item(rnum int) bool {
	return c.data.Value(rnum)
}

func (c boolColumn) NumRows() int {
	return c.data.Len()
}

func (c boolColumn) String(rnum int) string {
	return strconv.FormatBool(c.data.Value(rnum))
}

func (c boolColumn) Type() any {
	return BoolType
}

func (c boolColumn) Value(rnum int) any {
	return c.data.Value(rnum)
}

type float16Column struct {
	primitiveColumn[float16.Num]
}

func newFloat16Column(data []float16.Num) SimpleColumn[float16.Num] {
	return float16Column{primitiveColumn[float16.Num]{data}}
}

func (c float16Column) Item(rnum int) float16.Num {
	return c.data[rnum]
}

func (c float16Column) Type() any {
	return Float16Type
}

func newFloat32Column(data []float32) SimpleColumn[float32] {
	return primitiveColumn[float32]{data}
}

func newFloat64Column(data []float64) SimpleColumn[float64] {
	return primitiveColumn[float64]{data}
}

// Sadly, the `array.String“ type does not have a `Values` accessor.
type stringColumn struct {
	data *array.String
}

func newStringColumn(data *array.String) SimpleColumn[string] {
	return stringColumn{data}
}

func (c stringColumn) GetItem(rnum int, out *string) {
	*out = c.data.Value(rnum)
}

func (c stringColumn) Item(rnum int) string {
	return c.data.Value(rnum)
}

func (c stringColumn) NumRows() int {
	return c.data.Len()
}

func (c stringColumn) String(rnum int) string {
	return c.data.Value(rnum)
}

func (c stringColumn) Type() any {
	return StringType
}

func (c stringColumn) Value(rnum int) any {
	return c.data.Value(rnum)
}

type listColumn[T any] struct {
	data  []T // raw arrow data
	ncols int
	cols  []Column
}

func (c listColumn[T]) Column(cnum int) Column {
	return listItemColumn[T]{c.data, cnum, c.ncols}
}

func (c listColumn[T]) Columns() []Column {
	if c.cols == nil {
		c.cols = make([]Column, c.ncols)
		for i := 0; i < c.ncols; i++ {
			c.cols[i] = listItemColumn[T]{c.data, c.ncols, i}
		}
	}
	return c.cols
}

func (c listColumn[T]) GetItem(rnum int, out []T) {
	roffs := rnum * c.ncols
	for cnum := 0; cnum < c.ncols; cnum++ {
		out[cnum] = c.data[roffs+cnum]
	}
}

func (c listColumn[T]) Item(rnum int) []T {
	result := make([]T, c.ncols)
	c.GetItem(rnum, result)
	return result
}

func (c listColumn[T]) NumCols() int {
	return c.ncols
}

func (c listColumn[T]) NumRows() int {
	return len(c.data) / c.ncols
}

func (c listColumn[T]) GetRow(rnum int, out []any) {
	roffs := rnum * c.ncols
	for cnum := 0; cnum < c.ncols; cnum++ {
		out[cnum] = c.data[roffs+cnum]
	}
}

func (c listColumn[T]) Row(rnum int) []any {
	result := make([]any, c.ncols)
	c.GetRow(rnum, result)
	return result
}

func (c listColumn[T]) Signature() Signature {
	t := typeOf[T]()
	result := make([]any, c.ncols)
	for i := 0; i < c.ncols; i++ {
		result[i] = t
	}
	return result
}

func (c listColumn[T]) String(rnum int) string {
	return "(" + strings.Join(c.Strings(rnum), ", ") + ")"
}

func (c listColumn[T]) Strings(rnum int) []string {
	roffs := rnum * c.ncols
	result := make([]string, c.ncols)
	for cnum := 0; cnum < c.ncols; cnum++ {
		result[cnum] = asString(c.data[roffs+cnum])
	}
	return result
}

func (c listColumn[T]) Type() any {
	return reflect.TypeOf(*new([]T))
}

func (c listColumn[T]) Value(rnum int) any {
	return c.Item(rnum)
}

func newFloat64ListColumn(v []float64, ncols int) TabularColumn[float64] {
	return listColumn[float64]{v, ncols, nil}
}

func newInt8ListColumn(data []int8, ncols int) TabularColumn[int8] {
	return listColumn[int8]{data, ncols, nil}
}

func newInt16ListColumn(data []int16, ncols int) TabularColumn[int16] {
	return listColumn[int16]{data, ncols, nil}
}

func newInt32ListColumn(data []int32, ncols int) TabularColumn[int32] {
	return listColumn[int32]{data, ncols, nil}
}

func newInt64ListColumn(data []int64, ncols int) TabularColumn[int64] {
	return listColumn[int64]{data, ncols, nil}
}

func newUint64ListColumn(data []uint64, ncols int) TabularColumn[uint64] {
	return listColumn[uint64]{data, ncols, nil}
}

func newListColumn(c *array.FixedSizeList) Column {
	col := c.ListValues()
	nrows := c.Len()
	nvals := col.Len()
	ncols := nvals / nrows
	switch cc := col.(type) {
	case *array.Float64:
		return newFloat64ListColumn(cc.Float64Values(), ncols)
	case *array.Int8:
		return newInt8ListColumn(cc.Int8Values(), ncols)
	case *array.Int16:
		return newInt16ListColumn(cc.Int16Values(), ncols)
	case *array.Int32:
		return newInt32ListColumn(cc.Int32Values(), ncols)
	case *array.Int64:
		return newInt64ListColumn(cc.Int64Values(), ncols)
	case *array.Uint64:
		return newUint64ListColumn(cc.Uint64Values(), ncols)
	case *array.FixedSizeList: // Rational128
		ccv := cc.ListValues().(*array.Uint64)
		return newUint64ListColumn(ccv.Uint64Values(), 4)
	}
	return newUnknownColumn(nrows)
}

// Represents one sub-column of a `listColumn`
type listItemColumn[T any] struct {
	data  []T
	cnum  int
	ncols int
}

func (c listItemColumn[T]) GetItem(rnum int, out *T) {
	*out = c.data[(rnum*c.ncols)+c.cnum]
}

func (c listItemColumn[T]) Item(rnum int) T {
	return c.data[(rnum*c.ncols)+c.cnum]
}

func (c listItemColumn[T]) NumRows() int {
	return len(c.data) / c.ncols
}

func (c listItemColumn[T]) String(rnum int) string {
	return asString(c.Item(rnum))
}

func (c listItemColumn[T]) Type() any {
	return typeOf[T]()
}

func (c listItemColumn[T]) Value(rnum int) any {
	return c.Item(rnum)
}

type structColumn struct {
	cols []Column
}

// Note, its possible for a `structColumn` to be empty.
func newStructColumn(c *array.Struct) TabularColumn[any] {
	ncols := c.NumField()
	cols := make([]Column, ncols)
	for i := 0; i < ncols; i++ {
		cols[i] = newPartitionColumn(c.Field(i), c.Len())
	}
	return structColumn{cols}
}

func (c structColumn) Column(rnum int) Column {
	return c.cols[rnum]
}

func (c structColumn) Columns() []Column {
	return c.cols
}

func (c structColumn) GetItem(rnum int, out []any) {
	for n, c := range c.cols {
		out[n] = c.Value(rnum)
	}
}

func (c structColumn) GetRow(rnum int, out []any) {
	for cnum := 0; cnum < len(c.cols); cnum++ {
		out[cnum] = c.cols[cnum].Value(rnum)
	}
}

func (c structColumn) Item(rnum int) []any {
	row := make([]any, len(c.cols))
	c.GetItem(rnum, row)
	return row
}

func (c structColumn) NumCols() int {
	return len(c.cols)
}

func (c structColumn) NumRows() int {
	if len(c.cols) == 0 {
		return 0
	}
	return c.cols[0].NumRows()
}

func (c structColumn) Row(rnum int) []any {
	result := make([]any, len(c.cols))
	c.GetRow(rnum, result)
	return result
}

func (c structColumn) Signature() Signature {
	ncols := len(c.cols)
	result := make([]any, ncols)
	for i := 0; i < ncols; i++ {
		result[i] = c.cols[i].Type()
	}
	return result
}

func (c structColumn) String(rnum int) string {
	return "(" + strings.Join(c.Strings(rnum), ", ") + ")"
}

func (c structColumn) Type() any {
	return AnyListType
}

func (c structColumn) Value(rnum int) any {
	return c.Item(rnum)
}

func (c structColumn) Strings(rnum int) []string {
	ncols := len(c.cols)
	result := make([]string, ncols)
	for cnum := 0; cnum < ncols; cnum++ {
		result[cnum] = c.cols[cnum].String(rnum)
	}
	return result
}

// Represents a column with an unknown data type.
type unknownColumn struct {
	nrows int
}

func newUnknownColumn(nrows int) SimpleColumn[string] {
	return unknownColumn{nrows}
}

func (c unknownColumn) NumRows() int {
	return c.nrows
}

const unknown = "unknown"

func (c unknownColumn) GetItem(_ int, out *string) {
	*out = unknown
}

func (c unknownColumn) Item(_ int) string {
	return unknown
}

func (c unknownColumn) String(_ int) string {
	return unknown
}

func (c unknownColumn) Type() any {
	return StringType
}

func (c unknownColumn) Value(_ int) any {
	return unknown
}

// Returns the native type corresponding to elements of the given arrow array.
func columnType(c arrow.Array) reflect.Type {
	switch cc := c.(type) {
	case *array.Boolean:
		return BoolType
	case *array.Float16:
		return Float16Type
	case *array.Float32:
		return Float32Type
	case *array.Float64:
		return Float64Type
	case *array.Int8:
		return Int8Type
	case *array.Int16:
		return Int16Type
	case *array.Int32:
		return Int32Type
	case *array.Int64:
		return Int64Type
	case *array.String:
		return StringType
	case *array.Uint8:
		return Uint8Type
	case *array.Uint16:
		return Uint16Type
	case *array.Uint32:
		return Uint32Type
	case *array.Uint64:
		return Uint64Type
	case *array.FixedSizeList:
		switch cc.ListValues().(type) {
		case *array.Float32:
			return Float32ListType
		case *array.Float64:
			return Float64ListType
		case *array.Int8:
			return Int8ListType
		case *array.Int16:
			return Int16ListType
		case *array.Int32:
			return Int32ListType
		case *array.Int64:
			return Int64ListType
		case *array.Uint64:
			return Uint64ListType
		case *array.FixedSizeList:
			return Uint64ListType // Rational128
		default:
			return UnknownType
		}
	default:
		// case *array.Struct:
		return reflect.TypeOf(c).Elem()
	}
}

func (p *Partition) init() *Partition {
	if p.cols == nil {
		ncols := p.NumCols()
		p.cols = make([]Column, ncols)
		for i := 0; i < ncols; i++ {
			p.cols[i] = p.newColumn(i)
		}
	}
	return p
}

// Partition is the physical representation of relation data. Partitions may
// be shared by relations in the case where they only differ by constant values
// in the relation signature.
func newPartition(record arrow.Record) *Partition {
	return (&Partition{record: record}).init()
}

func (p *Partition) Column(rnum int) Column {
	return p.cols[rnum]
}

func (p *Partition) Columns() []Column {
	return p.cols
}

func (p *Partition) GetItem(rnum int, out []any) {
	p.GetRow(rnum, out)
}

func (p *Partition) GetRow(rnum int, out []any) {
	ncols := len(p.cols)
	for c := 0; c < ncols; c++ {
		out[c] = p.cols[c].Value(rnum)
	}
}

func (p *Partition) Item(rnum int) []any {
	return p.Row(rnum)
}

func (p *Partition) NumCols() int {
	return int(p.record.NumCols())
}

func (p *Partition) NumRows() int {
	return int(p.record.NumRows())
}

func (p *Partition) Row(rnum int) []any {
	result := make([]any, len(p.cols))
	p.GetRow(rnum, result)
	return result
}

// Returns the type signature describing the partition.
func (p *Partition) Signature() Signature {
	cols := p.record.Columns()
	result := make(Signature, len(cols))
	for i := 0; i < len(cols); i++ {
		result[i] = columnType(cols[i])
	}
	return result
}

func (p *Partition) Strings(rnum int) []string {
	ncols := len(p.cols)
	row := make([]string, ncols)
	for cnum := 0; cnum < ncols; cnum++ {
		row[cnum] = p.cols[cnum].String(rnum)
	}
	return row
}

func (p *Partition) Record() arrow.Record {
	return p.record
}

func (p *Partition) String(rnum int) string {
	return "(" + strings.Join(p.Strings(rnum), ", ") + ")"
}

func (p *Partition) Type() any {
	return AnyListType
}

func (p *Partition) Value(rnum int) any {
	return p.Row(rnum)
}

// Returns a column accessor for the given arrow array.
func newPartitionColumn(a arrow.Array, nrows int) Column {
	switch aa := a.(type) {
	case *array.Boolean:
		return newBoolColumn(aa)
	case *array.Float16:
		return newFloat16Column(aa.Values())
	case *array.Float32:
		return newFloat32Column(aa.Float32Values())
	case *array.Float64:
		return newFloat64Column(aa.Float64Values())
	case *array.Int8:
		return newPrimitiveColumn(aa.Int8Values())
	case *array.Int16:
		return newPrimitiveColumn(aa.Int16Values())
	case *array.Int32:
		return newPrimitiveColumn(aa.Int32Values())
	case *array.Int64:
		return newPrimitiveColumn(aa.Int64Values())
	case *array.String:
		return newStringColumn(aa)
	case *array.Uint8:
		return newPrimitiveColumn(aa.Uint8Values())
	case *array.Uint16:
		return newPrimitiveColumn(aa.Uint16Values())
	case *array.Uint32:
		return newPrimitiveColumn(aa.Uint32Values())
	case *array.Uint64:
		return newPrimitiveColumn(aa.Uint64Values())
	case *array.FixedSizeList:
		return newListColumn(aa)
	case *array.Struct:
		return newStructColumn(aa)
	}
	return newUnknownColumn(nrows)
}

// Returns a column accessor for the given partition column index.
func (p *Partition) newColumn(rnum int) Column {
	return newPartitionColumn(p.record.Column(rnum), p.NumRows())
}

// Characters are represented in arrow as uint32.
type charColumn struct {
	col SimpleColumn[uint32]
}

func newCharColumn(c SimpleColumn[uint32]) SimpleColumn[rune] {
	return charColumn{c}
}

func (c charColumn) GetItem(rnum int, out *rune) {
	*out = rune(c.col.Item(rnum))
}

func (c charColumn) Item(rnum int) rune {
	return rune(c.col.Item(rnum))
}

func (c charColumn) NumRows() int {
	return c.col.NumRows()
}

func (c charColumn) String(rnum int) string {
	return string(rune(c.col.Item(rnum)))
}

func (c charColumn) Type() any {
	return RuneType
}

func (c charColumn) Value(rnum int) any {
	return rune(c.col.Item(rnum))
}

type dateColumn struct {
	col DataColumn[int64]
}

func newDateColumn(col DataColumn[int64]) SimpleColumn[time.Time] {
	return dateColumn{col}
}

func (c dateColumn) GetItem(rnum int, out *time.Time) {
	*out = c.Item(rnum)
}

func (c dateColumn) Item(rnum int) time.Time {
	v := c.col.Item(rnum) // days since 1AD (Rata Die)
	return DateFromRataDie(v)
}

func (c dateColumn) NumRows() int {
	return c.col.NumRows()
}

func (c dateColumn) String(rnum int) string {
	return c.Item(rnum).Format("2006-01-02")
}

func (c dateColumn) Type() any {
	return TimeType
}

func (c dateColumn) Value(rnum int) any {
	return c.Item(rnum)
}

type dateTimeColumn struct {
	col DataColumn[int64]
}

func newDateTimeColumn(c DataColumn[int64]) SimpleColumn[time.Time] {
	return dateTimeColumn{c}
}

func (c dateTimeColumn) GetItem(rnum int, out *time.Time) {
	*out = c.Item(rnum)
}

func (c dateTimeColumn) Item(rnum int) time.Time {
	v := c.col.Item(rnum) // millis since 1AD
	return DateFromRataMillis(v)
}

func (c dateTimeColumn) NumRows() int {
	return c.col.NumRows()
}

func (c dateTimeColumn) String(rnum int) string {
	return c.Item(rnum).Format(time.RFC3339)
}

func (c dateTimeColumn) Type() any {
	return TimeType
}

func (c dateTimeColumn) Value(rnum int) any {
	return c.Item(rnum)
}

// decimalColumn projects the underlying pair of values as a decimal.
type decimalColumn[T int8 | int16 | int32 | int64] struct {
	col    DataColumn[T]
	digits int32
}

func (c decimalColumn[T]) NumRows() int {
	return c.col.NumRows()
}

func (c decimalColumn[T]) Type() any {
	return DecimalType
}

type decimal8Column struct {
	decimalColumn[int8]
}

func newDecimal8Column(col DataColumn[int8], digits int32) SimpleColumn[decimal.Decimal] {
	return decimal8Column{decimalColumn[int8]{col, digits}}
}

func (c decimal8Column) GetItem(rnum int, out *decimal.Decimal) {
	*out = decimal.New(int64(c.col.Item(rnum)), c.digits)
}

func (c decimal8Column) Item(rnum int) decimal.Decimal {
	return decimal.New(int64(c.col.Item(rnum)), c.digits)
}

func (c decimal8Column) String(rnum int) string {
	return c.Item(rnum).String()
}

func (c decimal8Column) Value(rnum int) any {
	return c.Item(rnum)
}

type decimal16Column struct {
	decimalColumn[int16]
}

func newDecimal16Column(col DataColumn[int16], digits int32) SimpleColumn[decimal.Decimal] {
	return decimal16Column{decimalColumn[int16]{col, digits}}
}

func (c decimal16Column) GetItem(rnum int, out *decimal.Decimal) {
	*out = c.Item(rnum)
}

func (c decimal16Column) Item(rnum int) decimal.Decimal {
	v := c.col.Item(rnum)
	return decimal.New(int64(v), c.digits)
}

func (c decimal16Column) String(rnum int) string {
	return c.Item(rnum).String()
}

func (c decimal16Column) Value(rnum int) any {
	return c.Item(rnum)
}

type decimal32Column struct {
	decimalColumn[int32]
}

func newDecimal32Column(col DataColumn[int32], digits int32) SimpleColumn[decimal.Decimal] {
	return decimal32Column{decimalColumn[int32]{col, digits}}
}

func (c decimal32Column) GetItem(rnum int, out *decimal.Decimal) {
	*out = c.Item(rnum)
}

func (c decimal32Column) Item(rnum int) decimal.Decimal {
	v := c.col.Item(rnum)
	return decimal.New(int64(v), c.digits)
}

func (c decimal32Column) String(rnum int) string {
	return c.Item(rnum).String()
}

func (c decimal32Column) Value(rnum int) any {
	return c.Item(rnum)
}

type decimal64Column struct {
	decimalColumn[int64]
}

func newDecimal64Column(col DataColumn[int64], digits int32) SimpleColumn[decimal.Decimal] {
	return decimal64Column{decimalColumn[int64]{col, digits}}
}

func (c decimal64Column) GetItem(rnum int, out *decimal.Decimal) {
	*out = c.Item(rnum)
}

func (c decimal64Column) Item(rnum int) decimal.Decimal {
	v := c.col.Item(rnum)
	return decimal.New(int64(v), c.digits)
}

func (c decimal64Column) String(rnum int) string {
	return c.Item(rnum).String()
}

func (c decimal64Column) Value(rnum int) any {
	return c.Item(rnum)
}

type decimal128Column struct {
	col    TabularColumn[uint64]
	digits int32
}

func newDecimal128Column(col TabularColumn[uint64], digits int32) SimpleColumn[decimal.Decimal] {
	return decimal128Column{col, digits}
}

func (c decimal128Column) GetItem(rnum int, out *decimal.Decimal) {
	*out = c.Item(rnum)
}

func (c decimal128Column) Item(rnum int) decimal.Decimal {
	var v [2]uint64
	c.col.GetItem(rnum, v[:])
	return NewDecimal128(v[0], v[1], c.digits)
}

func (c decimal128Column) NumRows() int {
	return c.col.NumRows()
}

func (c decimal128Column) String(rnum int) string {
	return c.Item(rnum).String()
}

func (c decimal128Column) Type() any {
	return DecimalType
}

func (c decimal128Column) Value(rnum int) any {
	return c.Item(rnum)
}

func newDecimalColumn(vt ValueType, c Column) Column {
	digits := -int32(vt[4].(int64))
	switch vt[3].(int64) {
	case 8:
		return newDecimal8Column(c.(DataColumn[int8]), digits)
	case 16:
		return newDecimal16Column(c.(DataColumn[int16]), digits)
	case 32:
		return newDecimal32Column(c.(DataColumn[int32]), digits)
	case 64:
		return newDecimal64Column(c.(DataColumn[int64]), digits)
	case 128:
		return newDecimal128Column(c.(TabularColumn[uint64]), digits)
	}
	return newUnknownColumn(c.NumRows())
}

// int128Column projects the underlying `[2]int64“ value as a `big.Int`.
type int128Column struct {
	col TabularColumn[uint64]
}

func newInt128Column(c TabularColumn[uint64]) SimpleColumn[*big.Int] {
	return int128Column{c}
}

func (c int128Column) GetItem(rnum int, out **big.Int) {
	*out = c.Item(rnum)
}

func (c int128Column) Item(rnum int) *big.Int {
	v := c.col.Item(rnum)
	// assert len(v) == 2
	return NewBigInt128(v[0], v[1])
}

func (c int128Column) NumRows() int {
	return c.col.NumRows()
}

func (c int128Column) String(rnum int) string {
	return c.Item(rnum).String()
}

func (c int128Column) Type() any {
	return BigIntType
}

func (c int128Column) Value(rnum int) any {
	return c.Item(rnum)
}

// uint128Column projects the underlying `[2]uint64“ value as `big.Int`.
type uint128Column struct {
	col TabularColumn[uint64]
}

func newUint128Column(c TabularColumn[uint64]) SimpleColumn[*big.Int] {
	return uint128Column{c}
}

func (c uint128Column) GetItem(rnum int, out **big.Int) {
	*out = c.Item(rnum)
}

func (c uint128Column) Item(rnum int) *big.Int {
	var v [2]uint64
	c.col.GetItem(rnum, v[:])
	return NewBigUint128(v[0], v[1])
}

func (c uint128Column) NumRows() int {
	return c.col.NumRows()
}

func (c uint128Column) String(rnum int) string {
	return c.Item(rnum).String()
}

func (c uint128Column) Type() any {
	return BigIntType
}

func (c uint128Column) Value(rnum int) any {
	return c.Item(rnum)
}

// Note, type param should be constrained to SimpleTypes, but this is used
// in too many places where we dont have the type of the parameter, and its
// extra overhead to recapture it.
type literalColumn[T any] struct {
	value T
	nrows int
}

func newLiteralColumn[T any](v T, nrows int) Column {
	return literalColumn[T]{v, nrows}
}

func (c literalColumn[T]) GetItem(rnum int, out *T) {
	*out = c.value
}

func (c literalColumn[T]) Item(rnum int) T {
	return c.value
}

func (c literalColumn[T]) NumRows() int {
	return c.nrows
}

func (c literalColumn[T]) String(_ int) string {
	return asString(c.value)
}

func (c literalColumn[T]) Type() any {
	return c.value
}

func (c literalColumn[T]) Value(_ int) any {
	return c.value
}

// rationalColumn projects the underlying pair of values as a `*big.Rat“.
type rationalColumn[T int8 | int16 | int32 | int64] struct {
	col TabularColumn[T]
}

func (c rationalColumn[T]) NumRows() int {
	return c.col.NumRows()
}

func (c rationalColumn[T]) Type() any {
	return RationalType
}

type rational8Column struct {
	rationalColumn[int8]
}

func (c rational8Column) GetItem(rnum int, out **big.Rat) {
	*out = c.Item(rnum)
}

func newRational8Column(col TabularColumn[int8]) SimpleColumn[*big.Rat] {
	return rational8Column{rationalColumn[int8]{col}}
}

func (c rational8Column) Item(rnum int) *big.Rat {
	var v [2]int8
	c.col.GetItem(rnum, v[:])
	n, d := int64(v[0]), int64(v[1])
	return big.NewRat(n, d)
}

func (c rational8Column) String(rnum int) string {
	return c.Item(rnum).String()
}

func (c rational8Column) Value(rnum int) any {
	return c.Item(rnum)
}

type rational16Column struct {
	rationalColumn[int16]
}

func newRational16Column(col TabularColumn[int16]) SimpleColumn[*big.Rat] {
	return rational16Column{rationalColumn[int16]{col}}
}

func (c rational16Column) GetItem(rnum int, out **big.Rat) {
	*out = c.Item(rnum)
}

func (c rational16Column) Item(rnum int) *big.Rat {
	var v [2]int16
	c.col.GetItem(rnum, v[:])
	n, d := int64(v[0]), int64(v[1])
	return big.NewRat(n, d)
}

func (c rational16Column) String(rnum int) string {
	return c.Item(rnum).String()
}

func (c rational16Column) Value(rnum int) any {
	return c.Item(rnum)
}

type rational32Column struct {
	rationalColumn[int32]
}

func newRational32Column(col TabularColumn[int32]) SimpleColumn[*big.Rat] {
	return rational32Column{rationalColumn[int32]{col}}
}

func (c rational32Column) GetItem(rnum int, out **big.Rat) {
	*out = c.Item(rnum)
}

func (c rational32Column) Item(rnum int) *big.Rat {
	var v [2]int32
	c.col.GetItem(rnum, v[:])
	n, d := int64(v[0]), int64(v[1])
	return big.NewRat(n, d)
}

func (c rational32Column) String(rnum int) string {
	return c.Item(rnum).String()
}

func (c rational32Column) Value(rnum int) any {
	return c.Item(rnum)
}

type rational64Column struct {
	rationalColumn[int64]
}

func newRational64Column(col TabularColumn[int64]) SimpleColumn[*big.Rat] {
	return rational64Column{rationalColumn[int64]{col}}
}

func (c rational64Column) GetItem(rnum int, out **big.Rat) {
	*out = c.Item(rnum)
}

func (c rational64Column) Item(rnum int) *big.Rat {
	var v [2]int64
	c.col.GetItem(rnum, v[:])
	return big.NewRat(v[0], v[1])
}

func (c rational64Column) String(rnum int) string {
	return c.Item(rnum).String()
}

func (c rational64Column) Value(rnum int) any {
	return c.Item(rnum)
}

type rational128Column struct {
	col TabularColumn[uint64]
}

func newRational128Column(col TabularColumn[uint64]) SimpleColumn[*big.Rat] {
	return rational128Column{col}
}

func (c rational128Column) GetItem(rnum int, out **big.Rat) {
	*out = c.Item(rnum)
}

func (c rational128Column) NumRows() int {
	return c.col.NumRows()
}

func (c rational128Column) Item(rnum int) *big.Rat {
	var v [4]uint64
	c.col.GetItem(rnum, v[:])
	n := NewBigInt128(v[0], v[1])
	d := NewBigInt128(v[2], v[3])
	return NewRational128(n, d)
}

func (c rational128Column) String(rnum int) string {
	return c.Item(rnum).String()
}

func (c rational128Column) Type() any {
	return RationalType
}

func (c rational128Column) Value(rnum int) any {
	return c.Item(rnum)
}

func newRationalColumn(c Column) Column {
	switch cc := c.(type) {
	case listColumn[int8]:
		return newRational8Column(cc)
	case listColumn[int16]:
		return newRational16Column(cc)
	case listColumn[int32]:
		return newRational32Column(cc)
	case listColumn[int64]:
		return newRational64Column(cc)
	case listColumn[uint64]:
		return newRational128Column(cc)
	}
	return newUnknownColumn(c.NumRows())
}

type symbolColumn struct {
	value string
	nrows int
}

func newSymbolColumn(v string, nrows int) SimpleColumn[string] {
	return symbolColumn{v, nrows}
}

func (c symbolColumn) GetItem(_ int, out *string) {
	*out = c.value
}

func (c symbolColumn) Item(_ int) string {
	return c.value
}

func (c symbolColumn) NumRows() int {
	return c.nrows
}

func (c symbolColumn) String(_ int) string {
	return c.value
}

func (c symbolColumn) Type() any {
	return c.value
}

func (c symbolColumn) Value(_ int) any {
	return c.value
}

const missing = "missing"

type missingColumn struct {
	nrows int
}

func newMissingColumn(nrows int) SimpleColumn[string] {
	return missingColumn{nrows}
}

func (c missingColumn) GetItem(_ int, out *string) {
	*out = missing
}

func (c missingColumn) Item(_ int) string {
	return missing
}

func (c missingColumn) NumRows() int {
	return c.nrows
}

func (c missingColumn) String(_ int) string {
	return missing
}

func (c missingColumn) Type() any {
	return MissingType
}

func (c missingColumn) Value(_ int) any {
	return missing
}

// ["rel", "base", "Decimal", <bits>, <digits>, <value>]
func newConstDecimalValue(ct ConstType) decimal.Decimal {
	digits := -int32(ct[4].(int64))
	switch ct[3].(int64) {
	case 8:
		return decimal.New(int64(ct[5].(int8)), digits)
	case 16:
		return decimal.New(int64(ct[5].(int16)), digits)
	case 32:
		return decimal.New(int64(ct[5].(int32)), digits)
	case 64:
		return decimal.New(ct[5].(int64), digits)
	case 128:
		return decimal.NewFromBigInt(ct[5].(*big.Int), digits)
	}
	return decimal.Zero // unreached
}

func newConstDecimalColumn(ct ConstType, nrows int) Column {
	switch ct[3].(int64) {
	case 8, 16, 32, 64, 128:
		break
	default:
		return newUnknownColumn(nrows)
	}
	return newLiteralColumn(newConstDecimalValue(ct), nrows)
}

// ["rel", "base", "Decimal", <bits>, <num>, <denom>]
func newConstRationalValue(ct ConstType) *big.Rat {
	switch ct[3].(int64) {
	case 8:
		n, d := ct[4].(int8), ct[5].(int8)
		return big.NewRat(int64(n), int64(d))
	case 16:
		n, d := ct[4].(int16), ct[5].(int16)
		return big.NewRat(int64(n), int64(d))
	case 32:
		n, d := ct[4].(int32), ct[5].(int32)
		return big.NewRat(int64(n), int64(d))
	case 64:
		n, d := ct[4].(int64), ct[5].(int64)
		return big.NewRat(int64(n), int64(d))
	case 128:
		n, d := ct[4].(*big.Int), ct[5].(*big.Int)
		return NewRational128(n, d)
	}
	return big.NewRat(1, 1)
}

func newConstRationalColumn(ct ConstType, nrows int) Column {
	switch ct[3].(int64) {
	case 8, 16, 32, 64, 128:
		break
	default:
		return newUnknownColumn(nrows)
	}
	return newLiteralColumn(newConstRationalValue(ct), nrows)
}

type constColumn struct {
	cols  []Column
	nrows int
	vals  []any
}

func newConstColumn(t ConstType, nrows int) Column {
	if matchPrefix(t, "rel", "base", "_") {
		switch t[2].(string) {
		case "AutoNumber":
			return newLiteralColumn(t[3].(uint64), nrows)
		case "Date":
			d := DateFromRataDie(t[3].(int64))
			return newLiteralColumn(d, nrows)
		case "DateTime":
			d := DateFromRataMillis(t[3].(int64))
			return newLiteralColumn(d, nrows)
		case "FilePos":
			return newLiteralColumn(t[3].(int64), nrows)
		case "FixedDecimal":
			return newConstDecimalColumn(t, nrows)
		case "Hash":
			return newLiteralColumn(t[3].(*big.Int), nrows)
		case "Rational":
			return newConstRationalColumn(t, nrows)
		case "Missing":
			return newMissingColumn(nrows)
		case "Year", "Month", "Week", "Day", "Hour", "Minute",
			"Second", "Millisecond", "Microsecond", "Nanosecond":
			return newLiteralColumn(t[3].(int64), nrows)
		}
	}
	cols := make([]Column, len(t))
	for i, v := range t {
		var cc Column
		switch tt := v.(type) {
		case ConstType:
			cc = newConstColumn(tt, nrows)
		case ValueType: // unexpected
			cc = newUnknownColumn(nrows)
		default:
			cc = newLiteralColumn(v, nrows)
		}
		cols[i] = cc
	}
	return constColumn{cols, nrows, nil}
}

func (c constColumn) Column(cnum int) Column {
	return c.cols[cnum]
}

func (c constColumn) Columns() []Column {
	return c.cols
}

func (c constColumn) GetItem(rnum int, out []any) {
	for cnum := 0; cnum < len(out); cnum++ {
		out[cnum] = c.cols[cnum].Value(rnum)
	}
}

func (c constColumn) GetRow(rnum int, out []any) {
	c.GetItem(rnum, out)
}

func (c constColumn) Item(_ int) []any {
	if c.vals == nil {
		c.vals = make([]any, len(c.cols))
		c.GetItem(0, c.vals)
	}
	return c.vals
}

func (c constColumn) NumCols() int {
	return len(c.cols)
}

func (c constColumn) NumRows() int {
	return c.nrows
}

func (c constColumn) Row(rnum int) []any {
	return c.Item(rnum)
}

func (c constColumn) Signature() Signature {
	ncols := len(c.cols)
	result := make([]any, ncols)
	for i := 0; i < ncols; i++ {
		result[i] = c.cols[i].Type()
	}
	return result
}

func (c constColumn) String(rnum int) string {
	return "(" + strings.Join(c.Strings(rnum), ", ") + ")"
}

func (c constColumn) Strings(rnum int) []string {
	ncols := len(c.cols)
	result := make([]string, ncols)
	for cnum := 0; cnum < ncols; cnum++ {
		result[cnum] = c.cols[cnum].String(rnum)
	}
	return result
}

func (c constColumn) Type() any {
	return AnyListType
}

func (c constColumn) Value(rnum int) any {
	return c.Item(rnum)
}

type valueColumn struct {
	cols []Column
}

func newBuiltinValueColumn(vt ValueType, c Column, nrows int) Column {
	if matchPrefix(vt, "rel", "base", "_") {
		switch vt[2].(string) {
		case "AutoNumber":
			return c // primitiveColumn[uint64]
		case "Date":
			return newDateColumn(c.(DataColumn[int64]))
		case "DateTime":
			return newDateTimeColumn(c.(DataColumn[int64]))
		case "FilePos":
			return c // primitiveColumn[int64]
		case "FixedDecimal":
			return newDecimalColumn(vt, c)
		case "Hash":
			return newUint128Column(c.(listColumn[uint64]))
		case "Rational":
			return newRationalColumn(c)
		case "Missing":
			return newMissingColumn(nrows)
		case "Year", "Month", "Week", "Day", "Hour", "Minute",
			"Second", "Millisecond", "Microsecond", "Nanosecond":
			return c
		}
	}
	return nil // not a recognized builtin value type
}

// Projects a valueColumn from an underlying simple column.
func newSimpleValueColumn(vt ValueType, c Column, nrows int) Column {
	ncols := len(vt)
	cols := make([]Column, ncols)
	for i, t := range vt {
		var cc Column
		switch tt := t.(type) {
		case ValueType:
			cc = newValueColumn(tt, c, nrows)
		default:
			cc = newRelationColumn(tt, c, nrows)
		}
		cols[i] = cc
	}
	return valueColumn{cols}
}

// Projects a valueColumn from an underlying `Tabular` column.
func newTabularValueColumn(vt ValueType, c Tabular, nrows int) Column {
	ncol := 0
	ncols := len(vt)
	cols := make([]Column, ncols)
	for i, t := range vt {
		var cc Column
		switch tt := t.(type) {
		case reflect.Type:
			cc = newRelationColumn(tt, c.Column(ncol), nrows)
			ncol++
		case ValueType:
			cc = newValueColumn(tt, c.Column(ncol), nrows)
			ncol++
		case string:
			cc = newSymbolColumn(tt, nrows)
		default:
			cc = newLiteralColumn(tt, nrows)
		}
		cols[i] = cc
	}
	return valueColumn{cols}
}

// Returns a `valueColumn` which is a projection of the given partition column.
func newValueColumn(vt ValueType, c Column, nrows int) Column {
	if cc := newBuiltinValueColumn(vt, c, nrows); cc != nil {
		return cc
	}
	if cc, ok := c.(Tabular); ok {
		return newTabularValueColumn(vt, cc, nrows)
	}
	return newSimpleValueColumn(vt, c, nrows)
}

func (c valueColumn) Column(cnum int) Column {
	return c.cols[cnum]
}

func (c valueColumn) Columns() []Column {
	return c.cols
}

func (c valueColumn) GetItem(rnum int, out []any) {
	for cnum := 0; cnum < len(out); cnum++ {
		out[cnum] = c.cols[cnum].Value(rnum)
	}
}

func (c valueColumn) GetRow(rnum int, out []any) {
	c.GetItem(rnum, out)
}

func (c valueColumn) Item(rnum int) []any {
	result := make([]any, len(c.cols))
	c.GetItem(rnum, result)
	return result
}

func (c valueColumn) NumCols() int {
	return len(c.cols)
}

func (c valueColumn) NumRows() int {
	return c.cols[0].NumRows()
}

func (c valueColumn) Row(rnum int) []any {
	return c.Item(rnum)
}

func (c valueColumn) Signature() Signature {
	ncols := len(c.cols)
	result := make([]any, ncols)
	for i := 0; i < ncols; i++ {
		result[i] = c.cols[i].Type()
	}
	return result
}

func (c valueColumn) String(rnum int) string {
	return "(" + strings.Join(c.Strings(rnum), ", ") + ")"
}

func (c valueColumn) Strings(rnum int) []string {
	ncols := len(c.cols)
	result := make([]string, ncols)
	for cnum := 0; cnum < ncols; cnum++ {
		result[cnum] = c.cols[cnum].String(rnum)
	}
	return result
}

func (c valueColumn) Type() any {
	return AnyListType
}

func (c valueColumn) Value(rnum int) any {
	return c.Item(rnum)
}

type baseRelation struct {
	meta  Signature
	part  *Partition
	sig   Signature
	cols  []Column
	nrows int
}

// Initialize row count and instantiate relation columns.
func (r *baseRelation) init() *baseRelation {
	if r.cols != nil {
		return r
	}

	// place partition columns in position coresponding to the metadata
	ncols := 0 // count of arrow columns consumed (there can be empty extras)
	pcols := make([]Column, len(r.meta))
	for i, m := range r.meta {
		if !isConstType(m) {
			pcols[i] = r.part.Column(ncols)
			ncols++
		}
	}

	// If the relation is fully specialized, the row count is 1, and there
	// will be no arrow data, otherwise the row count is determined by the
	// number of rows of arrow data.
	if ncols == 0 {
		r.nrows = 1
	} else {
		r.nrows = r.part.NumRows()
	}

	r.cols = make([]Column, len(r.meta))
	for i, m := range r.meta {
		c := newRelationColumn(m, pcols[i], r.nrows)
		r.cols[i] = c
	}

	return r
}

// Ensure the relation's type signature is instantiated.
func (r *baseRelation) ensureSignature() Signature {
	if r.sig != nil {
		return r.sig
	}
	r.sig = make([]any, len(r.meta))
	for i, t := range r.meta {
		r.sig[i] = relationType(t)
	}
	return r.sig
}

func newBaseRelation(p *Partition, meta Signature) Relation {
	return (&baseRelation{part: p, meta: meta}).init()
}

func (r *baseRelation) Metadata() Signature {
	return r.meta
}

func (r *baseRelation) Partition() *Partition {
	return r.part
}

func (r *baseRelation) GetItem(rnum int, out []any) {
	r.GetRow(rnum, out)
}

func (r *baseRelation) Item(rnum int) []any {
	return r.Row(rnum)
}

func (r *baseRelation) NumRows() int {
	return r.nrows
}

func (r *baseRelation) String(rnum int) string {
	return "(" + strings.Join(r.Strings(rnum), ", ") + ")"
}

func (r *baseRelation) Type() any {
	return AnyListType
}

func (r *baseRelation) Value(rnum int) any {
	return r.Row(rnum)
}

func (r *baseRelation) Column(cnum int) Column {
	return r.cols[cnum]
}

func (r *baseRelation) Columns() []Column {
	return r.cols
}

func (r *baseRelation) NumCols() int {
	return len(r.meta)
}

func (r *baseRelation) GetRow(rnum int, out []any) {
	for cnum := 0; cnum < len(r.cols); cnum++ {
		out[cnum] = r.cols[cnum].Value(rnum)
	}
}

func (r *baseRelation) Row(rnum int) []any {
	result := make([]any, len(r.cols))
	r.GetRow(rnum, result)
	return result
}

func (r *baseRelation) Signature() Signature {
	return r.ensureSignature()
}

func (r *baseRelation) Strings(rnum int) []string {
	ncols := len(r.cols)
	result := make([]string, ncols)
	for cnum := 0; cnum < ncols; cnum++ {
		result[cnum] = r.cols[cnum].String(rnum)
	}
	return result
}

// Answers if the given type describes a constant value.
func isConstType(t any) bool {
	switch t.(type) {
	case reflect.Type, ValueType:
		return false
	}
	return true
}

// Answers if the given type is a primitive type that may appear in a relation.
func isRelationPrimitive(t reflect.Type) bool {
	switch t {
	case BoolType:
		return true
	case Float16Type, Float32Type, Float64Type:
		return true
	case Int8Type, Int16Type, Int32Type, Int64Type:
		return true
	case Uint8Type, Uint16Type, Uint32Type, Uint64Type:
		return true
	case StringType:
		return true
	}
	return false
}

// Returns the type of the given builtin, nil if its not a builtin.
func builtinType(vt ValueType) reflect.Type {
	if matchPrefix(vt, "rel", "base", "_") {
		switch vt[2].(string) {
		case "AutoNumber":
			return Uint64Type
		case "Date":
			return TimeType
		case "DateTime":
			return TimeType
		case "FixedDecimal":
			return DecimalType
		case "FilePos":
			return Int64Type
		case "Hash":
			return BigIntType
		case "Missing":
			return MissingType
		case "Rational":
			return RationalType
		case "Year", "Month", "Week", "Day", "Hour", "Minute",
			"Second", "Millisecond", "Microsecond", "Nanosecond":
			return Int64Type
		}
	}
	return nil
}

func builtinValue(ct ConstType) any {
	if matchPrefix(ct, "rel", "base", "_") {
		switch ct[2].(string) {
		case "AutoNumber":
			return ct[3].(uint64)
		case "Date":
			return DateFromRataDie(ct[3].(int64))
		case "DateTime":
			return DateFromRataMillis(ct[3].(int64))
		case "FixedDecimal":
			return newConstDecimalValue(ct)
		case "FilePos":
			return ct[3].(int64)
		case "Hash":
			return ct[3].(*big.Int)
		case "Missing":
			return "missing"
		case "Rational":
			return newConstRationalValue(ct)
		case "Year", "Month", "Week", "Day", "Hour", "Minute",
			"Second", "Millisecond", "Microsecond", "Nanosecond":
			return ct[3].(int64)
		}
	}
	return nil
}

// Maps the given metadata element to the corresponding relation type.
func relationType(t any) any {
	switch tt := t.(type) {
	case reflect.Type: // primitive type
		switch tt {
		case CharType:
			return RuneType
		case Int128Type, Uint128Type:
			return BigIntType
		default:
			return tt
		}
	case ConstType:
		if bv := builtinValue(tt); bv != nil {
			return bv
		}
		result := make(ConstType, len(tt))
		for i, t := range tt {
			result[i] = relationType(t)
		}
		return result
	case ValueType:
		if bt := builtinType(tt); bt != nil {
			return bt
		}
		result := make(ValueType, len(tt))
		for i, t := range tt {
			result[i] = relationType(t)
		}
		return result
	default: // constant value
		return tt
	}
}

// Returns the relation column corresponding to the given base data column.
func newRelationColumn(t any, col Column, nrows int) Column {
	switch tt := t.(type) {
	case reflect.Type:
		switch tt {
		case CharType:
			return newCharColumn(col.(SimpleColumn[uint32]))
		case Int128Type:
			return newInt128Column(col.(TabularColumn[uint64]))
		case Uint128Type:
			return newUint128Column(col.(TabularColumn[uint64]))
		default:
			if isRelationPrimitive(tt) {
				return col // passed through
			}
			return newUnknownColumn(nrows)
		}
	case ConstType:
		return newConstColumn(tt, nrows)
	case ValueType:
		return newValueColumn(tt, col, nrows)
	case string:
		return newSymbolColumn(tt, nrows)
	default: // constant value other than string
		return newLiteralColumn(tt, nrows)
	}
}

func (r baseRelation) Slice(lo int, hi ...int) Relation {
	var c []Column
	var s Signature
	sig := r.Signature()
	if len(hi) > 0 {
		c = r.cols[lo:hi[0]]
		s = sig[lo:hi[0]]
	} else {
		c = r.cols[lo:]
		s = sig[lo:]
	}
	return newDerivedRelation(s, c)
}

// Represents a column of nil values, only appears when relations of different
// arity are unioned.
type nilColumn struct {
	nrows int
}

func newNilColumn(nrows int) DataColumn[any] {
	return nilColumn{nrows}
}

func (c nilColumn) GetItem(_ int, out *any) {
	*out = nil
}

func (c nilColumn) Item(_ int) any {
	return nil
}

func (c nilColumn) NumRows() int {
	return c.nrows
}

func (c nilColumn) String(_ int) string {
	return "<nil>"
}

func (c nilColumn) Type() any {
	return reflect.TypeOf(nil)
}

func (c nilColumn) Value(_ int) any {
	return nil
}

// Unions the  given columns into a single column.
type unionColumn struct {
	cols    []Column
	nrows   int
	colType any
}

func (c unionColumn) init() unionColumn {
	c.nrows = c.cols[0].NumRows()
	c.colType = c.cols[0].Type()
	for _, cc := range c.cols[1:] {
		c.nrows += cc.NumRows()
		if c.colType != cc.Type() {
			c.colType = MixedType
		}
	}
	return c
}

func newUnionColumn(cols []Column) DataColumn[any] {
	return (unionColumn{cols, -1, nil}).init()
}

func (c unionColumn) Item(rnum int) any {
	for _, cc := range c.cols {
		nrows := cc.NumRows()
		if rnum < nrows {
			return cc.Value(rnum)
		}
		rnum -= nrows
	}
	return nil // rnum out of range
}

func (c unionColumn) NumRows() int {
	return c.nrows
}

func (c unionColumn) String(rnum int) string {
	for _, cc := range c.cols {
		nrows := cc.NumRows()
		if rnum < nrows {
			return cc.String(rnum)
		}
		rnum -= nrows
	}
	return "" // rnum out of range
}

func (c unionColumn) Type() any {
	return c.colType
}

func (c unionColumn) Value(rnum int) any {
	return c.Item(rnum)
}

// Returns the maximum number of colums in the given list of relations.
func maxNumCols(rs []Relation) int {
	max := 0
	for _, r := range rs {
		ncols := r.NumCols()
		if max < ncols {
			max = ncols
		}
	}
	return max
}

// Note, unioning columns reduces the type to `any` because the columns may
// be heterogenous.
func makeUnionColumn(rels []Relation, cnum int) Column {
	cols := make([]Column, len(rels))
	for i, r := range rels {
		if cnum < r.NumCols() {
			cols[i] = r.Column(cnum)
		} else {
			cols[i] = newNilColumn(r.NumRows())
		}
	}
	return newUnionColumn(cols)
}

func newUnionRelation(rs []Relation) Relation {
	if len(rs) == 1 {
		return rs[0]
	}
	ncols := maxNumCols(rs)
	sig := make(Signature, ncols)
	cols := make([]Column, ncols)
	for cnum := 0; cnum < ncols; cnum++ {
		c := makeUnionColumn(rs, cnum)
		sig[cnum] = c.Type()
		cols[cnum] = c
	}
	return newDerivedRelation(sig, cols)
}

//
// derivedRealtion
//

type derivedRelation struct {
	sig  Signature
	cols []Column
}

func newDerivedRelation(sig Signature, cols []Column) Relation {
	return derivedRelation{sig, cols}
}

func (r derivedRelation) GetItem(rnum int, out []any) {
	r.GetRow(rnum, out)
}

func (r derivedRelation) Item(rnum int) []any {
	return r.Row(rnum)
}

func (r derivedRelation) NumRows() int {
	return r.cols[0].NumRows()
}

func (r derivedRelation) String(rnum int) string {
	return "(" + strings.Join(r.Strings(rnum), ", ") + ")"
}

func (r derivedRelation) Type() any {
	return AnyListType
}

func (r derivedRelation) Value(rnum int) any {
	return r.Row(rnum)
}

func (r derivedRelation) Column(cnum int) Column {
	return r.cols[cnum]
}

func (r derivedRelation) Columns() []Column {
	return r.cols
}

func (r derivedRelation) NumCols() int {
	return len(r.cols)
}

func (r derivedRelation) GetRow(rnum int, out []any) {
	for cnum, c := range r.cols {
		out[cnum] = c.Value(rnum)
	}
}

func (r derivedRelation) Row(rnum int) []any {
	result := make([]any, len(r.cols))
	r.GetRow(rnum, result)
	return result
}

func (r derivedRelation) Signature() Signature {
	return r.sig
}

func (r derivedRelation) Slice(lo int, hi ...int) Relation {
	var c []Column
	var s Signature
	if len(hi) > 0 {
		s = r.sig[lo:hi[0]]
		c = r.cols[lo:hi[0]]
	} else {
		s = r.sig[lo:]
		c = r.cols[lo:]
	}
	return newDerivedRelation(s, c)
}

func (r derivedRelation) Strings(rnum int) []string {
	ncols := len(r.cols)
	result := make([]string, ncols)
	for cnum := 0; cnum < ncols; cnum++ {
		result[cnum] = r.cols[cnum].String(rnum)
	}
	return result
}

//
// RelationCollection
//

type RelationCollection []Relation

// Select the relations matching the given signature prefix arguments. Match
// all if no arguments are given.
func (c RelationCollection) Select(args ...any) RelationCollection {
	if len(args) == 0 {
		return c
	}
	pre := Signature(args)
	rs := []Relation{}
	for _, r := range c {
		sig := r.Signature()
		if matchSig(pre, sig) {
			rs = append(rs, r)
		}
	}
	return RelationCollection(rs)
}

func (c RelationCollection) Union() Relation {
	return newUnionRelation(c)
}
