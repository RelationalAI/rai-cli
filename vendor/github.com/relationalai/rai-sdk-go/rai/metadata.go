// Copyright 2022 RelationalAI, Inc.

package rai

// Support for transforming the protobuf representation of relation metadata
// to its native golang representation. Primitive types are represented as
// golang reflect.Type values, and const and value type structures are
// simplified to make the representation easier to navigate.

// There are three kinds of signatures used to describe transaction results.
//
//   * Metadata signature, derived from protobuf metadata, describes how
//     to interpret partition data in order to construct the corresponding
//     relation. This signature can be a mixture of types, literal values and
//     const and value type compositors, and those compositors may also contain
//     types, literal values and nested compositors.
//
//   * Partition signature, this describes the physical partition data. This
//     signature will not contain any specialized values.
//
//   * Relation signature, this describes the actual relation which is
//     constructed by interpreting a partition according to it's metadata
//     description. This signature will only contain types and const & value
//     type compositors, which themselves may contain types and nested
//     compositors.

import (
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"time"

	"github.com/apache/arrow/go/v7/arrow/array"
	"github.com/apache/arrow/go/v7/arrow/float16"
	"github.com/relationalai/rai-sdk-go/rai/pb"
	"github.com/shopspring/decimal"
)

func (s Signature) String() string {
	return "(" + strings.Join(s.Strings(), ", ") + ")"
}

func (s Signature) Strings() []string {
	return asTypeStrings(s)
}

type ConstType Signature

func (t ConstType) String() string {
	return "const[" + strings.Join(t.Strings(), ", ") + "]"
}

func (t ConstType) Strings() []string {
	return asTypeStrings(t)
}

type ValueType Signature

func (t ValueType) String() string {
	return "value[" + strings.Join(t.Strings(), ", ") + "]"
}

func (t ValueType) Strings() []string {
	return asTypeStrings(t)
}

// Returns a Rel-ish string representation of the given type.
func asTypeString(v any) string {
	switch vv := v.(type) {
	case reflect.Type: // primitive type
		return fmt.Sprintf("%v", vv)
	case ConstType:
		return vv.String()
	case ValueType:
		return vv.String()
	case time.Time:
		return vv.Format(time.RFC3339)
	case string:
		return fmt.Sprintf("\"%s\"", vv)
	default:
		return fmt.Sprintf("%v", vv)
	}
}

// Returns a list of type strings corresponding to the given list of types.
func asTypeStrings(v []any) []string {
	result := make([]string, len(v))
	for i, item := range v {
		result[i] = asTypeString(item)
	}
	return result
}

//
// Type values
//

func typeOf[T any]() reflect.Type {
	return reflect.TypeOf(*new(T))
}

type Char uint32
type Int128 [2]uint64
type Uint128 [2]uint64
type Missing struct{}
type Mixed struct{}
type Unknown struct{}
type Unspecified struct{}

var UnknownType = typeOf[Unknown]()

// Partition column types (many also appear in relations)
var (
	// Simple types
	BoolType        = typeOf[bool]()
	CharType        = typeOf[Char]()
	Float16Type     = typeOf[float16.Num]()
	Float64Type     = typeOf[float64]()
	Float32Type     = typeOf[float32]()
	Int128Type      = typeOf[Int128]()
	Int64Type       = typeOf[int64]()
	Int32Type       = typeOf[int32]()
	Int16Type       = typeOf[int16]()
	Int8Type        = typeOf[int8]()
	StringType      = typeOf[string]()
	Uint128Type     = typeOf[Uint128]()
	Uint64Type      = typeOf[uint64]()
	Uint32Type      = typeOf[uint32]()
	Uint16Type      = typeOf[uint16]()
	Uint8Type       = typeOf[uint8]()
	UnspecifiedType = typeOf[Unspecified]()

	// Composite types
	AnyListType     = typeOf[[]any]()
	Float32ListType = typeOf[[]float32]()
	Float64ListType = typeOf[[]float64]()
	Int8ListType    = typeOf[[]int8]()
	Int16ListType   = typeOf[[]int16]()
	Int32ListType   = typeOf[[]int32]()
	Int64ListType   = typeOf[[]int64]()
	Uint64ListType  = typeOf[[]uint64]()
	StructType      = typeOf[array.Struct]()
)

// Relation specific column types
var (
	AnyType      = typeOf[any]() // heterogenous, tabular column
	BigIntType   = typeOf[*big.Int]()
	TimeType     = typeOf[time.Time]()
	DecimalType  = typeOf[decimal.Decimal]()
	RationalType = typeOf[*big.Rat]()
	RuneType     = typeOf[rune]()
	MissingType  = typeOf[Missing]()
	MixedType    = typeOf[Mixed]()
)

// Returns the native type corresponding to the given Rel primitive type code.
func asNativePrimitiveType(p pb.PrimitiveType) reflect.Type {
	switch p {
	case pb.PrimitiveType_BOOL:
		return BoolType
	case pb.PrimitiveType_CHAR:
		return CharType
	case pb.PrimitiveType_FLOAT_16:
		return Float16Type
	case pb.PrimitiveType_FLOAT_32:
		return Float32Type
	case pb.PrimitiveType_FLOAT_64:
		return Float64Type
	case pb.PrimitiveType_INT_8:
		return Int8Type
	case pb.PrimitiveType_INT_16:
		return Int16Type
	case pb.PrimitiveType_INT_32:
		return Int32Type
	case pb.PrimitiveType_INT_64:
		return Int64Type
	case pb.PrimitiveType_INT_128:
		return Int128Type
	case pb.PrimitiveType_UINT_8:
		return Uint8Type
	case pb.PrimitiveType_UINT_16:
		return Uint16Type
	case pb.PrimitiveType_UINT_32:
		return Uint32Type
	case pb.PrimitiveType_UINT_64:
		return Uint64Type
	case pb.PrimitiveType_UINT_128:
		return Uint128Type
	case pb.PrimitiveType_STRING:
		return StringType
	case pb.PrimitiveType_UNSPECIFIED_TYPE:
		return UnspecifiedType
	}
	return UnknownType
}

// Returns the primitive value corresponding to the given ProtoBuf
// representation of a primitive value.
func asNativeConstValue(pv *pb.PrimitiveValue) any {
	switch vv := pv.GetValue().(type) {
	case *pb.PrimitiveValue_Int128Val:
		hi := vv.Int128Val.Highbits
		lo := vv.Int128Val.Lowbits
		return NewBigInt128(lo, hi)
	case *pb.PrimitiveValue_Int64Val:
		return vv.Int64Val
	case *pb.PrimitiveValue_Int32Val:
		return vv.Int32Val
	case *pb.PrimitiveValue_Int16Val:
		return int16(vv.Int16Val)
	case *pb.PrimitiveValue_Int8Val:
		return int8(vv.Int8Val)
	case *pb.PrimitiveValue_Uint128Val:
		hi := vv.Uint128Val.Highbits
		lo := vv.Uint128Val.Lowbits
		return NewBigUint128(lo, hi)
	case *pb.PrimitiveValue_Uint64Val:
		return vv.Uint64Val
	case *pb.PrimitiveValue_Uint32Val:
		return vv.Uint32Val
	case *pb.PrimitiveValue_Uint16Val:
		return uint16(vv.Uint16Val)
	case *pb.PrimitiveValue_Uint8Val:
		return uint8(vv.Uint8Val)
	case *pb.PrimitiveValue_Float64Val:
		return vv.Float64Val
	case *pb.PrimitiveValue_Float32Val:
		return vv.Float32Val
	case *pb.PrimitiveValue_Float16Val:
		return float16.New(vv.Float16Val)
	case *pb.PrimitiveValue_CharVal:
		return rune(vv.CharVal)
	case *pb.PrimitiveValue_BoolVal:
		return vv.BoolVal
	case *pb.PrimitiveValue_StringVal:
		return string(vv.StringVal)
	}
	return UnknownType
}

// Returns the native representation of a constant type.
func asNativeConstType(c *pb.ConstantType) any {
	switch c.RelType.Tag {
	case pb.Kind_PRIMITIVE_TYPE:
		return asNativeConstPrimitiveType(c)
	case pb.Kind_VALUE_TYPE:
		return asNativeConstValueType(c)
	}
	return UnknownType
}

// Return the native representation of a constant primitive value.
func asNativeConstPrimitiveType(c *pb.ConstantType) any {
	// assert pb.RelType.Tag == pb.Kind_PRIMITIVE_TYPE
	args := c.Value.Arguments
	switch len(args) {
	case 0:
		return UnknownType
	case 1:
		return asNativeConstValue(args[0])
	}
	result := make([]any, len(args))
	for i, v := range args {
		result[i] = asNativeConstValue(v)
	}
	return result
}

// Constant value types are represented in protobuf as a (potentially nested)
// list of constant values and types, and an additional list of constant value
// "arguments". This routine maps the value arguments to their corresponding
// type position in the type list, and returns the merged list plus any
// remaining unused arguments.
func mergeConstValueArgs(sig ConstType, arg any) (ConstType, []any) {
	var args []any
	switch val := arg.(type) {
	case []any:
		args = val
	default:
		args = []any{val}
	}
	// Replace types with the corresponding constant arg
	for i, v := range sig {
		switch tt := v.(type) {
		case reflect.Type:
			v, args = args[0], args[1:]
		case ConstType:
			v, args = mergeConstValueArgs(ConstType(tt), args)
		case ValueType:
			// this should have been converted to a const by caller
			v, args = UnknownType, args[1:]
		}
		sig[i] = v
	}
	return sig, args
}

// Returns the native representation of a constant value type.
func asNativeConstValueType(c *pb.ConstantType) ConstType {
	t := c.RelType.ValueType.ArgumentTypes
	sig := make(ConstType, len(t))
	for i, rt := range t {
		var v any
		switch rt.Tag {
		case pb.Kind_CONSTANT_TYPE:
			v = asNativeConstType(rt.ConstantType)
		case pb.Kind_PRIMITIVE_TYPE:
			v = asNativePrimitiveType(rt.PrimitiveType)
		case pb.Kind_VALUE_TYPE:
			// a value type under a const is interpreted as a const
			v = ConstType(asNativeValueType(rt.ValueType))
		default:
			v = UnknownType
		}
		sig[i] = v
	}
	arg := asNativeConstPrimitiveType(c)
	sig, _ = mergeConstValueArgs(sig, arg)
	return sig
}

// Returns the native golang representation of a value type.
func asNativeValueType(vt *pb.ValueType) ValueType {
	return ValueType(asSignature(vt.ArgumentTypes))
}

// Return the native representation correspondingn to the given protobuf RelType.
func asNativeType(rt *pb.RelType) any {
	switch rt.Tag {
	case pb.Kind_PRIMITIVE_TYPE:
		return asNativePrimitiveType(rt.PrimitiveType)
	case pb.Kind_CONSTANT_TYPE:
		return asNativeConstType(rt.ConstantType)
	case pb.Kind_VALUE_TYPE:
		return asNativeValueType(rt.ValueType)
	}
	return UnknownType
}

// Returns the native type signature corresponding to the given protobuf metadata.
func asSignature(args []*pb.RelType) Signature {
	sig := make([]any, len(args))
	for i, arg := range args {
		sig[i] = asNativeType(arg)
	}
	return sig
}

// Returns a mapping from partition id to metadata signature
func asSignatureMap(m *pb.MetadataInfo) map[string]Signature {
	result := map[string]Signature{}
	for _, rm := range m.Relations {
		result[rm.FileName] = asSignature(rm.RelationId.Arguments)
	}
	return result
}

// Returns the metadata signature corresponding to the given `id`, and nil if
// the requested `id` does not exist.
func (m TransactionMetadata) Signature(id string) Signature {
	s, ok := m.sigMap[id]
	if !ok {
		return nil
	}
	return s
}

// Returns a mapping from relation `id` to metadata signature.
func (m TransactionMetadata) Signatures() map[string]Signature {
	return m.sigMap
}
