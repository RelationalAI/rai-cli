// Copyright 2022 RelationalAI, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rai

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v7/arrow/float16"
	"github.com/pkg/errors"
	"github.com/relationalai/rai-sdk-go/rai/pb"
)

func makeIndent(indent int) string {
	result := make([]rune, indent)
	for i := 0; i < indent; i++ {
		result[i] = ' '
	}
	return string(result)
}

func print(level int, format string, args ...any) {
	for i := 0; i < level; i++ {
		fmt.Print("    ")
	}
	fmt.Printf(format, args...)
}

// Encode the given item as JSON to the given writer.
func Encode(w io.Writer, item interface{}, indent int) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", makeIndent(indent))
	return enc.Encode(item)
}

// Print the given item as JSON to stdout.
func ShowJSON(item interface{}, indent int) error {
	return Encode(os.Stdout, item, indent)
}

// Deprecated: Use `ShowJSON` instead.
func Print(item interface{}, indent int) error {
	return Encode(os.Stdout, item, indent)
}

type Showable interface {
	Show()
}

// Pretty printers for RelationV1 and TransactionResult

func (r *RelationV1) Name() string {
	return r.RelKey.Name
}

func (r *RelationV1) GetRow(n int) []interface{} {
	result := make([]interface{}, len(r.Columns))
	for i, col := range r.Columns {
		result[i] = col[n]
	}
	return result
}

func (r *RelationV1) RowCount() int {
	if len(r.Columns) == 0 {
		return 0
	}
	return len(r.Columns[0])
}

func (r *RelationV1) Schema() string {
	rkey := &r.RelKey
	schema := []string{}
	schema = append(schema, rkey.Keys...)
	schema = append(schema, rkey.Values...)
	return strings.Join(schema, "*")
}

// Returns a "showable" string for the given value.
func displayString(v interface{}) string {
	switch vv := v.(type) {
	case bool:
		return strconv.FormatBool(vv)
	case int:
		return strconv.Itoa(vv)
	case string:
		if vv[0] == ':' {
			return vv // symbol
		}
		return fmt.Sprintf("\"%s\"", vv)
	}
	return fmt.Sprintf("%v", v)
}

func showRow(row []interface{}) {
	for i, item := range row {
		if i > 0 {
			fmt.Print(", ")
		}
		fmt.Print(displayString(item))
	}
	fmt.Println()
}

func (r *RelationV1) Show() {
	fmt.Printf("# %s (%s)\n", r.Name(), r.Schema())
	for i := 0; i < r.RowCount(); i++ {
		row := r.GetRow(i)
		showRow(row)
	}
}

func (tx *TransactionResult) Show() {
	for i, r := range tx.Output {
		if i > 0 {
			fmt.Println()
		}
		r.Show()
	}
	if len(tx.Problems) > 0 {
		fmt.Println()
		for _, p := range tx.Problems {
			fmt.Printf("Error (%s): %s\n", p.ErrorCode, p.Message)
			if p.Report != "" {
				fmt.Println(p.Report)
			}
		}
	}
}

func showConstantType(level int, ct *pb.ConstantType) {
	switch ct.RelType.Tag {
	case pb.Kind_PRIMITIVE_TYPE:
		print(level, "PRIMITIVE_TYPE\n")
		showRelTuple(level+1, ct.Value)
	case pb.Kind_VALUE_TYPE:
		print(level, "VALUE_TYPE\n")
		showValueType(level+1, ct.RelType.ValueType)
		showRelTuple(level+1, ct.Value)
	default:
		print(level, "UNKNOWN\n")
	}
}

func primValueString(v *pb.PrimitiveValue) string {
	switch vv := v.GetValue().(type) {
	case *pb.PrimitiveValue_Int128Val:
		return fmt.Sprintf("%v", vv.Int128Val)
	case *pb.PrimitiveValue_Int64Val:
		return fmt.Sprintf("%d", vv.Int64Val)
	case *pb.PrimitiveValue_Int32Val:
		return fmt.Sprintf("%d", vv.Int32Val)
	case *pb.PrimitiveValue_Int16Val:
		return fmt.Sprintf("%d", int16(vv.Int16Val))
	case *pb.PrimitiveValue_Int8Val:
		return fmt.Sprintf("%d", int8(vv.Int8Val))
	case *pb.PrimitiveValue_Uint128Val:
		return fmt.Sprintf("%v", vv.Uint128Val)
	case *pb.PrimitiveValue_Uint64Val:
		return fmt.Sprintf("%d", vv.Uint64Val)
	case *pb.PrimitiveValue_Uint32Val:
		return fmt.Sprintf("%d", vv.Uint32Val)
	case *pb.PrimitiveValue_Uint16Val:
		return fmt.Sprintf("%d", uint16(vv.Uint16Val))
	case *pb.PrimitiveValue_Uint8Val:
		return fmt.Sprintf("%d", uint8(vv.Uint8Val))
	case *pb.PrimitiveValue_Float64Val:
		return fmt.Sprintf("%f", vv.Float64Val)
	case *pb.PrimitiveValue_Float32Val:
		return fmt.Sprintf("%f", vv.Float32Val)
	case *pb.PrimitiveValue_Float16Val:
		return float16.New(vv.Float16Val).String()
	case *pb.PrimitiveValue_CharVal:
		return fmt.Sprintf("'%c'", rune(vv.CharVal))
	case *pb.PrimitiveValue_BoolVal:
		return fmt.Sprintf("%v", vv.BoolVal)
	case *pb.PrimitiveValue_StringVal:
		return fmt.Sprintf("\"%s\"", string(vv.StringVal))
	}
	return "UNKNOWN"
}

func showPrimitiveType(level int, pt pb.PrimitiveType) {
	print(level, "%s\n", pt.String())
}

func showMetadataArgs(level int, args []*pb.RelType) {
	for _, rt := range args {
		showRelType(level, rt)
	}
}

func showValueType(level int, vt *pb.ValueType) {
	showMetadataArgs(level, vt.ArgumentTypes)
}

func showRelTuple(level int, rt *pb.RelTuple) {
	args := make([]string, len(rt.Arguments))
	for i, arg := range rt.Arguments {
		args[i] = primValueString(arg)
	}
	switch len(args) {
	case 0:
		print(level, "()\n")
	case 1:
		print(level, "%s\n", args[0])
	default:
		print(level, "(%s)\n", strings.Join(args, ", "))
	}
}

func showRelType(level int, rt *pb.RelType) {
	switch rt.Tag {
	case pb.Kind_PRIMITIVE_TYPE:
		showPrimitiveType(level, rt.PrimitiveType)
	case pb.Kind_CONSTANT_TYPE:
		print(level, "CONSTANT_TYPE\n")
		showConstantType(level+1, rt.ConstantType)
	case pb.Kind_VALUE_TYPE:
		print(level, "VALUE_TYPE\n")
		showValueType(level+1, rt.ValueType)
	default:
		print(level, "UNKNOWN\n")
	}
}

// Show protobuf metadata.
func ShowMetadata(m *pb.MetadataInfo) {
	for _, rm := range m.Relations {
		print(0, "%s\n", rm.FileName)
		showMetadataArgs(0, rm.RelationId.Arguments)
	}
}

// Show a tabular data value.
func ShowTabularData(d Tabular) {
	for rnum := 0; rnum < d.NumRows(); rnum++ {
		if rnum > 0 {
			fmt.Println(";")
		}
		fmt.Print(strings.Join(d.Strings(rnum), ", "))
	}
	fmt.Println()
}

func ShowRelation(r Relation) {
	sig := r.Signature()
	fmt.Printf("// %s\n", strings.Join(sig.Strings(), ", "))
	ShowTabularData(r)
}

func (r *baseRelation) Show() {
	ShowRelation(r)
}

func (r derivedRelation) Show() {
	ShowRelation(r)
}

func (rc RelationCollection) Show() {
	for i, r := range rc {
		if i > 0 {
			fmt.Println()
		}
		r.Show()
	}
}

func (rsp *TransactionResponse) Show() {
	if err := ShowJSON(&rsp.Transaction, 4); err != nil {
		fmt.Println(errors.Wrapf(err, "failed to show transaction"))
		return
	}
	if rsp.Metadata == nil {
		return
	}
	rc := rsp.Relations("output")
	if len(rc) > 0 {
		fmt.Println()
		rc.Show()
	}
	rc = rsp.Relations("rel", "catalog", "diagnostic")
	if len(rc) > 0 {
		fmt.Printf("\nProblems:\n")
		ShowTabularData(rc.Union())
	}
}
