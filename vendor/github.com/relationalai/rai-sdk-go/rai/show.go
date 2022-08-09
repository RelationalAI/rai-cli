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
)

func makeIndent(indent int) string {
	result := make([]rune, indent)
	for i := 0; i < indent; i++ {
		result[i] = ' '
	}
	return string(result)
}

// Encode the given item as JSON to the given writer.
func Encode(w io.Writer, item interface{}, indent int) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", makeIndent(indent))
	return enc.Encode(item)
}

// Print the given item as JSON to stdout.
func Print(item interface{}, indent int) error {
	return Encode(os.Stdout, item, indent)
}

// Pretty printers for Relation and TransactionResult

func (r *Relation) Name() string {
	return r.RelKey.Name
}

func (r *Relation) GetRow(n int) []interface{} {
	result := make([]interface{}, len(r.Columns))
	for i, col := range r.Columns {
		result[i] = col[n]
	}
	return result
}

func (r *Relation) RowCount() int {
	if len(r.Columns) == 0 {
		return 0
	}
	return len(r.Columns[0])
}

func (r *Relation) Schema() string {
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

type Showable interface {
	Show()
}

func (r *Relation) Show() {
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
