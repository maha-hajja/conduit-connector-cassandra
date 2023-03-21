// Copyright © 2023 Meroxa, Inc.
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

package cassandra

import (
	"fmt"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestParser_Placeholders(t *testing.T) {
	is := is.New(t)
	parser := Parser{}
	len1 := parser.getPlaceholders(1)
	is.Equal(len1, "?")
	len3 := parser.getPlaceholders(3)
	is.Equal(len3, "?, ?, ?")

	vals := []string{"one", "two", "three"}
	out := parser.pairValuesWithPlaceholder(vals, "AND")
	is.Equal(out, "one = ? AND two = ? AND three = ?")
	out2 := parser.pairValuesWithPlaceholder(vals, ",")
	is.Equal(out2, "one = ? , two = ? , three = ?")
}

func TestParser_Insert(t *testing.T) {
	is := is.New(t)
	parser := Parser{}
	rec := sdk.Record{
		Key: sdk.StructuredData{"id": "6"},
		Payload: sdk.Change{
			After: sdk.StructuredData{
				"age": 22,
			},
		},
	}
	cql, vals := parser.BuildInsertQuery(rec, "my_table")
	is.Equal(cql, "INSERT INTO my_table (age, id) VALUES (?, ?)")
	is.Equal(vals, []interface{}{22, "6"})
}

func TestParser_Update(t *testing.T) {
	is := is.New(t)
	parser := Parser{}
	rec := sdk.Record{
		Key: sdk.StructuredData{"id": "6"},
		Payload: sdk.Change{
			After: sdk.StructuredData{
				"age": 33,
			},
		},
	}
	cql, vals := parser.BuildUpdateQuery(rec, "my_table")
	fmt.Println(cql)
	is.Equal(cql, "UPDATE my_table SET age = ? WHERE id = ?")
	is.Equal(vals, []interface{}{33, "6"})
}

func TestParser_Delete(t *testing.T) {
	is := is.New(t)
	parser := Parser{}
	rec := sdk.Record{
		Key: sdk.StructuredData{"id": "6", "id2": "6"},
		Payload: sdk.Change{
			After: sdk.StructuredData{},
		},
	}
	cql, vals := parser.BuildDeleteQuery(rec, "my_table")
	// key is a map, so we don't guarantee the order
	is.True(cql == "DELETE FROM my_table WHERE id = ? AND id2 = ?" || cql == "DELETE FROM my_table WHERE id2 = ? AND id = ?")
	is.Equal(vals, []interface{}{"6", "6"})
}
