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
	"strings"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	insertQuery = "INSERT INTO %s (%s) VALUES (%s)"
	updateQuery = "UPDATE %s SET %s WHERE %s"
	deleteQuery = "DELETE FROM %s WHERE %s"

	setStatementSeparator   = ","
	whereStatementSeparator = "AND"
)

// QueryBuilder builds a CQL query statement and its values from a record.
type QueryBuilder struct{}

// BuildInsertQuery takes a record, and returns the insert query statement and values representing that record.
func (q *QueryBuilder) BuildInsertQuery(rec sdk.Record, table string) (string, []interface{}) {
	keyCols, keyVals, cols, vals := q.getColumnsAndValues(rec.Key.(sdk.StructuredData), rec.Payload.After.(sdk.StructuredData))
	cols = append(cols, keyCols...)
	vals = append(vals, keyVals...)
	query := fmt.Sprintf(insertQuery, table, strings.Join(cols, ", "), q.getPlaceholders(len(cols)))
	return query, vals
}

// BuildUpdateQuery takes a record, and returns the update query statement and values representing that record.
func (q *QueryBuilder) BuildUpdateQuery(rec sdk.Record, table string) (string, []interface{}) {
	keyCols, keyVals, cols, vals := q.getColumnsAndValues(rec.Key.(sdk.StructuredData), rec.Payload.After.(sdk.StructuredData))
	setStatement := q.pairValuesWithPlaceholder(cols, setStatementSeparator)
	whereStatement := q.pairValuesWithPlaceholder(keyCols, whereStatementSeparator)
	vals = append(vals, keyVals...)
	query := fmt.Sprintf(updateQuery, table, setStatement, whereStatement)
	return query, vals
}

// BuildDeleteQuery takes a record, and returns the delete query statement and values representing that record.
func (q *QueryBuilder) BuildDeleteQuery(rec sdk.Record, table string) (string, []interface{}) {
	keyCols, keyVals, _, _ := q.getColumnsAndValues(rec.Key.(sdk.StructuredData), rec.Payload.After.(sdk.StructuredData))
	whereStatement := q.pairValuesWithPlaceholder(keyCols, whereStatementSeparator)
	query := fmt.Sprintf(deleteQuery, table, whereStatement)
	return query, keyVals
}

// getPlaceholders returns a string of question marks seperated by a comma with a given length.
func (q *QueryBuilder) getPlaceholders(length int) string {
	return strings.TrimSuffix(strings.Repeat("?, ", length), ", ")
}

func (q *QueryBuilder) pairValuesWithPlaceholder(cols []string, separator string) string {
	if len(cols) == 0 {
		return ""
	}
	return strings.Join(cols, " = ? "+separator+" ") + " = ?"
}

// getColumnsAndValues returns the key columns and values, and the payload columns and values, each in a slice and in the order mentioned.
func (q *QueryBuilder) getColumnsAndValues(key, payload sdk.StructuredData) ([]string, []interface{}, []string, []interface{}) {
	keyColumns := make([]string, 0, len(key))
	keyValues := make([]interface{}, 0, len(key))
	columns := make([]string, 0, len(payload))
	values := make([]interface{}, 0, len(payload))

	// range over both the key and payload values
	for k, v := range key {
		keyColumns = append(keyColumns, k)
		keyValues = append(keyValues, v)
	}

	// adding commas at the beginning and end of string, to prevent skipping payload fields that have a key as substring.
	keyColumnsString := "," + strings.Join(keyColumns, ",") + ","
	for k, v := range payload {
		// skip Key from payload if exists
		if strings.Contains(keyColumnsString, ","+k+",") {
			continue
		}
		columns = append(columns, k)
		values = append(values, v)
	}

	return keyColumns, keyValues, columns, values
}
