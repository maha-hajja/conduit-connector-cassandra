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

// Parser parses a record into a CQL query statement and its values
type Parser struct{}

// BuildInsertQuery takes a record, and returns the insert query statement and values representing that record.
func (p *Parser) BuildInsertQuery(rec sdk.Record, table string) (string, []interface{}) {
	keyCols, keyVals, cols, vals := p.getColumnsAndValues(rec.Key.(sdk.StructuredData), rec.Payload.After.(sdk.StructuredData))
	cols = append(cols, keyCols...)
	vals = append(vals, keyVals...)
	query := fmt.Sprintf(insertQuery, table, strings.Join(cols, ", "), p.getPlaceholders(len(cols)))
	return query, vals
}

// BuildUpdateQuery takes a record, and returns the update query statement and values representing that record.
func (p *Parser) BuildUpdateQuery(rec sdk.Record, table string) (string, []interface{}) {
	keyCols, keyVals, cols, vals := p.getColumnsAndValues(rec.Key.(sdk.StructuredData), rec.Payload.After.(sdk.StructuredData))
	setStatement := p.pairValuesWithPlaceholder(cols, setStatementSeparator)
	whereStatement := p.pairValuesWithPlaceholder(keyCols, whereStatementSeparator)
	vals = append(vals, keyVals...)
	query := fmt.Sprintf(updateQuery, table, setStatement, whereStatement)
	return query, vals
}

// BuildDeleteQuery takes a record, and returns the delete query statement and values representing that record.
func (p *Parser) BuildDeleteQuery(rec sdk.Record, table string) (string, []interface{}) {
	keyCols, keyVals, _, _ := p.getColumnsAndValues(rec.Key.(sdk.StructuredData), rec.Payload.After.(sdk.StructuredData))
	whereStatement := p.pairValuesWithPlaceholder(keyCols, whereStatementSeparator)
	query := fmt.Sprintf(deleteQuery, table, whereStatement)
	return query, keyVals
}

// getPlaceholders returns a string of question marks seperated by a comma with a given length.
func (p *Parser) getPlaceholders(length int) string {
	return strings.TrimSuffix(strings.Repeat("?, ", length), ", ")
}

func (p *Parser) pairValuesWithPlaceholder(cols []string, separator string) string {
	var result string
	for i := 0; i < len(cols); i++ {
		result += cols[i] + " = ? " + separator + " "
	}

	// remove the trailing separator from the resulting string
	result = strings.TrimSuffix(result, " "+separator+" ")

	return result
}

// getColumnsAndValues returns the key columns and values, and the payload columns and values, each in a slice and in the order mentioned.
func (p *Parser) getColumnsAndValues(key, payload sdk.StructuredData) ([]string, []interface{}, []string, []interface{}) {
	var keyColumns []string
	var keyValues []interface{}
	var columns []string
	var values []interface{}

	// range over both the key and payload values
	for k, v := range key {
		keyColumns = append(keyColumns, k)
		keyValues = append(keyValues, v)
		delete(payload, k) // delete Key from payload if exists
	}

	for k, v := range payload {
		columns = append(columns, k)
		values = append(values, v)
	}

	return keyColumns, keyValues, columns, values
}
