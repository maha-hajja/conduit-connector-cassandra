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
	"context"
	"fmt"
	"strings"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/gocql/gocql"
)

type Destination struct {
	sdk.UnimplementedDestination

	config  DestinationConfig
	session *gocql.Session
}

const (
	metadataCassandraTable = "cassandra.table"

	insertQuery = "INSERT INTO %s (%s) VALUES (%s)"
	updateQuery = "UPDATE %s SET %s WHERE %s"
	deleteQuery = "DELETE FROM %s WHERE %s"

	setStatementSeparator   = ","
	whereStatementSeparator = "AND"
)

func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{})
}

func (d *Destination) Parameters() map[string]sdk.Parameter {
	return d.config.Parameters()
}

func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	sdk.Logger(ctx).Info().Msg("Configuring Destination...")
	err := sdk.Util.ParseConfig(cfg, &d.config)
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	err = d.config.validateConfig()
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	return nil
}

func (d *Destination) Open(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Opening the connector.")
	// Define the Cassandra cluster configuration
	clusterConfig := gocql.NewCluster(d.config.Host)
	clusterConfig.Keyspace = d.config.Keyspace
	clusterConfig.Port = d.config.Port

	if d.config.AuthMechanism == AuthMechanismBasic {
		clusterConfig.Authenticator = gocql.PasswordAuthenticator{
			Username: d.config.AuthUsername,
			Password: d.config.AuthPassword,
		}
	}

	// Connect to the Cassandra cluster
	session, err := clusterConfig.CreateSession()
	if err != nil {
		return fmt.Errorf("error connecting to the cassandra cluster: %w", err)
	}
	d.session = session
	return nil
}

func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	for i, r := range records {
		err := d.validateStructuredRecord(r)
		if err != nil {
			return i, fmt.Errorf("invalid record format: %w", err)
		}

		err = sdk.Util.Destination.Route(ctx, r,
			d.handleInsert, // create
			d.handleUpdate, // update
			d.handleDelete, // delete
			d.handleInsert, // snapshot
		)
		if err != nil {
			return i, err
		}
	}
	sdk.Logger(ctx).Trace().Msgf("%v records written to destination", len(records))
	return len(records), nil
}

func (d *Destination) Teardown(ctx context.Context) error {
	if d.session != nil {
		d.session.Close()
	}
	return nil
}

// handleInsert create and execute the cql query to insert a row.
func (d *Destination) handleInsert(ctx context.Context, record sdk.Record) error {
	query := d.buildInsertQuery(record)
	err := query.Exec()
	if err != nil {
		return fmt.Errorf("error while inserting data: %w", err)
	}

	return nil
}

// handleUpdate create and execute the cql query to update a row.
func (d *Destination) handleUpdate(ctx context.Context, record sdk.Record) error {
	query := d.buildUpdateQuery(record)
	err := query.Exec()
	if err != nil {
		return fmt.Errorf("error while updating data: %w", err)
	}

	return nil
}

// handleDelete create and execute the cql query to delete a row.
func (d *Destination) handleDelete(ctx context.Context, record sdk.Record) error {
	query := d.buildDeleteQuery(record)
	err := query.Exec()
	if err != nil {
		return fmt.Errorf("error while deleting data: %w", err)
	}

	return nil
}

// getColumnsAndValues returns the key columns and values, and the payload columns and values, each in a slice and in the order mentioned.
func (d *Destination) getColumnsAndValues(key, payload sdk.StructuredData) ([]string, []interface{}, []string, []interface{}) {
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

// buildInsertQuery takes a record, and returns the insert query representing that record.
func (d *Destination) buildInsertQuery(rec sdk.Record) *gocql.Query {
	table := d.getTableName(rec.Metadata)
	keyCols, keyVals, cols, vals := d.getColumnsAndValues(rec.Key.(sdk.StructuredData), rec.Payload.After.(sdk.StructuredData))
	cols = append(cols, keyCols...)
	vals = append(vals, keyVals...)
	query := fmt.Sprintf(insertQuery, table, strings.Join(cols, ", "), d.getPlaceholders(len(cols)))
	return d.session.Query(query, vals...)
}

// buildUpdateQuery takes a record, and returns the update query representing that record.
func (d *Destination) buildUpdateQuery(rec sdk.Record) *gocql.Query {
	table := d.getTableName(rec.Metadata)
	keyCols, keyVals, cols, vals := d.getColumnsAndValues(rec.Key.(sdk.StructuredData), rec.Payload.After.(sdk.StructuredData))
	setStatement := d.pairValuesWithPlaceholder(cols, setStatementSeparator)
	whereStatement := d.pairValuesWithPlaceholder(keyCols, whereStatementSeparator)
	vals = append(vals, keyVals...)
	query := fmt.Sprintf(updateQuery, table, setStatement, whereStatement)
	return d.session.Query(query, vals...)
}

// buildDeleteQuery takes a record, and returns the delete query representing that record.
func (d *Destination) buildDeleteQuery(rec sdk.Record) *gocql.Query {
	table := d.getTableName(rec.Metadata)
	keyCols, keyVals, _, _ := d.getColumnsAndValues(rec.Key.(sdk.StructuredData), rec.Payload.After.(sdk.StructuredData))
	whereStatement := d.pairValuesWithPlaceholder(keyCols, whereStatementSeparator)
	query := fmt.Sprintf(deleteQuery, table, whereStatement)
	return d.session.Query(query, keyVals...)
}

// getPlaceholders returns a string of question marks seperated by a comma with a given length.
func (d *Destination) getPlaceholders(length int) string {
	return strings.TrimSuffix(strings.Repeat("?, ", length), ", ")
}

// getTableName returns the table name from the record metadata, or if that doesn't exist, then it returns the table
// name from the connector configurations.
func (d *Destination) getTableName(metadata map[string]string) string {
	tableName, ok := metadata[metadataCassandraTable]
	if !ok {
		return d.config.Table
	}
	return tableName
}

func (d *Destination) pairValuesWithPlaceholder(cols []string, separator string) string {
	var result string
	for i := 0; i < len(cols); i++ {
		result += cols[i] + " = ? " + separator + " "
	}

	// remove the trailing separator from the resulting string
	result = strings.TrimSuffix(result, separator+" ")

	return result
}

// validateStructuredRecord return an error if the record key or payload is not structured.
func (d *Destination) validateStructuredRecord(record sdk.Record) error {
	// check that payload is structured
	if _, ok := record.Payload.After.(sdk.StructuredData); !ok {
		return fmt.Errorf("payload should be structured data")
	}
	// check that key is structured
	if _, ok := record.Payload.After.(sdk.StructuredData); !ok {
		return fmt.Errorf("key should be structured data")
	}
	return nil
}
