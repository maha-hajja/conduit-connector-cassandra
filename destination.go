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

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/gocql/gocql"
)

type Destination struct {
	sdk.UnimplementedDestination

	config       DestinationConfig
	session      *gocql.Session
	queryBuilder QueryBuilder
}

const metadataCassandraTable = "cassandra.table"

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
	clusterConfig := gocql.NewCluster(d.config.Nodes...)
	clusterConfig.Keyspace = d.config.Keyspace

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

func (d *Destination) Teardown(context.Context) error {
	if d.session != nil {
		d.session.Close()
	}
	return nil
}

// handleInsert create and execute the cql query to insert a row.
func (d *Destination) handleInsert(_ context.Context, record sdk.Record) error {
	table := d.getTableName(record.Metadata)
	query, vals := d.queryBuilder.BuildInsertQuery(record, table)
	err := d.session.Query(query, vals...).Exec()
	if err != nil {
		return fmt.Errorf("error while inserting data: %w", err)
	}

	return nil
}

// handleUpdate create and execute the cql query to update a row.
func (d *Destination) handleUpdate(_ context.Context, record sdk.Record) error {
	table := d.getTableName(record.Metadata)
	query, vals := d.queryBuilder.BuildUpdateQuery(record, table)
	err := d.session.Query(query, vals...).Exec()
	if err != nil {
		return fmt.Errorf("error while updating data: %w", err)
	}

	return nil
}

// handleDelete create and execute the cql query to delete a row.
func (d *Destination) handleDelete(_ context.Context, record sdk.Record) error {
	table := d.getTableName(record.Metadata)
	query, vals := d.queryBuilder.BuildDeleteQuery(record, table)
	err := d.session.Query(query, vals...).Exec()
	if err != nil {
		return fmt.Errorf("error while deleting data: %w", err)
	}

	return nil
}

// validateStructuredRecord return an error if the record key or payload is not structured.
func (d *Destination) validateStructuredRecord(record sdk.Record) error {
	// delete operation doesn't need a structured payload
	if record.Operation != sdk.OperationDelete {
		// check that payload is structured
		if _, ok := record.Payload.After.(sdk.StructuredData); !ok {
			return fmt.Errorf("payload should be structured data")
		}
	}
	// check that key is structured for all operations
	if _, ok := record.Key.(sdk.StructuredData); !ok {
		return fmt.Errorf("key should be structured data")
	}
	return nil
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
