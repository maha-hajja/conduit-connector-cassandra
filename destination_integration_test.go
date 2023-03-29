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
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/gocql/gocql"
	"github.com/matryer/is"
)

const (
	testKeyspace = "conduit_test"
	testNodes    = "127.0.0.1:9042"
)

func TestDestination_Write(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	// simple connect to get a Cassandra session
	session := simpleConnect(t, map[string]string{
		"nodes": testNodes,
	})
	// use the simple connect session to setup for the test
	table := setupTest(t, session)

	destination := NewDestination()
	err := destination.Configure(ctx, map[string]string{
		"nodes":    testNodes,
		"keyspace": testKeyspace,
		"table":    table,
	})
	is.NoErr(err)
	err = destination.Open(ctx)
	is.NoErr(err)
	defer func() {
		err := destination.Teardown(ctx)
		is.NoErr(err)
	}()

	testCases := []struct {
		name   string
		record sdk.Record
	}{{
		name: "snapshot operation to insert query",
		record: sdk.Record{
			Position:  sdk.Position("foo"),
			Operation: sdk.OperationSnapshot,
			Key:       sdk.StructuredData{"id1": "6", "id2": 6},
			Payload: sdk.Change{
				After: sdk.StructuredData{
					"column1": 22,
					"column2": false,
					// match the precision that Cassandra uses for timestamp.
					"column3": time.Now().UTC().Truncate(time.Millisecond),
				},
			},
		},
	}, {
		name: "create operation to insert query",
		record: sdk.Record{
			Operation: sdk.OperationCreate,
			Key:       sdk.StructuredData{"id1": "7", "id2": 7},
			Payload: sdk.Change{
				After: sdk.StructuredData{
					"column1": 33,
					"column2": true,
					"column3": time.Now().UTC().Truncate(time.Millisecond),
				},
			},
		},
	}, {
		name: "update operation",
		record: sdk.Record{
			Position:  sdk.Position("foo"),
			Operation: sdk.OperationUpdate,
			// this record is already in the table
			Key: sdk.StructuredData{"id1": "1", "id2": 1},
			Payload: sdk.Change{
				After: sdk.StructuredData{
					"column1": 44,
					"column2": false,
					"column3": time.Now().UTC().Truncate(time.Millisecond),
				},
			},
		},
	}, {
		name: "delete operation",
		record: sdk.Record{
			Operation: sdk.OperationDelete,
			// this record is already in the table
			Key: sdk.StructuredData{"id1": "1", "id2": 1},
			Payload: sdk.Change{
				// rawData payload for a delete operation should be fine
				After: sdk.RawData{},
			},
		},
	},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			is = is.New(t)
			id1 := tt.record.Key.(sdk.StructuredData)["id1"]
			id2 := tt.record.Key.(sdk.StructuredData)["id2"]

			i, err := destination.Write(ctx, []sdk.Record{tt.record})
			is.NoErr(err)
			is.Equal(i, 1)
			time.Sleep(time.Second)

			got, err := queryTestTable(session, table, id1, id2)
			switch tt.record.Operation {
			case sdk.OperationCreate, sdk.OperationSnapshot, sdk.OperationUpdate:
				is.NoErr(err)
				is.Equal(tt.record.Payload.After, got)
			case sdk.OperationDelete:
				is.Equal(err, gocql.ErrNotFound)
			}
		})
	}
}

func TestDestination_Data_Format(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	session := simpleConnect(t, map[string]string{
		"nodes": testNodes,
	})
	table := setupTest(t, session)

	destination := NewDestination()
	err := destination.Configure(ctx, map[string]string{
		"nodes":    testNodes,
		"keyspace": testKeyspace,
		"table":    table,
	})
	is.NoErr(err)
	err = destination.Open(ctx)
	is.NoErr(err)
	defer func() {
		err := destination.Teardown(ctx)
		is.NoErr(err)
	}()

	testCases := []struct {
		name         string
		record       sdk.Record
		wantErr      bool
		errSubString string
	}{{
		name: "rawData payload for a create operation should fail",
		record: sdk.Record{
			Operation: sdk.OperationCreate,
			Key:       sdk.StructuredData{"id1": "1", "id2": 1},
			Payload: sdk.Change{
				After: sdk.RawData{},
			},
		},
		wantErr:      true,
		errSubString: "payload should be structured data",
	}, {
		name: "rawData key should fail",
		record: sdk.Record{
			Operation: sdk.OperationCreate,
			Key:       sdk.RawData("id:1"),
			Payload: sdk.Change{
				After: sdk.StructuredData{},
			},
		},
		wantErr:      true,
		errSubString: "key should be structured data",
	}, {
		name: "rawData payload for a delete operation should pass",
		record: sdk.Record{
			Operation: sdk.OperationDelete,
			Key:       sdk.StructuredData{"id1": "1", "id2": 1},
			Payload: sdk.Change{
				After: sdk.RawData{},
			},
		},
		wantErr: false,
	},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			i, err := destination.Write(ctx, []sdk.Record{tt.record})
			if tt.wantErr {
				is.True(err != nil)
				is.True(strings.Contains(err.Error(), tt.errSubString))
				is.Equal(i, 0)
			} else {
				is.NoErr(err)
				is.Equal(i, 1)
			}
		})
	}
}

func simpleConnect(t *testing.T, cfg map[string]string) *gocql.Session {
	is := is.New(t)
	var config DestinationConfig
	err := sdk.Util.ParseConfig(cfg, &config)
	is.NoErr(err)
	clusterConfig := gocql.NewCluster(config.Nodes...)

	// Connect to the Cassandra cluster
	session, err := clusterConfig.CreateSession()
	is.NoErr(err)
	t.Cleanup(func() {
		session.Close()
	})
	return session
}

// setupTest creates a new keyspace and table and inserts a record into the table, returns the table name.
func setupTest(t *testing.T, session *gocql.Session) string {
	is := is.New(t)

	table := randomIdentifier(t)

	query := `
	CREATE KEYSPACE IF NOT EXISTS %s
	WITH replication = {
		'class': 'SimpleStrategy',
		'replication_factor': '1'
	}
	`
	query = fmt.Sprintf(query, testKeyspace)
	err := session.Query(query).Exec()
	is.NoErr(err)

	t.Cleanup(func() {
		query = `DROP KEYSPACE IF EXISTS %s`
		query = fmt.Sprintf(query, testKeyspace)
		err := session.Query(query).Exec()
		is.NoErr(err)
	})

	query = `
	CREATE TABLE IF NOT EXISTS %s.%s (
			id1 text,
			id2 int,
			column1 int,
			column2 boolean,
			column3 timestamp,
			PRIMARY KEY (id1, id2)
		)
	`
	query = fmt.Sprintf(query, testKeyspace, table)
	err = session.Query(query).Exec()
	is.NoErr(err)

	t.Cleanup(func() {
		query := `DROP TABLE IF EXISTS %s.%s`
		query = fmt.Sprintf(query, testKeyspace, table)
		err := session.Query(query).Exec()
		is.NoErr(err)
	})

	query = `INSERT INTO %s.%s (id1, id2, column1, column2, column3) VALUES (?, ?, ?, ?, ?)`
	query = fmt.Sprintf(query, testKeyspace, table)
	err = session.Query(query, "1", 1, 123, false, time.Now().UTC().Truncate(time.Millisecond)).Exec()
	is.NoErr(err)

	return table
}

func queryTestTable(session *gocql.Session, table string, id1 any, id2 any) (sdk.StructuredData, error) {
	var (
		column1 int
		column2 bool
		column3 time.Time
	)

	query := "SELECT column1, column2, column3 FROM %s.%s WHERE id1=? AND id2=?"
	query = fmt.Sprintf(query, testKeyspace, table)
	if err := session.Query(query, id1, id2).Scan(&column1, &column2, &column3); err != nil {
		return nil, err
	}

	return sdk.StructuredData{
		"column1": column1,
		"column2": column2,
		"column3": column3,
	}, nil
}

func randomIdentifier(t *testing.T) string {
	return fmt.Sprintf("conduit_%v_%d",
		strings.ToLower(t.Name()),
		time.Now().UnixMicro()%1000)
}
