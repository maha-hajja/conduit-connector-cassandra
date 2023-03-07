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

//go:generate paramgen -output=paramgen_dest.go DestinationConfig

import (
	"context"
	"fmt"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/gocql/gocql"
)

type Destination struct {
	sdk.UnimplementedDestination

	config  DestinationConfig
	session *gocql.Session
}

type DestinationConfig struct {
	// The keyspace (similar to a database in a relational database system) that has the table.
	Keyspace string `json:"keyspace" validate:"required"`
	// The table name.
	Table string `json:"table" validate:"required"`
	// The host to access Cassandra.
	Host string `json:"host" validate:"required"`
	// Cassandra’s TCP port.
	Port int `json:"port" default:"9042"`
	// Authentication mechanism used by Cassandra.
	AuthMechanism string `json:"auth.mechanism" validate:"inclusion=none|basic" default:"none"`
	// Username, only if basic auth is used.
	AuthUsername string `json:"auth.basic.username"`
	// Password, only if basic auth is used.
	AuthPassword string `json:"auth.basic.password"`
}

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
	return nil
}

func (d *Destination) Open(ctx context.Context) error {
	// Define the Cassandra cluster configuration
	clusterConfig := gocql.NewCluster(d.config.Host)
	clusterConfig.Keyspace = d.config.Keyspace
	clusterConfig.ConnectTimeout = time.Second * 5
	clusterConfig.Port = d.config.Port

	// todo: check auth mechanism
	clusterConfig.Authenticator = gocql.PasswordAuthenticator{
		Username: "",
		Password: "",
	}

	// Connect to the Cassandra cluster
	session, err := clusterConfig.CreateSession()
	if err != nil {
		return fmt.Errorf("error connecting to the cassandra cluster: %w", err)
	}
	defer session.Close()
	return nil
}

func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	// Write writes len(r) records from r to the destination right away without
	// caching. It should return the number of records written from r
	// (0 <= n <= len(r)) and any error encountered that caused the write to
	// stop early. Write must return a non-nil error if it returns n < len(r).
	return 0, nil
}

func (d *Destination) Teardown(ctx context.Context) error {
	if d.session != nil {
		d.session.Close()
	}
	return nil
}
