package cassandra

//go:generate paramgen -output=paramgen_dest.go DestinationConfig

import (
	"context"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Destination struct {
	sdk.UnimplementedDestination

	config DestinationConfig
}

type DestinationConfig struct {
	// The keyspace (similar to a database in a relational database system) that has the table.
	Keyspace string `json:"keyspace" validate:"required"`
	// The table name.
	Table string `json:"table" validate:"required"`
	// Column name that records should use for their Key fields.
	KeyColumn string `json:"keyColumn" validate:"required"`
	// The host to access Cassandra.
	Host string `json:"host" validate:"required"`
	// Cassandra’s TCP port.
	Port string `json:"port" default:"9042"`
	// Username, only if password auth is turned on for Cassandra.
	AuthUsername string `json:"auth.username"`
	// Password, only if password auth is turned on for Cassandra.
	AuthPassword string `json:"auth.password"`
}

func NewDestination() sdk.Destination {
	// Create Destination and wrap it in the default middleware.
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
	// validate password and username both exist or don't
	return nil
}

func (d *Destination) Open(ctx context.Context) error {
	// Open is called after Configure to signal the plugin it can prepare to
	// start writing records. If needed, the plugin should open connections in
	// this function.
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
	// Teardown signals to the plugin that all records were written and there
	// will be no more calls to any other function. After Teardown returns, the
	// plugin should be ready for a graceful shutdown.
	return nil
}
