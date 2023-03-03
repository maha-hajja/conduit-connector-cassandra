package cassandra_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	cassandra "github.com/conduitio-labs/conduit-connector-cassandra"
	"github.com/gocql/gocql"
)

func TestTeardown_NoOpen(t *testing.T) {
	con := cassandra.NewDestination()
	err := con.Teardown(context.Background())
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}
