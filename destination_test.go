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
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

func TestTeardown_NoOpen(t *testing.T) {
	con := NewDestination()
	err := con.Teardown(context.Background())
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func Test_Maha(t *testing.T) {
	// Define the Cassandra cluster configuration
	clusterConfig := gocql.NewCluster("127.0.0.1")
	clusterConfig.Keyspace = "store"
	clusterConfig.ConnectTimeout = time.Second * 5
	clusterConfig.Consistency = gocql.Quorum
	clusterConfig.Authenticator = gocql.PasswordAuthenticator{
		Username: "",
		Password: "",
	}

	// Connect to the Cassandra cluster
	session, err := clusterConfig.CreateSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()

	// Execute a CQL query
	var result int
	if err := session.Query("SELECT item_count FROM shopping_cart WHERE userid = ?", "1234").Scan(&result); err != nil {
		panic(err)
	}
	fmt.Println(result)
}
