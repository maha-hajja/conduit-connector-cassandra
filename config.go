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
	"net"
	"regexp"
	"strconv"
)

//go:generate paramgen -output=paramgen_dest.go DestinationConfig

type DestinationConfig struct {
	// The keyspace name that has the table (similar to a database in a relational database system).
	Keyspace string `json:"keyspace" validate:"required"`
	// The table name.
	Table string `json:"table" validate:"required"`
	// Comma separated list of Cassandra nodes' addresses (at least one), ex: 127.0.0.1:9042,127.0.0.2:8080
	Nodes []string `json:"nodes" validate:"required"`
	// Authentication mechanism used by Cassandra.
	AuthMechanism string `json:"auth.mechanism" validate:"inclusion=none|basic" default:"none"`
	// Username, only if basic auth is used.
	AuthUsername string `json:"auth.basic.username"`
	// Password, only if basic auth is used.
	AuthPassword string `json:"auth.basic.password"`
}

const (
	AuthMechanismBasic = "basic"
	AuthMechanismNone  = "none"
)

// validateConfig extra validations needed for destination config.
func (d *DestinationConfig) validateConfig() error {
	if d.AuthMechanism == AuthMechanismBasic && (d.AuthUsername == "" || d.AuthPassword == "") {
		return fmt.Errorf("auth.basic.username and auth.basic.password should be provided for basic authentication mechanism")
	}
	err := d.validateNodes()
	if err != nil {
		return err
	}
	return nil
}

func (d *DestinationConfig) validateNodes() error {
	var err error
	for _, n := range d.Nodes {
		err = d.validateHostPort(n)
		if err != nil {
			return fmt.Errorf("invalid node format %q: %w", n, err)
		}
	}
	return nil
}

func (d *DestinationConfig) validateHost(host string) error {
	hostRegexRFC952 := regexp.MustCompile(`^[a-zA-Z]([a-zA-Z0-9\-]+[\.]?)*[a-zA-Z0-9]$`)
	hostRegexRFC1123 := regexp.MustCompile(`^([a-zA-Z0-9]{1}[a-zA-Z0-9-]{0,62}){1}(\.[a-zA-Z0-9]{1}[a-zA-Z0-9-]{0,62})*?$`)
	if !hostRegexRFC1123.MatchString(host) && !hostRegexRFC952.MatchString(host) {
		return fmt.Errorf("invalid hostname format")
	}
	return nil
}

func (d *DestinationConfig) validateHostPort(hostport string) error {
	host, port, err := net.SplitHostPort(hostport)
	if err != nil {
		return fmt.Errorf("invalid host:port format: %w", err)
	}
	// Port should be <= 65535 and >=1.
	if portNum, err := strconv.ParseInt(port, 10, 32); err != nil || portNum > 65535 || portNum < 1 {
		return fmt.Errorf("invalid port value, should be an int between 1 and 65535")
	}
	return d.validateHost(host)
}
