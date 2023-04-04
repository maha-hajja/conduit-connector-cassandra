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
	"testing"

	"github.com/matryer/is"
)

func TestConfig_AuthMechanism(t *testing.T) {
	testCases := []struct {
		name    string
		config  DestinationConfig
		wantErr bool
	}{{
		name: "password missing",
		config: DestinationConfig{
			AuthMechanism: AuthMechanismBasic,
			AuthUsername:  "username",
		},
		wantErr: true,
	}, {
		name: "username missing",
		config: DestinationConfig{
			AuthMechanism: AuthMechanismBasic,
			AuthPassword:  "pass",
		},
		wantErr: true,
	}, {
		name: "username and password missing",
		config: DestinationConfig{
			AuthMechanism: AuthMechanismBasic,
		},
		wantErr: true,
	}, {
		name: "valid for none mechanism",
		config: DestinationConfig{
			AuthMechanism: AuthMechanismNone,
		},
		wantErr: false,
	},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			err := tt.config.validateConfig()
			if tt.wantErr {
				is.True(err != nil)
				return
			}
			is.NoErr(err)
		})
	}
}

func TestConfig_Nodes(t *testing.T) {
	testCases := []struct {
		name    string
		config  DestinationConfig
		wantErr bool
	}{{
		name: "port is greater than 65535",
		config: DestinationConfig{
			Nodes: []string{
				"127.0.0.1:99999",
			},
		},
		wantErr: true,
	}, {
		name: "port is lower than 1",
		config: DestinationConfig{
			Nodes: []string{
				"127.0.0.1:0",
			},
		},
		wantErr: true,
	}, {
		name: "port is not integer",
		config: DestinationConfig{
			Nodes: []string{
				"127.0.0.1:conduit",
			},
		},
		wantErr: true,
	}, {
		name: "invalid host with port, ends with .",
		config: DestinationConfig{
			Nodes: []string{
				"conduit.io.:8080",
			},
		},
		wantErr: true,
	}, {
		name: "localhost",
		config: DestinationConfig{
			Nodes: []string{
				"localhost:8080",
			},
		},
		wantErr: false,
	}, {
		name: "valid host_port",
		config: DestinationConfig{
			Nodes: []string{
				"127.0.0.1:9042",
				"localhost:9042",
			},
		},
		wantErr: false,
	}, {
		name: "invalid hostport, ends with .",
		config: DestinationConfig{
			Nodes: []string{
				"127.0.0.1.:9042",
			},
		},
		wantErr: true,
	}, {
		name: "host without port is valid",
		config: DestinationConfig{
			Nodes: []string{
				"localhost",
				"127.0.0.1",
			},
		},
		wantErr: false,
	}, {
		name: "invalid host, ends with .",
		config: DestinationConfig{
			Nodes: []string{
				"localhost..",
			},
		},
		wantErr: true,
	},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			err := tt.config.validateConfig()
			if tt.wantErr {
				is.True(err != nil)
				return
			}
			is.NoErr(err)
		})
	}
}
