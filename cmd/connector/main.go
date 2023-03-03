package main

import (
	sdk "github.com/conduitio/conduit-connector-sdk"

	cassandra "github.com/conduitio-labs/conduit-connector-cassandra"
)

func main() {
	sdk.Serve(cassandra.Connector)
}
