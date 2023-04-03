# Conduit Connector for Cassandra
[Conduit](https://conduit.io) destination connector for Cassandra.

## How to build?
Run `make build` to build the connector.

## Testing
Run `make test` to run all the unit tests. Run `make test-integration` to run the integration tests.

The Docker compose file at `test/docker-compose.yml` can be used to run the required resource locally.

## Destination
This destination connector pushes data from upstream resources to Cassandra via Conduit.

It parses the record received into a CQL (Cassandra Query Language) and executes that CQL into Cassandra using 
[gocql](https://github.com/gocql/gocql). If the record has the operation `create` or `snapshot` then it's parsed into an
`INSERT` query, if the operation is `update` then it's parsed into an `UPDATE` query, and `DELETE` query for
`delete` operation.

Make sure that the destination table that the connector will write the records to, has the same schema as the 
payload for the records received. so if the payload looks like this:

```json
{
  "id": 1,
  "name": "john",
  "full-time": true,
  "salary": 1000.1,
  "age": 25
}
```
then you should have a Cassandra table that looks like:
```sql
CREATE TABLE table_name ( id int NOT NULL, name varchar(255), full_time bool, salary double, age int, PRIMARY KEY (id));
```

### Configuration

| name                       | description                                | required | default value |
|----------------------------|--------------------------------------------|----------|---------------|
| `nodes` | Comma separated list of Cassandra nodes' addresses (at least one), ex: `127.0.0.1:9042`,`127.0.0.2:8080`. | true     |          |
| `keyspace` | The keyspace name that has the table (similar to a database in a relational database system). | true     |          |
| `table` | The table name to write data into. | true     |          |
| `auth.mechanism` | Authentication mechanism used by Cassandra, use `basic` for password auth, and `none` if auth is off. | false     | `none`         |
| `auth.basic.username` | Username, required only if `basic` auth mechanism is used. | false     |          |
| `auth.basic.password` | Password, required only if `basic` auth mechanism is used. | false     |          |


## Example pipeline configuration file
```yaml
   pipelines:
   cassandra-pipeline:
     status: running
     name: example-pipeline
     description: write data into Cassandra.
     connectors:
       postgres-con:
         type: source
         plugin: builtin:postgres # you can use any other source connector, this is just an example.
         name: postgres-source
         settings:
           url: postgresql://username:pass@127.0.0.1:5432/mydb #postgresql://{username}:{password}@{host}:{port}/{database}
           table: employees
           orderingColumn: id
       cassandra-con:
         type: destination
         plugin: standalone:cassandra
         name: cassandra-dest
         settings:
           nodes: 127.0.0.1:9042 #{host}:{port}
           keyspace: company
           table: employees
     processors:
       proc1:
         type: parsejsonpayload #postgres creates raw data payload, but json formatted, so this processor will convert the raw data into structured. 
```
Build your Cassandra connector, then place the connector binary in the `connectors` directory relative to Conduit,
check [connectors](https://github.com/ConduitIO/conduit#connectors) for more details. Also, check [Pipeline Configuration Files Docs](https://github.com/ConduitIO/conduit/blob/main/docs/pipeline_configuration_files.md)
 for more details about how to run this pipeline.

## Known Issues & Limitations
* Supports only structured data format key and payload. If your data is raw and json formatted, then you can use
[conduit's builtin processor](https://github.com/ConduitIO/conduit/blob/main/pkg/processor/procbuiltin/parsejson.go) 
  `parsejsonpayload` or `parsejsonkey` to parse your json data into structured data.

## Planned work
* Support raw data formats for keys and payloads.
