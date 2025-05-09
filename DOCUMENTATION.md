# MIMIRO Data Hub - User Guide

## Concepts

Semantic Graph databases are the ideal basis for an integration platform, they are schemaless and use URIs to robustly identify things, classes and property classes. This provides the foundation for taking any kind of data, from any source, and connecting it together in a single data space.

Data integration is often done by writing ad-hoc code to talk to different APIs, and long running batch ETL jobs. The data hub standardises on a simple, generic protocol for exposing and updating remote systems with support for both batch, incremental and streaming modes.

Combining the semantic graph database with simple synchronisation protocol delivers a generic and powerful capability for collecting, connecting and delivering data from and to many sources - it is the ultimate data liberation technology.

Once the data is in the graph database it can be connected via queries and transformed to produce new unified data structures. These data can then be used as the basis for ML and AI, or sent to external third parties via APIs or data exports.

Finally, data is changing over time, it is often useful to go back to a given moment to see how things were connected at that specific point. The MIMIRO graph database is an immutable store allowing the graph to be queried and traversed at any point in time.

## Getting Going

Getting started with the MIMIRO data hub is quick and easy. The data hub can be run locally with just one command. Check out the [getting started](./README.md#getting-started) section for how to get it running.

Working with the data hub API can be done in many ways, but we really recommend getting and installing, `mim`, the MIMIRO data hub CLI.

`mim` can be downloaded for any platform: linux, macos, and windows. Check out the [releases](https://github.com/mimiro-io/datahub-cli/releases) on github.

The data hub cli `mim` is written in Go and compiled into a single binary. Download the right version for your operating systems, unpack it and add the `mim` binary to your path.

## Specifications

The MIMIRO data hub implements the [Universal Data API specification (UDA)](https://open.mimiro.io/specifications). The MIMIRO data hub extends the API in the UDA specification with APIs for queries, jobs and transformations.

## Accessing and Managing the MIMIRO Data Hub

All access to a data hub is via the [REST API](https://api.mimiro.io/api). This can be accessed either via the `mim` or via code and http requests. `cURL` can also be used to access the API.

By default, API endpoints are unsecured. Where needed, JWT tokens secure endpoints. Acquiring a valid token differs in different contexts. Check with the provider of the endpoint to get access to security information. There is no security enabled when running the data hub locally.

Once installed, use the `mim` to connect to an endpoint. This action stores the web address of the server, and the JWT token if needed, for subsequent operations. You can register many different endpoints. This is useful as the `mim` can be used to connect to any [UDA](#specifications) compliant endpoint.

```
mim login --server="https://api.example.io" --token="jwt bearer token"
```

or to access a local / insecure server:

```
mim login --server="http://localhost:8080"
```

## Data Structures

At the core of the data hub are `datasets`. Each dataset contains `entities`. An `entity` is a single data object that has an identifier, properties, and references to other entities.

The JSON data format, along with some special keys, is used when serialising an Entity.

```json
{
    "id": "a uri identifier",
    "deleted": "flag indicating if the entity is deleted",
    "props": {},
    "refs": {}
}
```

The following is an example entity:

```json
{
    "id": "http://data.mimiro.io/people/homer",
    "deleted": "false",
    "props": {
        "http://data.mimiro.io/schema/person/fullname": "homer simpson"
    },
    "refs": {
        "http://data.mimiro.io/schema/person/worksfor": "http://data.mimiro.io/companies/mimiro",
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type": "http://data.mimiro.io/schema/person"
    }
}
```

Note the use of URIs for property names and entity identifiers. Entities are often returned from the data hub in an array. To make the payload more concise, a context can be added as the first JSON object. The context defines prefixes and corresponding expansions.

```json
[
    {
        "id": "@context",
        "namespaces": {
            "schema": "http://data.mimiro.io/schema/",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "person": "http://data.mimiro.io/schema/person/",
            "people": "http://data.mimiro.io/people/",
            "companies": "http://data.mimiro.io/companies/"
        }
    },

    {
        "id": "people:homer",
        "deleted": "false",
        "props": {
            "person:fullname": "Homer Simpson"
        },
        "refs": {
            "person:worksfor": "companies:mimiro",
            "rdf:type": "schema:person"
        }
    }
]
```

Refer to the [specification](#specification) for more details on the `props` and `refs` data sections, or the data format in general.

## The mim cli

The `mim` provides an easy way to interact with a data hub. It provides help on every command. Typing just `mim` shows the top-level help.

Use the help command to find out more about different commands.

```
> mim help dataset
```

## Loading Datasets

Datasets can be populated directly or by using jobs. To load data into a dataset, it can be sent using the cli or the HTTP API.

Datasets need to be created before data can be loaded.

To create a dataset

```
> mim dataset create test.people

SUCCESS  Dataset has been created
```

All datasets MUST be uniquely named. It is recommended to use a naming convention to make it easier to work with datasets. Using a hierarchical `.` notation such as `orgunit.system.collection` works well.

To load data from a local file called people.json use the following CLI command.

The contents of people.json is as follows:

```json
[
    {
        "id": "@context",
        "namespaces": {
            "schema": "http://data.mimiro.io/schema/",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "person": "http://data.mimiro.io/schema/person/",
            "people": "http://data.mimiro.io/people/",
            "companies": "http://data.mimiro.io/companies/"
        }
    },

    {
        "id": "people:homer",
        "props": {
            "person:fullname": "Homer Simpson"
        },
        "refs": {
            "person:worksfor": "companies:mimiro",
            "rdf:type": "schema:person"
        }
    }
]
```

Load the data with the following `mim` command.

```
> mim dataset store test.people --filename=people.json

SUCCESS  Entities Loaded
```

## Getting Entities from Datasets

Entities can be retrieved from datasets as a stream of changes or as the latest entities in a dataset.

To get the changes from a dataset:

```
> mim dataset changes test.people
```

By default the format of the returned JSON is 'raw' but by adding `--pretty`-flag you can have it more human readable.

Entities are returned as an array of JSON objects and can also contain a continuation token. A continuation token can be used in subsequent requests.

## Setting public namespaces for a Dataset

By default, the context object in data hub responses lists all available namespace mappings in the data hub. When there is a large number of datasets with many namespaces in the data hub, this can be undesired.
Therefore, it is possible to configure a limited list of namespaces per dataset to be used in response contexts.

### Creating datasets with public namespaces

The recommended way to provide public namespaces is to provide them when creating new datasets.

```
> mim dataset create test.people --publicNamespaces=http://test.name.space,http://namespace.test

SUCCESS  Dataset has been created
```

### Update existing datasets with public namespaces

Each dataset has a meta-entity in an dataset called `core.Dataset`. To configure custom namespaces, the property `http://data.mimiro.io/core/dataset/publicNamespaces`
can be added to the dataset's entity in `core.Dataset`.

In this example, we add two namespaces as `publicNamespaces` to dataset `namespaces.Test`.

```json
> cat update.json
[
  {
    "id": "@context",
    "namespaces": {
      "ns0": "http://data.mimiro.io/core/dataset/",
      "ns1": "http://data.mimiro.io/core/",
      "ns2": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    }
  },
  {
    "id": "ns0:namespaces.Test",
    "refs": {
      "ns2:type": "ns1:dataset"
    },
    "props": {
      "ns0:items": 0,
      "ns0:name": "namespaces.Test",
      "http://data.mimiro.io/core/dataset/publicNamespaces": [
        "http://data.mimiro.io/core/dataset/",
        "http://data.mimiro.io/core/"
      ]
    }
  }
]
```

```
mim dataset store core.Dataset --filename=update.json
```

Now, when we retrieve entities from `namespaces.Test`, data hub will supply only `publicNamespaces` as context

```
> mim dataset entities namespaces.Test

# Listing entities from http://localhost:8080/datasets/namespaces.Test/entities

# Namespaces:

#   | Namespace
ns0 | http://data.mimiro.io/core/dataset/
ns1 | http://data.mimiro.io/core/

```

## Proxy Datasets

If data hub is deployed in an infrastructure setting with both internal and external services connected to it, data hub
can act as proxy for datasets in unexposed services.

An example: there is a UDA layer in an internal network, exposing tables of a relational database as datasets.
There is also a data hub with access to the internal network. The data hub is accessible from the internet.
By setting up proxy datasets for the database layer, data hub can make the data accessible for external services
without having to load the data.

Proxy datasets need the base Url of the proxied remote dataset as configuration. Optionally a security provider can
be referenced, if the remote dataset requires authentication. See (security providers)[#Working_with_security_providers]
It is also possible to set a request timeout for the proxy dataset, if omitted the default timeout is unlimited.

To create a proxy dataset, use the following command:
```shell
> curl -XPOST \
  --header "Content-Type: application/json" \
  --data '{"ProxyDatasetConfig": {"remoteUrl": "https://dsurl", "proxyAuthProvider": "providerName", "timeoutSeconds": 10}}' \
  http://datahub/datasets/dataset-name
```

or using the `mim` CLI:
```shell
> mim dataset create test.people \
    --proxy=true \
    --proxyRemoteUrl=https://remote/datasets/name \
    --proxyAuthProvider=authProviderName

SUCCESS  Dataset has been created
```

## Virtual Datasets

Virtual Datasets are similar to Proxy Datasets, but instead of proxying data from a remote source, they are based on
a transformation script that generates entities on the fly.

The transformation script is written in Javascript and can use the same functions as a transform in a job.

Since virtual datasets are not backed by a real data source, they are read-only.

The transform script needs to implement a javascript function with the following signature:

```javascript
function build_entities(params, since, limit) {
    const continuationToken = since + (limit || 5);
    for (let i = since; i < continuationToken; i++) {
        Emit(NewEntity());
    }
    return continuationToken;
}
```
DataHub will allow users to send any json object or array to the virtual dataset endpoint in a GET request. The data will be passed to the build_entities function as the params argument.
`since` request parameters are also forwarded to the javascript function.

Any value that is returned from the function will be used as the continuation token in the constructed UDA batch response.

Each entity that is emitted using the `Emit` function in the script will be included in the response.

To create a virtual dataset, create a dataset with extra configuration in the request body like so:

```bash
curl -XPOST \
  --header "Content-Type: application/json" \
  --data '{"virtualDatasetConfig": { "transform": "BASE64-ENCODED-TRANSFORMATION-SCRIPT" } }' \
  http://datahub/datasets/dataset-name
```


## Query

The query model is very simple, for now. It is possible to fetch a single entity via its URI, and it is possible to traverse from one, or many entities to related entities via incoming or outgoing references.

Examles are given both using the `mim` CLI

```
mim query ...
```

and using the HTTP API by posting a query payload to

```
POST /query
```

### Getting A Single Entity

To lookup a single entity:

-   using `mim`, the data hub CLI
    ```shell
    > mim query --id="http://data.mimiro.io/people/homer"
    ```
-   using the HTTP API, with the following POST body
    ```json
    {
        "entityId": "http://data.mimiro.io/people/homer"
    }
    ```

### Getting Related Entities

To fetch related entities for a given entity:

-   using `mim`, the data hub CLI

    ```shell
    > mim query --entity="http://data.mimiro.io/people/homer" \
                --via="http://data.mimiro.io/schema/person/"
    ```

-   using the HTTP API, with the following POST body
    ```json
    {
        "startingEntities": ["http://data.mimiro.io/people/homer"],
        "predicate": "http://data.mimiro.io/schema/person/"
    }
    ```

and to get entities referencing a given entity, e.g. all entities of type person.

-   using `mim`, the data hub CLI
    ```shell
    > mim query --entity="http://data.mimiro.io/schema/person" \
                --via="http://www.w3.org/1999/02/22-rdf-syntax-ns#type" \
                --inverse=true
    ```
-   using the HTTP API, with the following POST body

    ```json
    {
        "startingEntities": ["http://data.mimiro.io/schema/person"],
        "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
        "inverse": true
    }
    ```

### All Query parameters

The query payload for the /query endpoint accepts the following options, all of which have an equivalent
parameter in `mim query`, please consult `mim query --help` for details.

```json
{
    "entityId": "http://some.id",
    "startingEntities": ["http://one.id", "http://another.id"],
    "predicate": "*",
    "inverse": false,
    "datasets": ["people", "workplaces"],
    "details": false,
    "limit": 100,
    "continuations": []
}
```

| parameter        | default value                             | description                                                                                                             |
| ---------------- | ----------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| entityId         | empty                                     | if set, all other parameters except for datasets are ignored. Ask to lookup one entity                                  |
| startingEntities | []                                        | if no entityId and no continuations given, start query from these entities                                              |
| predicate        | none,required when using startingEntities | use '\*' to follow all relations, else only follow this relation                                                        |
| inverse          | false                                     | specify query direction, only relevant when using startingEntities                                                      |
| datasets         | []                                        | a filter. if other value than empty list, only consider relations belonging to these datasets                           |
| details          | false                                     | only reledant when using entityId. if true, augment returned entity with information about datasets and change history  |
| limit            | 100                                       | limit number of query results. if set explicitly, response may contain contiuation token list                           |
| continuations    | []                                        | value found in a previous query result page. can - together with limit - be used to retrieve next page of query results |

### Incoming or outgoing query

There are two types of queries; incoming and outgoing.
The incoming query finds all the references pointing to the id of your starting entity.

The outgoing query finds all the reference-ids that your starting entity is pointing to.

#### Incoming

```json
> starting entity
{
    "id":"ns0:a-company",
    "refs":{
        "ns0:type":"Company"
    },
    "props":{
        "ns0:name":"a-company"
    }
}

>referencing entity
{
    "id":"ns2:bob",
    "refs":{
        "ns1:worksfor":"ns0:a-company",
        "ns0:type":"Person"
    },
    "props":{
        "ns2:name":"bob"
    }
}

```

#### Outgoing

```json
>starting entity
{
    "id":"ns0:bob",
    "refs":{
        "ns1:worksfor":"ns2:a-company",
        "ns0:type":"Person"
    },
    "props":{
        "ns0:name":"bob"
    }
}

>referenced entity
{
    "id":"ns2:a-company",
    "refs":{
        "ns0:type":"Company"
    },
    "props":{
        "ns2:name":"a-company"
    }
}
```

### Javascript Query

As well as entity lookup and traversal, it is possible to run a javascript query against the data hub. This allows for more complex queries, such as filtering and aggregation.

To execute a javascript query it must be sent to the 'query' endpoint of the data hub. The query is sent as a POST request with the query as the body of the request.

```
POST /query
```

The query requets must be formatted as a JSON object with the following properties:

-   `query`: the javascript query to executed encoded as base64

An example is shown below:

```json
{
    "query": "base64 encoded javascript query"
}
```

The request must set the `Content-Type` header to `application/x-javascript-query`.

The query itself is a single function called ´do_query´.

The query function has access to all the functions that a transform has access to as well as the following functions:

-   `GetDatasetChanges(name, since, limit)` - returns a list of changes to the dataset since value. The limit parameter is optional and can be used to limit the number of changes returned. The changes object returned has the following properties, NextToken, Entities, and Context.
-   `WriteQueryResult(any)` - writes the object to the output stream. The object must be a valid json object.

An example query that iterates the changes in a dataset and does some counting before writing the result to the output stream is shown below:

```javascript
function do_query() {
    let obj = GetDatasetChanges("people", 0, 5);
    let entities = obj.Entities;
    let count = 0;
    for (let i = 0; i < entities.length; i++) {
        count++;
    }

    let result = { count: count };
    WriteQueryResult(result);
}
```

WriteQueryResult can be called multiple times to write multiple objects to the output stream. Array seperators are added automatically.

The result returned from the query is a JSON array containing the objects inserted there by the query. There is no fixed schema for the query results.

## Data Layers

Data Layers implement the [UDA Specification](#specification). They are used to expose data from different data sources, such as file systems and relational databases, in a standard way.

It is possible to connect and use `mim` against any compliant data layer.

We are developing and making available data layers for common systems. They are described at the [MIMIRO OPEN](https://open.mimiro.io) site.

## Jobs

Jobs are used to fetch data from remote datalayers into the data hub, they are used to connect and transform data in the data hub and they are used to send data to remote datalayers.

A Job is defined with three components: a source, an optional transform and a sink. Jobs that pull from or push to external datalayers execute on a schedule, jobs that move data between datasets can either be on a schedule or triggered as data arrives in the source dataset.

Job definitions are described using JSON and can be uploaded to the data hub using the CLI or the API directly. The following annotated JSON document is a template for how to write a job definition.

```json
{
    "id": "a unique job definition id",
    "triggers": [
        {
            "triggerType": "either 'cron' or 'onchange'",
            "jobType": "either 'incremental' or 'fullsync'",
            "schedule": "a cron expression defining when to execute the defined jobType. Only set when triggerType=cron",
            "monitoredDataset": "name of dataset to monitor, used with triggerType=onchange",
            "onError": [{
              "errorHandler":  "log or reRun",
                "retryDelay": "delay between retries, only used with reRun",
                "maxRetries": "number of retries, only used with reRun",
                "maxItems": "only used with log, the handler will log this many failing items before stopping. value 0 means log all failing items"
            }]
        }
    ],
    "source": {
        "Type": "Name of source type - see below for list"
    },
    "sink": {
        "Type": "Name of sink type - see below for list"
    },
    "type": "job"
}
```

### Source Types

The following source types exist and can be used. Note that access to new types of systems is provided by creating new data layers, no new source types will be added to the data hub.

#### HttpDataset Source

The `httpdatasetsource` reads data from a remote data layer that implements the Universal Data API specification.

It can be configured as follows:

```json
{
    "source": {
        "Type": "HttpDatasetSource",
        "Url": "full url of change endpoint of the datalayer to read from",
        "TokenProvider": "optional: name of token provider that allows access"
    }
}
```

#### Dataset Source

The dataset source reads entities from a dataset in the data hub.

```json
{
    "source": {
        "Type": "DatasetSource",
        "Name": "name of the dataset to read from",
        "LatestOnly": "true or false, indicating whether to emit all changes or only the latest change of each entity"
    }
}
```

The `LatestOnly` flag can be set to limit the number of entities emitted to only the latest version of each entity.
The default is that all changes of each entity are emitted, so that the whole dataset can be transformed and/or copied without loss of history.

#### Union Dataset Source

A union dataset source can be used to consume multiple datasets in the data hub.
All configured datasets are read sequentially, as if their contents were concatenated.

```json
{
    "source": {
        "Type": "UnionDatasetSource",
        "DatasetSources": [
            {
                "Type": "DatasetSource",
                "Name": "name of first dataset to read from",
                "LatestOnly": "true or false, indicating whether to emit all changes or only the latest change of each entity"
            },
            {
                "Type": "DatasetSource",
                "Name": "name of next dataset to read from",
                "LatestOnly": "true or false, indicating whether to emit all changes or only the latest change of each entity"
            }
        ]
    }
}
```

`DatasetSources` is a list of `DatasetSource` configurations. Other source types
are not supported.

Also note that changing the order or adding new `DatasetSource`
entries to the list makes previously produced continuation tokens invalid. The job
should be reset in that case.

#### Multi Source

This source has one main dataset which works like a `DatasetSource`. In addition, a list of `dependency` datasets can be configured.

A `dependency` is typically a dataset which is used in the job's transform queries to construct composite entities with attributes from both
main dataset and dependency dataset(s).

By declaring a dependency dataset, and defining how to link entities in the dependency dataset to entities in the main dataset via a sequence of `joins`,
MultiSource can in incremental jobs emit entities that have not been changed themselves, but which need to be reprocessed due to changed dependencies.

```json
{
    "source": {
        "Type": "MultiSource",
        "Name": "name of the main dataset",
        "LatestOnly": "true or false, indicating whether to emit all changes or only the latest change of each entity"
        "Dependencies": [
            {
                "dataset": "name of a dependency dataset",
                "joins": [
                    {
                        "dataset": "name of a linking dataset",
                        "predicate": "ref-name containing the link URI",
                        "inverse": true
                    },
                    {
                        "dataset": "name of main dataset. the last join in a dependency should link back to the main dataset",
                        "predicate": "ref-name containing  link to main entity",
                        "inverse": false
                    }
                ]
            }
        ]
    }
}
```

Note that the order of joins in a MultiSource's Dependency configuration is the inverse of the applied query order in transform code.

Example: A source dataset in a job is "person", and the job aims to enrich all person entities using the following queries in a transform.

The first query finds a company that the person has a "worksfor" relation to. The second Query finds a location for the workplace.
The location is then added to the person entity.

```javascript
const ns = GetNamespacePrefix("http://example.com/");
const companies = Query(GetId(e), PrefixField(ns, "worksfor"), false);
if (companies.length !== 0) {
    const firstCompany = companies[0][2];
    const workPlaces = Query(
        GetId(firstCompany),
        PrefixField(ns, "address"),
        false
    );
    if (workPlaces.length !== 0) {
        const firstWorkLocation = workPlaces[0][2];
        SetProperty(e, ns, "workLocation", GetProperty(firstWorkLocation));
    }
}
```

This is the typical use case for MultiSource: whenever locations or companies change, this job needs to reprocess all persons
connected to those companies and locations.

MultiSource emits all person entities affected by changes in locations or companies, if we add the following dependency configuration:

```json
{
    "source": {
        "Type": "MultiSource",
        "Name": "person",
        "LatestOnly": "true"
        "Dependencies": [{ "dataset": "location", "joins": [
            { "dataset": "company", "predicate": "http://example.com/address", "inverse": true },
            { "dataset": "person", "predicate": "http://example.com/worksfor", "inverse": true }
        ]}]
    }
}
```

When triggered, MultiSource will find all changed locations first, all compaties pointing to those locations second, and finally
all persons working for the found companies. These person are emitted from the Source and can now be reprocessed in the jobs transform.

##### MultiSouce dependency registration in javascript.

It is also possible to configure MultiSource dependencies using a special function in a job transform script:
`function track_queries(start)`

If present in a job's transform script, MultiSource will execute this function, providing a single parameter
representing the main dataset as starting point for dependency registrations.

The starting object offers the methods `hop(datasetName, relationshipName)` and `iHop(datasetName, relationshipName)`
for adding dependencies to MultiSource, where `hop` adds datasets connected via outgoing relations,
and `iHop` adds inverse relations. The relation direction in track_queries is the same as in the applied queries in `transform_entities`.

The above example configuration can be replaced using javascript registations.

```javascript
function track_queries(start) {
    start
        .hop("company", "http://example.com/worksfor")
        .hop("location", "http://example.com/address");
}
```

The json configuration of MultiSource does not need a `Dependencies` part if `track_queries` is used, the
rest of the source configuration remains the same.

##### Many Dependencies

To track changes in many dependent datasets, many configuration objects can be added to the `Dependencies` array when using
json config.

When using `track_queries`, many dependencies can be added as separate function call chains on the starting object. An example,
where the `start` parameter again represents a "person" dataset:

```javascript
function track_queries(start) {
    // first dependency, from person to home location
    start.hop("location", "http://example.com/home");

    // 2nd dependency, via all orders that inversely point to a person, to ordered products
    start
        .iHop("order", "http://show.web/customer")
        .hop("product", "http://shop.web/orderItem");
}
```

Note that the above example uses `iHop` to denote an inverse dependency. Inverse queries can find a lot of entities
pointing back to the previous hop, so it is recommended to use [PagedQueries](#pagedquery) in transform code.

### Sink Types

The following sink types are used to write data either to a dataset or to a remote datalayer endpoint.

#### HttpDatasetSink

HttpDatasetSink writes data to a remote data layer that implements the Universal Data API specification.
The sink's HTTP client has a timeout of 1 minute.
It will also retry failed requests up to 3 times in 2 second intervals.


```json
{
    "sink": {
        "Type": "HttpDatasetSink",
        "Url": "full url of the entities endpoint to write to",
        "TokenProvider": "name of token provider to allow access"
    }
}
```

#### DatasetSink

```json
{
    "sink": {
        "Type": "DatasetSink",
        "Name": "name of the dataset to write to"
    }
}
```

### Triggers

The triggers list can contain any number of trigger definitions.

There are two types of triggers that can be used to schedule jobs:
- `cron` periodically executes a job on a schedule
- `onchange` executes a job when a monitored dataset changes.

Additionally, a trigger configuration defines the mode of operation of the job:
- `incremental` only processes changes that are new since the last run
- `fullsync` processes all entities in the source dataset

> [!CAUTION]
> In jobs with an HttpDatasetSource, incremental updates can only be used on the /changes endpoint.

It can make sense to transfer small incremental updates with high frequency, while also having a daily fullsync defined in the same job.

Triggers can be configured with an `onError` section, which defines how to handle sink errors in the job.
There are two error handlers available:
- `log` will log the error and continue with the next entity.
  When the configured number of errors (`maxItems`) is reached, the job will stop and fail.
  If no `maxItems` is configured, the default is to log all errors and continue.
- `reRun` will re-run the entire job a number of times, with a delay between each retry.
  If the job still fails after the configured number of runs, the job will stop and fail.
  `reRun` can be configured with `maxRetries` and `retryDelay` to control the number of retries and the delay between retries.
  When `maxRetries` is omitted in config, the default is 3 tries. The default `retryDelay` is 10 seconds.

Note that [HttpDatasetSink](#HttpDatasetSink) also has a built in retry mechanism for failed HTTP requests. In many
cases that will be sufficient. A `reRun` error handler can be added when a remote target is expected to have longer downtimes.


### Examples

An example job definition that copies data between two datasets and runs on a cron schedule:

```json
{
    "id": "sync-datasetsource-to-datasetsink",
    "triggers": [
        {
            "triggerType": "cron",
            "jobType": "incremental",
            "schedule": "@every 2s"
        }
    ],
    "source": {
        "Type": "DatasetSource",
        "Name": "Products"
    },
    "sink": {
        "Type": "DatasetSink",
        "Name": "NewProducts"
    }
}
```

The following example shows a job definition that reads from a remote datalayer and stores the data in a dataset.

```json
{
    "id": "sync-remote-datasetsource-to-dataset",
    "triggers": [
        {
            "triggerType": "cron",
            "jobType": "incremental",
            "schedule": "@every 2s"
        }
    ],
    "source": {
        "Type": "HttpDatasetSource",
        "Url": "http://localhost:7777/datasets/products/changes"
    },
    "sink": {
        "Type": "DatasetSink",
        "Name": "Products"
    }
}
```

The following example shows how to configure a job to send data from a dataset to a remote data layer.
It also has a log error handler that logs the first failing entity and then stops with failure:

```json
{
    "id": "sync-datasetssource-to-httpdatasetsink",
    "triggers": [
        {
            "triggerType": "cron",
            "jobType": "incremental",
            "schedule": "@every 2s",
            "onError": [{
              "errorHandler":  "log",
              "maxItems": 1
            }]
        }
    ],
    "source": {
        "Type": "DatasetSource",
        "Name": "Products"
    },
    "sink": {
        "Type": "HttpDatasetSink",
        "Url": "http://localhost:7777/datasets/products/entities"
    }
}
```

To run a job just once, install it as paused and trigger a single run with the `/jobs/<id>/run` endpoint or `mim jobs operate`.
Example for paused job:

```json
{
    "id": "sync-remote-datasetsource-to-dataset",
    "triggers": [
        {
            "triggerType": "cron",
            "jobType": "incremental",
            "schedule": "@every 2s"
        }
    ],
    "paused": true,
    "source": {
        "Type": "DatasetSource",
        "Name": "Products"
    },
    "sink": {
        "Type": "DatasetSink",
        "Name": "NewProducts"
    }
}
```

To run a job that sends a full sync (snapshot) to a remote datalayer).
The job has a reRun error handler that retries the job 3 times with a 10 second delay between each retry:

```json
{
    "id": "sync-dataset-to-remote-dataset-full-sync",
    "triggers": [
        {
            "triggerType": "cron",
            "jobType": "fullsync",
            "schedule": "@every 24h",
            "onError": [{ "errorHandler":  "reRun" }]
        }
    ],
    "source": {
        "Type": "DatasetSource",
        "Name": "Products"
    },
    "sink": {
        "Type": "HttpDatasetSink",
        "Url": "http://server:8000/datasets/products/entities"
    }
}
```

### Jobs API

Creating, managing and checking the status of jobs can be done via the CLI or directly with the API.

To get help with jobs using the CLI enter:

```shell
mim help jobs
```

#### Listing Jobs

To list the jobs currently defined in a data hub.

```shell
mim jobs list
```

#### Adding a Job

A job should be defined in a .json file as described in the section above. The job is added using the following command:

Note: replace the filename with the one containing the job definition.

```shell
mim jobs add -f jobs.json
```

List the jobs to see that it has been created successfully.

```shell
mim jobs list
```

#### Deleting a Job

Jobs can be deleted using their id. You will be asked to confirm the deletion. This can be overriden using the force flag -C.

```shell
mim jobs delete -C=false simple-job
```

#### Pausing a Job

To pause a job so that it is not scheduled to run:

```shell
mim jobs operate simple-job -o pause
```

#### Starting a Job

To start a job:

```shell
mim jobs operate simple-job -o run
```

#### Stopping a Job

To stop a running job:

```shell
mim jobs operate simple-job -o kill
```

#### Inspecting a Job

To get the status of a job:

```shell
mim jobs status simple-job
```

#### Getting latest run info from a Job

To get information on latest run of a Job:

```shell
mim jobs history simple-job
```

## Transactional Updates

The data hub has two main modes of update:

1. Batches of entities are loaded into a single dataset, either via the API or using a job and loading it from another dataset or external data layer. Data updated in this way is guaranteed to have been committed.

2. Using transactions to make a single update to several datasets in a single transaction. Unlike the above sometimes it is necessary to write into several datasets at once in a transactional fashion.

Transactions can be executed either via the data hub API, or as part of a javascript transform.

The API endpoint is:

`/transactions`

and supports POST of a json document that represents the transaction.

The data structure for a transaction consists of a namespace declaring context, followed by a map of dataset names that each map to a list of entities.

The following example shows a transaction serialised as JSON.

```json
{
    "@context": {
        "namespaces": {
            "schema": "http://data.mimiro.io/schema/",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "person": "http://data.mimiro.io/schema/person/",
            "people": "http://data.mimiro.io/people/",
            "companies": "http://data.mimiro.io/companies/"
        }
    },
    "people": [
        {
            "id": "people:homer",
            "props": {
                "person:fullname": "Homer Simpson"
            },
            "refs": {
                "person:worksfor": "companies:mimiro",
                "rdf:type": "schema:person"
            }
        }
    ]
}
```

## Transforms

Jobs take data from a source and write it to a sink. It is often necessary to transform the source entities into a new form before writing them to the sink. Each job definition can contain a `transform` that is executed to carry out the manipulation.

Transforms can be implemented either as an HTTP based service or as an internal transform. Internal transforms are written using Javascript.

### External Transform

External transforms are HTTP services that implement a single endpoint. When a job executes, it sends a batch of entities to the external service. The service is free to operate on the entities as required, and then return a new JSON document containing the modified or created entities.

The following example shows how to configure a job to use an HTTP transform:

```json
{
    "title": "test-http-transform",
    "id": "sync-datasetsource-to-datasetsink-with-js",
    "runOnce": true,
    "source": {
        "Type": "DatasetSource",
        "Name": "Products"
    },
    "triggers": [
        {
            "triggerType": "onchange",
            "jobType": "incremental",
            "monitoredDataset": "Products"
        }
    ],
    "transform": {
        "Url": "http://localhost:8080/path-to/transform/",
        "Type": "HttpTransform",
        "TimeOut": 0
    },
    "sink": {
        "Type": "DatasetSink",
        "Name": "NewProducts"
    }
}
```

Note: external transforms can suffer from latency issues as data must be passed back and forth over the wire and any queries are also executed remotely. To mitigate against this, ensure that the query for related entities is used in batch mode. Alternatively, use internal transforms where possible. Another option is to set the attribute `TimeOut` on the transform. If set to `0` as shown above, there will be no timeout or set it to infinite effectively. The timeout is set in seconds.

### Internal Transform

Internal transforms are written in Javascript and executed in a sandbox.

Note: The version of Javascript supported is ES6. Please check for the restrictions regarding this version. The examples in this documentation were written when ES5.1 was the newest version supported, therefore there are no examples with `const` or `let`.

#### There is now support for writing transforms in TypeScript

Link provideded below to tutorial on how to set up your environment to write transforms in TypeScript.

[Open Mimiro TypeScript post](https://open.mimiro.io/software/typescript/)

Internal transforms can be run in parallel. To do this include an attribute on the transform called `Parallelism` whose value is an integer. e.g. 10 to run the transform in parallel.

Transforms written in Javascript need to be encoded as base64 and added in the transform section of a job definition. This can be done with the help of `mim jobs add -f job-with-transform.json -t javascript-transform.js` to automatically encode it or by manually encoding the transform section and adding it in the job-with-transform.json.

Example Job Definition:

```json
{
    "title": "test-internal-transform",
    "id": "sync-datasetsource-to-datasetsink-with-js",
    "runOnce": true,
    "triggers": [
        {
            "triggerType": "onchange",
            "jobType": "incremental",
            "monitoredDataset": "Products"
        }
    ],
    "source": {
        "Type": "DatasetSource",
        "Name": "Products"
    },
    "transform": {
        "Type": "JavascriptTransform",
        "Parallelism": 10,
        "Code": "ZnVuY3Rpb24gdHJhbnNmb3JtX2VudGl0aWVzKGVudGl0aWVzKSB7CiAgIHZhciBzdGFydHMgPSBbXTsKICAgdmFyIHJlcyA9IFF1ZXJ5KHN0YXJ0cywgInRlc3QiLCBmYWxzZSk7CiAgIHJldHVybiBlbnRpdGllczsKfQo="
    },
    "sink": {
        "Type": "DatasetSink",
        "Name": "NewProducts"
    }
}
```

The unencoded Javascript must contain a function called `transform_entities` that takes a single parameter. The script may contain other supporting functions. Take note of the in-built functions (listed below) when defining and naming functions. Be careful not to redefine these functions.

```javascript
function transform_entities(entities) {
    return entities;
}
```

The entities parameter is an array of json `Entity` objects as described in the data model section. Any valid Javascript can be used to modify the structure. NOTE: prefer changing the existing entity structures rather than trying to create something new.

There are a number of built-in functions to help operate on entities.

#### GetId

`GetId(entity)` takes a single parameter of type `Entity` and returns the value of the `id` property.

Given an entity:

```javascript
e = {
    id: "ns0:bob",
};

id = GetId(e);
```

returns value `ns0:bob`

#### SetId

`SetId(entity, id)` takes a parameter of type `Entity` and a string with an id, and updates the id of the entity.

Example:

```javascript
var e = NewEntity();
SetId(e, PrefixField("ns0", 42));

Log("Id is now: " + GetId(e));
```

```text
 INFO  - Id is now: ns0:42
```

#### GetNamespacePrefix

URIs are often represented as CURIEs. CURIEs are formed of a prefix part and local part. The prefix is key that corresponds to an expansion. To resolve a CURIE into a full URI the local part of appended to the prefix expansion.

`GetNamespacePrefix` can be used to return the prefix part for a given URI expansion. This is useful when looking up values or properties or references of an entity.

The function is used as follows:

```javascript
var personTypePrefix = GetNamespacePrefix(
    "http://data.mimiro.io/schema/people/"
);
```

#### AssertNamespacePrefix

`AssertNamespacePrefix` is used to create or return a prefix. This should be used in transforms that require a new namespace. It is used as follows:

```javascript
var newTypePrefix = AssertNamespacePrefix(
    "http://data.mimiro.io/schema/company/"
);
```

#### Timing

`Timing` can be used to create custom timing metrics around parts of a transform. The function accepts a metric name as
first parameter and a "send" boolean as optional second parameter. When the send parameter is false or omitted, the `Timing`
function registers a start-timestamp for the given metric name. When the send parameter is `true`, Timing sends the duration
since start as timing value to statsd.

```javascript
Timing("hello"); //register start for metric "hello"
// ... do something
Timing("hello", true); // send duration since start for metric "hello"
```

#### Log

Any value can be passed to `Log` and it will print it to the console. This should be used when testing and developing transforms locally. `Log()` can be used with different log-levels.

```javascript
var someval = "hello";
Log(someval);
```

Without defining the log-level the console log will default to loglevel `INFO`
Supported log-levels are:

```javascript
Log(someval, "INFO");
Log(someval, "WARNING");
Log(someval, "ERROR");
```

#### FindById

Many lookups can be done by taking the value of a reference and looking up the entity by its id value.

```javascript
// lookup by CURIE
var p1 = FindById("ns0:bob");

// lookup by full URI
var p2 = FindById("http://data.mimiro.io/people/bob");
```

#### Query

The Query function is used to lookup related entities.
It accepts an array of entity ids (CURIEs or full URIs), a relationship to traverse,
a flag indicating if the traversal is incoming or outgoing,
and an array of dataset names to limit the query scope if desired.

The result is a list of lists where each inner list is a result row.
The result row contains the entity id, the property used to find to find a relation,
and then the related entity.

_Note: when large query result sets are possible, it is recommended to use
`PagedQuery` instead of `Query`._

_Note: if an entity has multiple related entities then each appear in its own row._

```json
>returned from the Query function:

[
    [ "entity-id" , "property uri", { "id" : "related entity 1" } ],
    [ "entity-id", "property uri", { "id" : "related entity 2"}]
]
```

```javascript
// find all the companies bob works for, outgoing query
var queryResult = Query(["ns0:bob"], "ns1:worksfor", false, []);

// assuming there is a company then get that company
var company = queryResult[0][2];
Log(company);
```

```text
 INFO  - [company]
```

```javascript
//find all people that works for company in the dataset test.people, incoming query
var queryResult = Query(["ns0:company"], "ns1:worksfor", true, ["test.people"]);

//assuming there are multiple hits
var people = queryResult;
Log(people);
```

```text
 INFO  - [
["company",
 "worksfor:company",
{
    "id":"bob",
    "refs":{
        "type":"Person",
        "worksfor":"company"
    },
    "props":{
        "name":"bob",
        "start-date": "1970-01-01",
        "end-date": "1999-12-31"
    }
}],
["company",
 "worksfor:company",
{
    "id":"janet",
    "refs":{
        "type":"Person",
        "worksfor":"company"
    },
    "props":{
        "name":"janet",
        "start-date":"2000-01-01"
    }
}]]]
```

#### PagedQuery

`PagedQuery` is similar to `Query` in that it takes a list of starting URIs, a relationship to traverse, an inverse flag and
a list of datasets to scope the query. These parameters are provided using a parameter object. The format is

```javascript
{
    StartURIs: [string], // list of entity IDs as query start point(s) - REQUIRED
    Via: string,         // relationship to follow. "*" means "all" - REQUIRED
    Inverse: boolean,    // inverse flag telling the query whether to follow outgoing or incoming relations - OPTIONAL, default false
    Datasets: [string],   // list of dataset name to limit scope of query - OPTIONAL, default [] (no scope)
    Continuations: [tokenValue] // continue a previously started query - OPTIONAL, overrides all other attributes when set
}
```

In addition to those parameters, `PagedQuery` also requires a `pageSize` parameter and a callback function.

Under the hood, `PagedQuery` pages through the query result in batches,
and emits those batches to the callback function.
This is more memory efficient than loading complete query results in one operation for cases with many query results.

```javascript
// PagedQuery may be call the callback many times,
// emitting arrays of query result items
var cb = function (resultPage) {
    for (resultItem of resultPage) {
        Log(resultItem);
        // if the callback returns false ,PagedQuery will stop calling it.
        // Return true if you want to process more pages
        return false;
    }
};
PagedQuery(
    {
        StartURIs: ["ns0:company"], // list of starting URIs
        Via: "ns1:worksfor", // relationship to follow, can be "*" to follow all
        Inverse: true, // inverse flag
        Datasets: ["people", "companies"], // scope query to datasets: people and companies
    },
    10, // set page size to 10
    cb
);
```

output:

```json
{
    "StartURI": "ns0:company",
    "PredicateURI": "ns1:worksfor",
    "RelatedEntity": {
        "id": "ns0:bob",
        "props": { "active": true },
        "refs": {}
    }
}
```

Note that in the above example, our callback was called only once. This is because the callback returned `false`
after it processed the first item in the result batch.

If you want to consume the complete query result, then the callback must return `true` for every batch.

#### Imperative page consumption with `PagedQuery`

If the logic in a transform script calls for it, a query result can be consumed using multiple
calls to `PagedQuery`.

The return value of the `PagedQuery` function is a continuation value, which can be used as input for subsequent
calls to `PagedQuery`, in order to resume query processing.

When `Continuations` is provided in `PagedQuery`'s object parameter, then it takes precedence over the other attributes in the same object.

In the following example we use query continuations to go through a query result with different batch sizes.

```javascript
const nsPref = AssertNamespacePrefix("http://example.io/employments/");

let firstInactiveEmployee = null;

// page processing function.
const processPage = resultPage => resultPage.forEach(resultItem => {
        let relatedEntity = resultPage.RelatedEntity;
        if (GetProperty(relatedEntity, nsPref, "active") == false) {
            firstInactiveEmployee = relatedEntity;
            return;
        }
    })

// first only pull 10 results from database, in many cases we are lucky and quickly find a match here
const continuation = PagedQuery({StartURIs: [nsPref+":company"]}, 10, resultPage => {
    processPage(resultPage);
    return false;
}}

// if we did not find a match in the first 10, proceed with larger page size
if (firstInactiveEmployee === null) {
    PagedQuery({Continuations: continuation}, 10000, resultPage => {
        processPage(resultPage);
        if (firstInactiveEmployee !== null) {
            return false;
        }
        return true;
    }}
}

```

#### GetProperty

To get the value of a named property on an entity use the GetProperty function.

```javascript
var personTypePrefix = GetNamespacePrefix(
    "http://data.mimiro.io/schema/person/"
);

personName = GetProperty(person, personTypePrefix, "name");
```

The `GetProperty` function can also take an optional extra defaultValue

```javascript
var e = NewEntity();

// field1 is missing
var value = GetProperty(e, "ns0", "field1", "my default value");

Log(value);
```

```text
 INFO  - my default value
```

#### SetProperty

To set the value of a named property on an entity use the SetProperty function.

```javascript
var personTypePrefix = GetNamespacePrefix(
    "http://data.mimiro.io/schema/person/"
);

SetProperty(person, personTypePrefix, "name", "bobby");
```

#### AddReference

To set the value of a named reference on an entity use the AddReference function.
Note that the reference value must be a CURIE.
Also note that existing references with the same predicate will be overwritten.

```javascript
var personTypePrefix = AssertNamespacePrefix("http://data.mimiro.io/schema/person/");
var companyPrefix = AssertNamespacePrefix("http://data.mimiro.io/schema/company/");

AddReference(person, personTypePrefix, "worksfor", PrefixField(companyPrefix, "acme"));
```

#### SetDeleted / GetDeleted

`SetDeleted` takes a parameter `entity` of type Entity, and a boolean flag, and updates the deleted state on the Entity.

`GetDeleted` takes a single parameter `entity` of type Entity, and returns the deleted state of the Entity. If entity is
missing or null, this function returns undefined.

Example:

```javascript
var e = NewEntity();

SetDeleted(e, true);
var deleted = GetDeleted(e);

Log("Deleted: " + ToString(deleted));
```

```text
 INFO  - Deleted: true
```

#### RenameProperty

`RenameProperty` is used to rename a property and/or prefix.:
example data:

```json
{
    "id": "@context",
    "namespaces": {
        "ns0": "http://data.mimiro.io/HR/person/"
    }
},
{
    "id": "ns0:10",
    "props": {
        "ns0:name": "John",
        "ns0:sirname": "Finclestock"
    }
}
```

```javascript
var hrperson = GetNamespacePrefix("http://data.mimiro.io/HR/person/");
var crmperson = AssertNamespacePrefix("http://data.mimiro.io/CRM/person/");
var results = [];
function transform_entities(entities) {
    // iterate over all the entities passed into the function
    for (e of entities) {
        //this will rename both the namespace and the property
        RenameProperty(e, hrperson, "name", crmperson, "firstname");
        //this will rename only the property
        //RenameProperty(e, hrperson, "name", hrperson, "firstname");
        results.push(e);
    }
    // return the array of results
    return results;
}
```

The entity will now look like this:

```json
{
    "id": "@context",
    "namespaces": {
        "ns0": "http://data.mimiro.io/HR/person/",
        "ns1": "http://data.mimiro.io/CRM/person/"
    }
},
{
    "id": "ns0:10",
    "props": {
        "ns1:firstname": "John",
        "ns0:sirname": "Finclestock"
    }
}
```

#### ToString

`ToString` is used to convert values to the string representation of the value. This should be used in transforms that require a conversion from i.e integer to string:

```javascript
var myStringValue = ToString(myIntegerValue);
```

Can also be used as with `GetProperty()`:

```javascript
var myStringValue = ToString(
    GetProperty(originalEntity, originalPrefix, "OriginalIntegerValue")
);
```

#### RemoveProperty

`RemoveProperty` is used to remove a property from the entity.
example data:

```json
{
    "id": "@context",
    "namespaces": {
        "ns0": "http://data.mimiro.io/HR/person/"
    }
},
{
    "id": "ns0:10",
    "props": {
        "ns0:name": "John",
        "ns0:sirname": "Finclestock"
    }
}
```

```javascript
var hrperson = GetNamespacePrefix("http://data.mimiro.io/HR/person/");
var results = [];
function transform_entities(entities) {
    // iterate over all the entities passed into the function
    for (e of entities) {
        //this will remove the property
        RemoveProperty(e, hrperson, "sirname");
        results.push(e);
    }
    // return the array of results
    return results;
}
```

the entity will now look like this:

```json
{
    "id": "@context",
    "namespaces": {
        "ns0": "http://data.mimiro.io/HR/person/"
    }
},
{
    "id": "ns0:10",
    "props": {
        "ns0:name": "John"
    }
}
```

#### NewEntity

In many transforms it is OK to simple modify the entity in place. However, sometimes it is necessary to create new entities. These MUST be created using the built-in `NewEntity` function.

Below is an idiomatic use of the `NewEntity`.

```javascript
function transform_entities(entities) {
    // define a new array that will contain the new entity objects.
    var newEntities = [];

    // iterate over all the entities passed into the function
    for (e of entities) {
        // for each existing entity create a new entity object
        var newEntity = NewEntity();
        newEntity["ID"] = "some new id";

        // add the new entity to the array
        newEntities.push(newEntity);
    }

    // return the array of new entities
    return newEntities;
}
```

#### NewEntityFrom

When creating new entities, we need to track if the entity in the upstream dataset is marked as deleted or not. To simplify the process the function `NewEntityFrom` is needed.
Below is an idiomatic use of the `NewEntityFrom`. We should primarily use `NewEntityFrom` when we create entities in a transform.
This Function takes 4 parameters:

`NewEntityFrom(originalEntity, addType, copyProps, copyRefs)` where addType, copyProps, copyRefs are boolean.

```javascript
function transform_entities(entities) {
    var newEntities = [];

    for (e of entities) {
        var newEntity = NewEntityFrom(e, true, false, false);
        //this will generate a new entity with the id and deleted flag from the original entity (e).

        // for each existing entity check if the entity is deleted
        if (!GetDeleted(newEntity)) {
            // add your transform code
        }
        // add the new entity to the array
        newEntities.push(newEntity);
    }

    // return the array of new entities
    return newEntities;
}
```

this will emit an entity with the deleted flag set and type copied:

```json
{
    "id": "ns1:some_id_1",
    "recorded": 0,
    "deleted": true,
    "refs": {
        "ns1:type": "ns2:MyEntityType"
    },
    "props": {}
}
```

#### AsEntity

`AsEntity(value)` can be use to convert entity-shaped properties (sub-entities) into Entity instances, which in turn enables the use of other transform helper functions.

Example usage:

```
function transform_entities(entities) {
    var ns = GetNamespacePrefix(...);
    for (e of entities) {
       var address = GetProperty(e, ns, "address");
       // address is an entity-shaped json value, so we can apply AsEntity
       var addressEntity = AsEntity(address);

       // GetProperty and other helpers work on addressEntity
       var street = GetProperty(addressEntity, ns, "street");
    }
    return entities;
}
```

#### NewTransaction

`NewTransaction()` is used to create a new transaction object. A transaction can then be executed using the ExecuteTransaction function. Note that this function simply returns an empty transaction data structure. It does not open a transaction.

#### AssertDataset

`AssertDataset(datasettname)` can be used to assert the existence of a transaction sink.
If the dataset does not already exists, it is created when `ExecuteTransaction(txn)` is invoked
on the transaction that has the assertion.

```javascript
function transform_entities(entities) {
    var newEntities = [];
    for (e of entities) {
        newEntities.push(e);
    }
    var txn = NewTransaction();
    txn.AssertDataset("NewProducts");
    txn.DatasetEntities["NewProducts"] = newEntities;
    ExecuteTransaction(txn);
    return entities;
}
```

#### ExecuteTransaction

`ExecuteTransaction(txn)` is used to execute a transaction. The following example shows how to use both NewTransaction and ExecuteTransaction in a transform.

```
function transform_entities(entities) {
    for (e of entities) {
        var txn = NewTransaction();
        var newentities = [];
        newentities.push(e);
        txn.DatasetEntities["NewProducts"] = newentities;
        txn.DatasetEntities["ProductAudit"] = newentities;
        ExecuteTransaction(txn);
    }
    return entities;
}
```

It is recommended that jobs using transactions configure the DevNullSink.

### Using `mim` for Transforms

The `mim` can be used to run and develop transforms locally before creating jobs.

#### Testing a Transform on a dataset

The following command runs the transform script `transform1.js` on the dataset `test.people`. The data is fetched from the dataset, the script is executed locally, and the output displayed.

```shell
mim transform test test.people --file transform1.js
```

#### Testing a Transform on a given entity

There is also a possibility to test the transform on a known entity in the data hub by running a query and applying the transformation on the returned entity, the command runs the same transform as above but on the entity `http://data.mimiro.io/people/bob`. The data is fetched from the dataset, the script is executed locally, and the output displayed.

```shell
mim query --id "http://data.mimiro.io/people/bob" --via="*" --json | mim transform test --file transform1.js
```

#### Generate base64 encoded transform

To include a transform in a job definition it needs to be encoded as a base64
string. This can be done with the CLI.

```shell
mim transform import --file=transform1.js
```

The raw text and the encoded javascript is sent to stdout. It can then be copied into the json job definition as shown at the start of this section.

#### Updating the transform of a job

To update the transform of an existing job use the following command:

```shell
mim transform import simple-job --file=transform1.js
```

NOTE: The transform_entities function must be exported when using the above command. However, when generating and inserting base64 script the function MUST NOT be exported. We aim to fix this.

## Lineage

With jobs being the primary way of moving data around in the data hub, they form a lineage graph for datasets.
Sinks are the end of the line for a dataset, and sources are the start. Transform queries are additional inputs and
transform transactions are additional outputs.

The lineage graph can be queried using the lineage API.

### Lineage API

Get only lineage nodes directly connected to a single dataset
```
GET /lineage/{dataset}
```

Get the complete lineage graph
```
GET /lineage
```

The response format is a list of graph edges. Callers can use this to for example build a graphviz visualization of the lineage.

```javascript
[
    { "From": "source-dataset", "To": "sink-dataset", "Type": "copy" },
    { "From": "source-dataset", "To": "sink-dataset", "Type": "transform" },
    { "From": "other-dataset", "To": "sink-dataset", "Type": "transform-hop" }
]

```

The `Type` of an edge can be one of the following:
- `copy` - the sink dataset is a copy of the source dataset
- `transform` - the sink dataset is the result of a transform of the source dataset
- `transform-hop` - the sink dataset enriched with data from the `From` dataset in a transform


## Configuration

The Datahub can be configured in several ways, but it should work for testing purposes without any setup needed. However, once you are ready to deploy into a production environment, you need to configure security as a minimum.

The Datahub is fully configured through ENV variables, but does also support loading configurations from a file as well.

### Environment

#### General

`SERVER_PORT=8080`

The SERVER_PORT setting defines the server http port to use. Default is 8080. Note that the Datahub in no way supports terminating TLS (aka https) connections, and you must put it behind a proxy for https support.

`STORE_LOCATION=./server`

This is the location of the Badger database files, and will grow to a large size in an active Datahub.
There are disk considerations you need to consider around this location, and you should follow the directions on the Badger homepage, especially around the GOMAXPROCS=128 setting for troughput.

The faster your disk setup, the faster you can consume data.

If this is empty, then Datahub will attempt to use your $HOME directory, or if that is not present, it will default to /tmp. That may or may not work on Windows.

`LOG_LEVEL=INFO`

You can tune the LOG_LEVEL of the Datahub. The supported values are DEBUG and INFO.

`DD_AGENT_HOST=`

The Datahub supports reporting metrics trough a StatsD server. This is turned off if left empty, and you can turn it on by giving it an ip-address and a port combination.

`MAX_COMPACTION_LEVELS`

Can be used to override Badger's default 7 LSM levels. When more that 1.1TB disk space usage are exceeded or expected to be exceeded, 8 compaction levels are needed.

`FLATTEN_ON_START`

boolean flag to make datahub flatten the database on start. This is useful when you have deleted a lot of data in the database and you want to release some disk space.
Should only be set temporarily, as it slows startup time down considerably.

#### Securing the Data Hub

There are two main security models for the data hub.

1. No security / API gateway seured. All calls are allowed at the data hub API level. This mode can be used either when developing or when the data hub API is protected behind an API gateway that implements secure access.

2. Data Hub Security. This involves a data hub allowing for the registration of clients and a public key. The client (often in this model another data hub) retrieves a JWT access token by sending a request (signed with a private key) to authenticate.

In secured mode, it is also possile to configure an OPA endpoint. OPA is used to authorize requests. Authorization is then based on a union of data hub ACL rules and OPA policy rules for the current user.

Additionally, an external authentication provider can be configured to validate JWT tokens. This has to be an OAuth2 provider. The data hub will then validate JWT tokens against both the built-in provider and the external provider.

The following environment variables can be set to configure the data hub security.

`SECURITY_STORAGE_LOCATION=`

SECURITY_STORAGE_LOCATION is used to point to a location on disk where the datahub will generate a key pair to use with Data Hub Security. Note that the data hub must have permissions to create these file on the location.

`NODE_ID=`

NODE_ID is used to give a unique identifier to a running data hub instance. It is needed when regstering this data hub instance as a client to other data hubs. It is the users responsibility to assign unique identifiers.

`ADMIN_USERNAME=`

To boot strap the administration and secure access via client certificates a root admin user is requried. The credentials for this are passed in at start up as environment variables. Depending on the setup these values should come from secrets managers. Required for secured mode.

`ADMIN_PASSWORD=`

This is the password value for the admin user. Required in secured mode. It is highly recommended to ensure that this password is very secure.

`ADMIN_LOCAL_ONLY=false`

If set to true admin access is only available from the local machine / container where the data hub is running. (coming soon)

`AUTHORIZATION_MIDDLEWARE=noop`

By configuring what AUTHORIZATION_MIDDLEWARE to use, secure mode is toggled on or off. The following values are supported:

-   noop - this completely turns Authorization and Authentication off. Use for testing only! Not setting this variable is equivalent to setting it to noop.
-   on - this validates JWT tokens, and uses the Datahub client ACL for authorization.
-   opa - deprecated. replaced by "on"
-   local - deprecated. replaced by "on"

`TOKEN_WELL_KNOWN=https://some-service/.well-known/jwks.json`

This points to an external well-known endpoint for validation of your JWT token.

`TOKEN_AUDIENCE=https://token.audience`

This is the audience the external token is valid for. Make sure the external Oauth2 service creates bearer tokens
for this audience.

`TOKEN_ISSUER=https://token.issuer`

This is the issuer the external token is created with. Make sure the external Oauth2 service creates bearer tokens
with this issuer.

`OPA_ENDPOINT=`

If you are using OPA service, this must point to where your OPA service endpoint is located.

#### Contacting datalayers

Datalayers are themselves secured services, and the Datahub needs access to them.
The datalayers currently support different security mechanisms through the use of login providers.

The default built in provider supports jwt/auth0 through a set of env variables:

`DL_JWT_CLIENT_ID=`

This is the client id supported by the token generator service.

`DL_JWT_CLIENT_SECRET=`

This is the client secret supported by the token generator service.

`DL_JWT_AUDIENCE=`

This is the intended audience for the token, and needs to be supported by the token generator service.

`DL_JWT_GRANT_TYPE=app_credentials`

This is the grant type for the token. Note that this should be a machine token type, however for local testing purposes, other grant types can be used.

`DL_JWT_ENDPOINT=https://auth.example.io/oauth/token`

This is the endpoint that gets called to generate a token. A token is cached for 24hours to prevent saturating this endpoint, so your token must be valid for the same time. Note that client tokens with refresh are not supported.

The payload that is generated is compatible with both Auth0 and Mimiro:

```json
{
    "client_id": "ABCD1234",
    "client_secret": "<super_secret>",
    "audience": "https://api.example.io",
    "grant_type": "app_credentials"
}
```

The first time you load the Datahub, these settings will be added to the list of login providers, and you can work with it through the /providers
endpoint. The default provider will be named "jwttokenprovider", and as long as you keep the env variables listed above, it will be recreated
if deleted.

However, you can add more providers. Currently 2 types of providers are supported, namely basic username/password and auth0 compatible jwt tokens
with id and secret.

### Securing Data Hub with ACLs and Client Certificates

Assuming there are two data hubs and the goal is to have one data hub be able to run a job that accesses a dataset on another.

To register clients and ACLs it is first necessary to log into the data hub with the admin permissions.

To login with admin credentials create a new login alias. Notice the type is 'admin'. The clientId and clientSecret should align with the data hub environment variables ADMIN_USERNAME and ADMIN_PASSWORD.

```
mim login add
    --alias localadmin \
    --type admin
    --server "https://localhost:8080" \
    --audience "https://localhost:8080" \
    --authorizer "https://auth.localhost:8081" \
    --clientId "ADMIN_USERNAME" \
    --clientSecret "ADMIN_PASSWORD" \

```

Then get the client id and public key from the data hub that will be connecting to this data hub. The client-id is the NODE_ID of the data hub that will be a client. The public key can be found based on the SECURITY_STORAGE_LOCATION environment variable of the client data hub. Ensure you only share the public key.

Register the client data hub with the following command:

```
mim client add <client-id> -f clientkey.pub
```

You can list registered clients with:

```
mim client list
```

It will show something like:

```json
{
    "cnode23": {
        "ClientId": "cnode23",
        "PublicKey": "LS0tLS1CRUdJTiBQVUJ .... dHSGNHSDBuSjltVGV1K1J1aXJkWEJxbFAvbXNyTmdzCjBTWXZSbEZvUG1UZk5KZE5nbmNRYkxscHF2U1h4eGdxbi9CT1gxdWhIVFprYUV5WWFtMVBuRzdVM3B5K3h3ancKWU9uc3F2Um5hQnJTOFJuRGU4VHFxR05HOTVjSm5DOEhkSmdNT1Zia09rdEsyYjBPTXlSQ1ozOGg5NG5QUkZBYwpwbzhNcW8xblVUZER0NkRhL3ZvQ1ZLMXU2dHp4UmxIM0RESm9aWll1NFBCMnBGTk94ODZlUG9pdERmTUdZUTlECisyR0tLS0tRU5EIFBVQkxJQyBLRVktLS0tLQo=",
        "Deleted": false
    }
}
```
To add ACLS a json-file can be created from this template:

```json
[
    {
        "Resource":"/datasets/your.Dataset*",
        "Action":"read",
        "Deny":false
    }
]
```
After this file has been created it should be uploaded to the data hub.

```
mim acl add <client-id> -f acls.json
```

Then get, edit and update the ACL for the client:

```
mim acl get <client-id> client23-acl.json
```

The resource patterns are either exact matches or '*' matches. This will match any subpart of the URL and isnt restricted to path segments. e.g. '/datasets/core.*' can be used to secure all datasets starting with 'core.'.

To grant access to, for instance, /changes and /entities on a give dataset, one would add it like so: '/datasets/core.Dataset/*'.

Note the trailing '/' in the line above. Something to have in mind is if both endpoints of the dataset are granted access to with '/datasets/core.Dataset*' there is also potential to grant access to '/datasets/core.DatasetWithAdditionalInfo'.

To grant full access to the client. Add to the ACL file so it looks like:

```json
[{ "Resource": "/*", "Action": "write", "Deny": false }]
```
Other options can look like this

```json
[{"Resource": "/datasets/core.*", "Action": "read", "Deny": false },{"Resource": "/datasets/test.Users*", "Action": "read", "Deny": false }, {"Resource": "/datasets/test.Places/changes", "Action": "read", "Deny": false }]
```

Then upload the config.

```
mim acl add <client-id> -f acls.json
```

On the client data hub it is necessary to upload a provider config that can be referenced from jobs that need to access the remote data hub.

This can be done with the following:

```
mim provider add -f remote-provider.json
```

Or
a POST to /provider/logins

```json
{
    "name": "remote-data-hub-name-provider",
    "type": "nodebearer",
    "endpoint": {
        "type": "text",
        "value": "URL-of-remote-data-hub/security/token"
    },
    "audience": {
        "type": "text",
        "value": "the name (NODE_ID) of the remote data hub you want to read from"
    }
}
```
The name of this provider should be used in the job to specify which one the data hub should use for this job

#### Remote data hub as source config
```json
  "source": {
    "Type": "HttpDatasetSource",
    "Url": "URL-of-remote-data-hub/datasets/some.Dataset/changes",
    "TokenProvider": "remote-data-hub-name-provider"
  },
```

#### Remote data hub as sink config
```json
  "sink": {
    "Type": "HttpDatasetSink",
    "Url": "URL-of-remote-data-hub/datasets/some.Dataset/entities",
    "TokenProvider": "remote-data-hub-name-provider"
  },
```

### Working with security providers

There is an endpoint to work with these, please see the api spec file for details.

Adding a new provider with basic security looks like this:

POST /provider/logins:

```json
{
    "name": "login1",
    "type": "basic",
    "user": {
        "type": "text",
        "value": "server1"
    },
    "password": {
        "type": "env",
        "value": "SERVER1_SECRET_PASSWORD"
    }
}
```

2 different providers are currently supported, "basic", "bearer". Basic means username+password, "bearer" means an auth0 compatible bearer token id and secret.

To prevent leaking of credentials, a ValueReader type has been added, which type supports "text", "env" and "ssm", to read as text, from environment and from AWS SSM respectively.

The name of the provider can then be added to a job through its "TokenProvider" field.

When the provider is used the first time, the values are loaded from their store. Any change in values of the type "env" and "ssm" requires the Datahub to be restarted.

#### Backup

Backup of the Datahub is important. This makes sure you can recover from disaster.

`BACKUP_LOCATION=`

To enable back, a backup location needs to be configured. The Datahub will attempt to create the directories in this location if they are missing.

We do recomment that you put the backup on a separate disk from the STORE_LOCATION for performance reasons. If you are in a Cloud setup, you should probably use something like AWS ELB to make sure your disk survive a shutdown.

`BACKUP_SCHEDULE=`

The backup gets scheduled in the internal Job runner in the Datahub, and the schedule supports the same cron schedules as regular Jobs. You can find the documentation [here](https://pkg.go.dev/github.com/robfig/cron#hdr-CRON_Expression_Format).

If you don't provide a schedule, the default schedule is "_/5 _ \* \* \*", aka every 5 minutes.

`BACKUP_USE_RSYNC=true`

If this is true, then the backup will use rsync for it's backup. rsync must be installed, and on the path for this to work.
If this is false, the Badger DB native backup will be used instead.


#### Logging profile

Logging is a little bit special, in the fact that we need to set it up earlier than we read the configuration variables.

Therefore, this profile can only be set as an env variable

`export PROFILE=local`

The valid profiles are:

-   test - this turns logging off when running tests
-   local - makes logs be readable in a console, and sets the zap logger for Development
-   other - changes logs to json format, and sets the logger for Production

The "local" profile is suitable for production use, if you don't need the json formatting.

### Configuration by file

The configuration file supports the same variables as the env variables. It should be a flat file with properties in all upper case. A template file ".env.tpl" is supplied with the source code, and should be adapted and renamed.

#### Config file locations

1. You can start the server with a "--config=/path/to/file/my-conf". It uses the file at the given location.
2. You can start the server with a "--config=/path/to/file/". If it ends with a "/", the server will look for a file named ".env".
3. Add a ".env" file to a "$HOME/.datahub/" directory.

The 3rd option is the recommended option for running locally, and you can use the --config option to quickly switch between different configurations.

## Security

### JWT

The Datahub assumes you want to secure it using JWT tokens in combination with an "Authorization" header, carrying a "Bearer <token>" with a valid JWT token.

What a valid token is, depends on your setup, however, we are strict in our interpretation of what that means.

Partial token payload:

```json
{
    "aud": "https://example.mimiro.io",
    "exp": 1615468468,
    "iat": 1615382068,
    "iss": "https://example.mimiro.io"
}
```

In general we expect the fields "aud", "exp", "iat" and "iss" to be filled out and correct. We also expect the KID header to match an x509 public certificate found in the well-known endpoint.

If you are using JWT for Authentication and Authorization, we expect a list of "scopes": [], with either "datahub:r" and/or "datahub:w" to be present for read and write operations respectively. For this to be valid, the "gty" field of the token must equal "client-credentials", signaling a machine token.

For any other type of token, we expect the "adm" field to be set to true.

### OPA

For more advanced authorization scenarios, we support [OPA](https://www.openpolicyagent.org/). Open Policy Agent is a generic framework for authorizing users or services.

For OPA to work, you must implement 2 functions on the OPA server:

-   /v1/data/datahub/authz/allow
-   /v1/data/datahub/authz/datasets

The allow function must return a json reponse in this format:

```json
{
    "result": true
}
```

Valid result is true for valid, false if not.

In case of true, the second function is called to get a list of valid datasets for this user.

This function will return a list of datasets:

```json
{
    "result": ["dataset1", "dataset2"]
}
```

A special result with ["*"] allows access to all datasets.

A payload with the following data is sent to the OPA service with the request:

```json
{
    "method": "GET/POST",
    "path": "/dataset/<the.dataset>/changes",
    "token": "Raw jwt-token",
    "scopes": "Token scopes if present"
}
```

Note that the backing implementation of the OPA ruleset is outside of the scope of this documentation, and is in practice up to the OPA service maintainer in your organization. However the functions and their return values are not optional, and must confirm.


## Maintenance

### statistics

Datahub has a statistics endpoint that can be used to monitor the size of datasets in the Datahub.
It is available at `/statistics`. In the list of datasets, the sizes are shown in bytes. There is one
generated dataset entry called `all`, which is the sum of all datasets.

It is also possible to narrow down statistics to a specific dataset by adding the dataset name to the URL, e.g. `/statistics/people`.

The `mim` CLI also has a command to get statistics for a dataset:

```shell
mim stats list
```
or to get the largest datasets in different storage categories:
```shell
mim stats top
```

Note that the statistics are not real-time, but are updated regularly in a background process.

### Compaction

Sometimes datasets accumulate a lot of historic data that is no longer needed.
This can be removed by compacting the dataset. The compaction process can be controlled by applying different
compaction strategies.

Currently, only one compaction strategy is supported, `deduplicate`. This strategy will compact the dataset by removing all
successive duplicate versions of each entity in a dataset, while retaining timeline correctness.
Entities are considered duplicates if they have the same ID, deletion status, properties, and the same references.
The `deduplicate` strategy will also compact reference indexes, even when entity versions otherwise are not duplicates.
For example, if all versions of an entity have the same rdf type reference, the reference index will only contain that information once after compaction.

To trigger a compaction. `POST` to the endpoint `/compaction` with the following payload:

```json
{
    "dataset": "people",
    "strategy": "deduplicate"
}
```
The request will return immediately, while the compaction process will run in the background. Follow logs to see compaction progress.

Note that disk space is not freed up immediately after compaction. For a time, the space is kept allocated for reuse, but
eventually the disk space may be released.
