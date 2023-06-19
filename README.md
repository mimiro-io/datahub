# MIMIRO Data Hub

[![Docker Image CI](https://github.com/mimiro-io/datahub/actions/workflows/docker-image.yml/badge.svg)](https://github.com/mimiro-io/datahub/actions/workflows/docker-image.yml)

The MIMIRO data hub is a semantic, entity graph database combined with data integration capabilities and a jobs engine for data transformation.

The data hub stores and manages datasets. Each dataset contains entities. Entities have identity, properties and references to other entities. All identities, property types and reference types are URIs to enable meaningful semantic interchange and support for schemaless, open world data models.

The data hub exposes an API that can be used to create and populate datasets directly. It is also possible to configure and schedule jobs that load data into the data hub from remote data layers.

Jobs with javascript transformations can be used to process entities to create new datasets or send data to external, receiving data layers.

The data hub and external data layers implement the MIMIRO Universal Data API (UDA) specification https://open.mimiro.io/specifications.

The [change log](CHANGELOG.md) is kept fairly up-to-date.

## Project Status
This software release is just a part of the [MIMIRO OPEN](https://open.mimiro.io) activity. We aim to provide open specifications, open source, open data, and best practice to help improve data exchange in domains such as agriculture.

We follow semantic versioning and are pre 1.0. This means that the API or binary representation could change. However, we are actively using the data hub in a production setting at MIMIRO for both internal and external data integration use cases.

## Table of Contents
  * [Getting Started](#getting-started)
  * [Documentation](#documentation)
  * [Contributing](#contributing)
  * [Contact](https://en.mimiro.no/#contact)

# Getting Started

MIMIRO data hub can be built and run from source or run as docker container. Building from source is currently only supported on linux and mac osx based systems. On Windows we recommended using WSL2 for building and running from source.

## Building

To build the MIMIRO data hub install Go 1.16 or above, and then invoke the makefile as described below.

```bash
make build
```

For Windows users, we recommend installing [WSL](https://docs.microsoft.com/en-us/windows/wsl/install-win10), and setting up the environment there.

## Running in Docker

[Docker images](https://hub.docker.com/repository/docker/mimiro/datahub) are built and released to docker hub. They can also be built locally with the Dockerfile.

To build a local docker image:

```
docker build -t local/mimiro-datahub .
```

To run the data hub in docker:

```
docker run -d --name mimiro-datahub -p 8080:8080 local/mimiro-datahub
```

Something to note if running the datahub in docker is that by default docker does not have persistent storage of the data in the created container. This means that if the docker container is shut down your data saved in the datahub will dissapear. To make sure that your data persists you will need to define a persisting docker volume on disk. Read more on how to set up [docker volumes](https://docs.docker.com/storage/volumes/).

## Running the Binary

The MIMIRO data hub is built as a single binary. It can be run in the following ways:

```bash
make run
```

or

```
./bin/datahub-server
```

# Documentation

The [documentation](DOCUMENTATION.md) is aimed at people looking to use MIMIRO for data integration and as a graph database. It provides guidance on how to configure and use the data hub via its API. This includes loading data, querying, executing jobs and data transformation.

# Contributing

Before contributing please read our [Code of Conduct](CODE-OF-CONDUCT.md).

Our guide to [contributing](CONTRIBUTING.md) outlines other aspects of how to contribute to the MIMIRO data hub.





