# KnetMiner ETL Tools

KnetMiner ETL tools for Knowledge Graph building.

In this project, you'll find a set of tools that we (will) use to build the KnetMiner knowledge graphs, in the form of Neo4j databases.

The project provides with helpers to transform and map various data source formats (eg, CSV, JSON, RDF) into labelled property graphs (LPGs).

This is based on producing [PG-JSONL](https://pg-format.github.io/specification/#pg-jsonl), ready to be loaded into Neo4j, using our own loader (to be developed).

In turn, we use an intermediate representation, consisting of [graph triples](src/ketl/__init__.py), which are flexible with operations such as merging or filtering.

We use [Spark](https://spark.apache.org/) and [SnakeMake](https://snakemake.readthedocs.io/en/stable/) to support most of the ETL tasks.

TODO: write much more! :-)
