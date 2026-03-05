# KnetMiner ETL Tools (KETL)

See the the [main README](../README.md) for an intro to the project.

At the moment, the KnetMiner ETL tools has the following components.

* [Core graph triple representation](core-graph.md)
* [Tabular mappers](tabmap.md): mappers for tabular data, such as CSV/TSV files.
* [A Spark Dataframe-to-PG-JSONL mapper](PG-JSONL.md), which dumps a built knowledge graph into file(s) in the PG-JSONL property graph format, ready to be loaded into Neo4j.
* [SnakeMake helpers and examples](snakemake.md), showing how to use KETL tools to compose ETL pipelines that are progressive and can work on distributed systems, such as HPC clusters or clouds.
 