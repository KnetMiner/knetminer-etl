## Tests and examples
* [ ] Document RRes and local examples, write a tutorial

## Real Use cases

Next steps for the KnetMiner base pipeline:
1. [ ] Publish the new ETL to pypi
1. [ ] Create a new project knetminer-schema-etl in knetminer-schemas, which offers helpers for generic KnetMiner mappings (applicable to similar datasets). Extends knetminer-etl.
1. [ ] Create a new knetminer-pilot-etl project, which extends knetminer-schema-etl and implements the real use case.
  1. [ ] I don't know if there should be one repo per dataset, or one giant repo for all datasets. To be discussed.

## General
* [X] Mark relevant tests as integration tests, via `@pytest.mark.integration`
* [ ] Neo4j loader
* [ ] Migrate from AgriSchemas ETL utils
* [ ] Factorise the root module?
* [X] CI
* [X] isort and other code quality tools
* [X] Some more module renaming, eg, test_ketl -> test_core
* [ ] Compression options: output of `pg_df_2_pg_jsonl()`, input of `NeoLoader`

## NeoLoader
* [X] More logs
* [X] Edges
* [ ] Actual batching (and performance)
* [X] Singleton->single values, not lists
* [X] Use value converters?
	* No, see the note in `async_pg_jsonl_neo_loader()`
* [X] Nullable properties
* [X] Multiple labels
* [X] Move from io to its own module
* [X] Add CLI wrapper
	* [ ] Add a Bash wrapper?
* [X] Loading in Snake workflow
* [X] Configuration
* [X] Get rid of neo warnings
* [X] Neo retries (in edge creation)
* [X] done flag (for Snakemake)
* [X] Performance: add a common label to all the nodes and index it on `id` before edge loading


# Grand Restructuring of 2026-05

* [X] See [#2](https://github.com/KnetMiner/knetminer-etl/issues/2).
* [X] See other TODOs in the real test case
	* [X] Unit test for it
