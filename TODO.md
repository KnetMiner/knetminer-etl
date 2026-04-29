* [X] Mark relevant tests as integration tests, via `@pytest.mark.integration`
* [ ] Neo4j loader
* [ ] Migrate from AgriSchemas ETL utils
* [ ] Factorise the root module?
* [X] CI
* [X] isort and other code quality tools
* [ ] Some more module renaming, eg, test_ketl -> test_core
* [ ] Compression options: output of pg_df_2_pg_jsonl(), input of NeoLoader

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
* [ ] Loading in Snake workflow
* [X] Configuration
* [X] Get rid of neo warnings
* [X] Neo retries (in edge creation)
* [X] done flag (for Snakemake)
* [ ] Performance: add a common label to all the nodes and index it on `id` before edge loading
