* [X] Mark relevant tests as integration tests, via `@pytest.mark.integration`
* [ ] Neo4j loader
* [ ] Migrate from AgriSchemas ETL utils
* [ ] Factorise the root module?
* [X] CI
* [X] isort and other code quality tools

## NeoLoader
* [ ] More logs
* [X] Edges
* [ ] Actual batching (and performance)
* [X] Singleton->single values, not lists
* [X] Use value converters?
	* No, see the note in `async_pg_jsonl_neo_loader()`
* [X] Nullable properties
* [X] Multiple labels
* [X] Move from io to its own module
* [ ] Add CLI wrapper
* [ ] Loading in Snake workflow
* [X] Get rid of neo warnings
* [ ] Neo retries (in edge creation)
* [ ] done flag (for Snakemake)
