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


# Grand Review of 2026-05

## `ValueConverter`
* [ ] pre-serialisers need to be ~~separated~~ removed:
	* [ ] introduce the `from_fun()` helpers (see below)
	* [ ] introduce `with_value_filter( fun ) -> ValueMapper` in `ValueMapper`
* [ ] Serialisation applies only to node/edge properties, there is no need for it with triple keys like 'ID', 'TYPE', 'FROM', 'TO'.

## `Mapper`
* [ ] it keeps the converter, but aggregate mappers must set their component converters when they're none
  * TODO: ID/regular mappers?
* [X] rename to `ValueMapper`
* [ ] Various properties should become fluent, ie, `withXXX( x )`

## `ConstantPropertyMapper`
* [X] Rename to `ConstantTripleMapper`

## `RowValueMapper`

## `RowTripleMapperMixin`
* [X] Rename to `RowTripleMapper`. It's too complicated to be considered a mixin.

## `IdColumnMapper`
* [X] Rename to `IdColumnValueMapper`

## `ColumnMapper`
* [ ] Rename to `ColumnTripleMapper`

## Build functions

* [ ] `type_triple_mapper (...)`: makes a `ConstantTripleMapper` for the type label
* [ ] `row_value_mapper_from_fun (...)`
* [ ] `row_triple_mapper_from_fun (...)`
* [ ] `id_column_value_mapper_from_fun (...)`
* [ ] `auto_edge_id_row_value_mapper(...)`
* [ ] `edge_id_row_value_mapper(...)`
* [ ] `edge_from_row_triple_mapper(...)`: makes a `RowTripleMapper`
* [ ] `edge_to_row_triple_mapper(...)`: makes a `RowTripleMapper`
* [ ] `edge_id_row_value_mapper(...)`
* [ ] `node_id_column_value_mapper(...)`
* [ ] `edge_from_column_triple_mapper(...)`: makes a `ColumnTripleMapper`, uses 1 col only
* [ ] `edge_to_column_triple_mapper(...)`: makes a `ColumnTripleMapper`, uses 1 col only

## `SparkDataFrameMapper`
* [ ] Simplify the constructor by removing the separation between `row_mappers` and `const_prop_mappers`, it is able to split an initial mixed list.
* [ ] Use the mapper's column_ids only if present in at least one, else use all the DF columns. Document that the latter can be inefficient for wide tables, but given that's not usually the case, it's a good default.

## `triples_2_pg_df()`
* [ ] see if we can remove `triples_type` and instead we can build a mixed result of nodes and edges (by playing with Spark SQL, eg, `CASE WHEN`).
