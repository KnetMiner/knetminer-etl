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


# Grand Restructuring of 2026-05

## `ValueConverter`
* [X] pre-serialisers need to be ~~separated~~ removed:
	* [X] introduce the `from_fun()` helpers (see below)
	* [X] introduce `with_value_wrapper( fun ) -> ValueMapper` in `ValueMapper`
		* [ ] to be tested
* [ ] Serialisation applies only to node/edge properties, there is no need for it with triple keys like 'ID', 'TYPE', 'FROM', 'TO'.
* [ ] Serialisation **is not** related to value mappers (eg, column mappers), since it's about all data types and hence it is to be linked to aggregate mappers, such as `SparkDataFrameMapper`.
**Even better**: introduce a configurable global value converter.

## `Mapper`
* [ ] The converter becomes a param of `value()`, the aggregate mappers pass down the configured converter when they need to map a custom element property, not special triple keys (id, type, etc)
	* [ ] thus, Id mappers must go away, they're value mappers with special role, and with no serialisation involved.
	* We considered using `with_value_wrapper()` to equip a mapper with a converter, but this conflates
	concerns and worse, makes a stateful change to the mapper. If the same mapper is used both for saving a custom property and something like a node ID, the latter means looking for troubles.
* [X] rename to `ValueMapper`
* [X] Various properties should become fluent, ie, `withXXX( x )`

## `ConstantPropertyMapper`
* [X] Rename to `ConstantTripleMapper`

## `RowValueMapper`

## `RowTripleMapperMixin`
* [X] Rename to `RowTripleMapper`. It's too complicated to be considered a mixin.

## `IdColumnMapper`
* [ ] Remove it. As said above, With the new design, this is just a `RowValueMapper` (including a `ColumnValueMapper`) playing the role of ID mapper (assigned to the `id_mapper` field of `SparkDataFrameMapper`).

## `ColumnMapper`
* [X] Rename to `ColumnTripleMapper`
* [X] Add `RowValueMapper.to_triple_mapper()` and `ColumnValueMapper.to_triple_mapper()` as a specialisation of the former (which deals with default property ID = column ID and returns a more specific `ColumnTripleMapper`).


## Build functions

* [X] `type_triple_mapper (...)`: makes a `ConstantTripleMapper` for the type label
* [X] `row_value_mapper (...)`
* [X] `row_triple_mapper (...)`
* [X] `edge_id_row_value_mapper(...)`
* [X] `edge_auto_id_row_value_mapper(...)`
* [X] `edge_source_row_triple_mapper(...)`: makes a `RowTripleMapper`
* [X] `edge_target_row_triple_mapper(...)`: makes a `RowTripleMapper`
* [X] `edge_source_column_triple_mapper(...)`: makes a `ColumnTripleMapper`, uses 1 col only
* [X] `edge_target_column_triple_mapper(...)`: makes a `ColumnTripleMapper`, uses 1 col only

## `SparkDataFrameMapper`
* [ ] Simplify the constructor by removing the separation between `row_mappers` and `const_prop_mappers`, it is able to split an initial mixed list.
* [ ] Add a `use_column_ids` flag = False. The mapper's column_ids are only used if this is set (and their presence is enforced in that case), else use all the DF columns. Document that the latter can be inefficient for wide tables, but given that's not usually the case, it's a good default.
* [ ] Must take control of when to apply the configured converter.

## `triples_2_pg_df()`
* [ ] see if we can remove `triples_type` and instead we can build a mixed result of nodes and edges (by playing with Spark SQL, eg, `CASE WHEN`).
