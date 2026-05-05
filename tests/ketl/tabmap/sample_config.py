"""
A sample/test config of tab mappers, to be used in an ETL workflow.
See tests/resources/tabmap-test.snakefile

"""
from ketl.core import ConstantTripleMapper
from ketl.tabmap.core import ColumnTripleMapper, IdColumnValueMapper, TabFileMapper

PROTEINS_MAPPER = TabFileMapper (
	id_mapper = IdColumnValueMapper ( column_id = "accession" ),	
	row_mappers = [
		ColumnTripleMapper ( column_id = "name", property = "hasProteinName" ),
		ColumnTripleMapper ( "accession", "hasAccession" ),
	],
	const_prop_mappers = [
		ConstantTripleMapper.for_type ( "Protein" ),
		ConstantTripleMapper ( property = "source", constant_value = "SnakeTest" )
	],
	spark_options = { "inferSchema": False }
)

ENCODING_MAPPER = TabFileMapper (
	id_mapper = None, # Auto-generated from relation type and endpoint IDs
	row_mappers = [
		ColumnTripleMapper.for_from ( column_id = "gene accession" ),
		ColumnTripleMapper.for_to ( column_id = "accession" ),
		ColumnTripleMapper ( column_id = "link notes" )
	],
	const_prop_mappers = [
		# The PG relationship type
		ConstantTripleMapper.for_type ( "encodes-protein" ),
		ConstantTripleMapper ( property = "source", constant_value = "SnakeTest" )
	],
	spark_options = { "inferSchema": False }	
)
