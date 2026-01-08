"""
A sample/test config of tab mappers, to be used in an ETL workflow.
See tests/resources/tabmap-test.snakefile

"""
from ketl.tabmap import TabFileMapper, IdColumnMapper, ColumnMapper, RowValueMapper
from ketl import ConstantPropertyMapper
from pyspark.sql.types import IntegerType

PROTEINS_MAPPER = TabFileMapper (
	id_mapper = IdColumnMapper ( column_id = "accession" ),	
	row_mappers = [
		ColumnMapper ( column_id = "name", property = "hasProteinName" ),
		ColumnMapper ( "accession", "hasAccession" ),
	],
	const_prop_mappers = [
		ConstantPropertyMapper.for_type ( "Protein" ),
		ConstantPropertyMapper ( property = "source", constant_value = "SnakeTest" )
	],
	spark_options = { "inferSchema": False }
)

ENCODING_MAPPER = TabFileMapper (
	id_mapper = None, # Auto-generated from relation type and endpoint IDs
	row_mappers = [
		ColumnMapper.for_from ( column_id = "gene accession" ),
		ColumnMapper.for_to ( column_id = "accession" ),
		ColumnMapper ( column_id = "link notes" )
	],
	const_prop_mappers = [
		# The PG relationship type
		ConstantPropertyMapper.for_type ( "encodes-protein" ),
		ConstantPropertyMapper ( property = "source", constant_value = "SnakeTest" )
	],
	spark_options = { "inferSchema": False }	
)
