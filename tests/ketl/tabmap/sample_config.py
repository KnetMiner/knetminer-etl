"""
A sample/test config of tab mappers, to be used in an ETL workflow.
See tests/resources/tabmap-test.snakefile

"""
import ketl.helpers as khelpers
import ketl.tabmap.helpers as tbhelpers
from ketl.core import ConstantTripleMapper
from ketl.tabmap.core import (ColumnTripleMapper, ColumnValueMapper,
                              TabFileMapper)

PROTEINS_MAPPER = TabFileMapper (
	id_mapper = ColumnValueMapper ( column_id = "accession" ),	
	mapper_components = [
		ColumnTripleMapper ( column_id = "name", property = "hasProteinName" ),
		ColumnTripleMapper ( "accession", "hasAccession" ),
		khelpers.type_triple_mapper ( "Protein" ),
		ConstantTripleMapper ( property = "source", constant_value = "SnakeTest" )
	],
	spark_options = { "inferSchema": False }
)

ENCODING_MAPPER = TabFileMapper (
	id_mapper = None, # Auto-generated from relation type and endpoint IDs
	mapper_components = [
		tbhelpers.edge_source_row_triple_mapper ( "gene accession" ),
		tbhelpers.edge_target_row_triple_mapper ( "accession" ),
		ColumnTripleMapper ( column_id = "link notes" ),
		# The PG relationship type
		khelpers.type_triple_mapper ( "encodes-protein" ),
		ConstantTripleMapper ( property = "source", constant_value = "SnakeTest" )
	],
	spark_options = { "inferSchema": False }	
)
