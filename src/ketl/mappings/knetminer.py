"""
Mappers and helpers for common types used in KnetMiner.
"""

from enum import StrEnum

from ketl.core import ConstantTripleMapper
from ketl.tabmap.core import ColumnValueMapper, RowTripleMapper, RowValueMapper, SparkDataFrameMapper, SparkDataFrameMapperBase
import ketl.tabmap.helpers as tbhelpers

from functools import cache

def create_accession_tabmapper (
	acc_source_mapper: str|RowValueMapper, acc_mapper: str|RowValueMapper
) -> RowTripleMapper:
	"""
	Creates a property triple mapper that creates the accession property for a node, ie, 
	something like: `$nodeId accession "ENSEMBL:ACC001"`

	## Parameters
	- `acc_source_mapper`: the mapper for the accession source. If it's a string, it will be the 
	  column ID where to take the source value from. If it's a :class:`RowValueMapper`, 
		it will use the mapper to gather the source value. If it's a string with the '!' prefix
		(eg, "!ENSEMBL"), it will be used as a constant value for the source.
	- `acc_mapper`: the mapper for the accession. Similarly to `acc_source_mapper`, it can 
		be a string (column ID), a :class:`RowValueMapper`, or a constant value with the '!' prefix.

	# Returns
	A :class:`ketl.tabmap.core.RowTripleMapper` that creates the 'accessions' property for a node.
	"""
	if isinstance ( acc_source_mapper, str ):
		if acc_source_mapper.startswith ( "!" ):
			# The extractor will get a constant from this
			acc_source_mapper = acc_source_mapper [ 1: ]
		else:
			acc_source_mapper = ColumnValueMapper ( column_id = acc_source_mapper )
	elif not isinstance ( acc_source_mapper, RowValueMapper ):
		raise ValueError ( f"create_accession_tabmapper(): acc_source_mapper has invalid type {type(acc_source_mapper)}" )
	
	# The same for the accession value
	if isinstance ( acc_mapper, str ):
		if acc_mapper.startswith ( "!" ):
			acc_mapper = acc_mapper [ 1: ]
		else:
			acc_mapper = ColumnValueMapper ( column_id = acc_mapper )
	elif not isinstance ( acc_mapper, RowValueMapper ):
		raise ValueError ( f"create_accession_tabmapper(): acc_mapper has invalid type {type(acc_mapper)}" )
	
	def acc_extractor ( row: dict[str, str] ) -> str | None:
		acc_source = acc_source_mapper if isinstance ( acc_source_mapper, str ) \
			else acc_source_mapper.value ( row )
		acc = acc_mapper if isinstance ( acc_mapper, str ) \
			else acc_mapper.value ( row )
		if not ( acc_source and acc ): return None
		return f"{acc_source}:{acc}"

	return tbhelpers.row_triple_mapper ( acc_extractor, "accessions" )


@cache
def data_source_triple_mapper ( data_source: str ) -> ConstantTripleMapper:
	"""
	Creates a property triple mapper that creates the dataSource property for a node, ie, 
	something like: `$nodeId dataSources "ENSEMBL"`

	Returned values are cached.

	TODO: introduce this in tests and examples.

	"""
	return ConstantTripleMapper ( property = "dataSources", constant_value = data_source )


class NCBITaxIDs ( StrEnum ):
	"""
	Enum for the NCBI Taxonomy IDs of the species used in KnetMiner.
	"""
	ARABIDOPSIS = "3702"
	WHEAT = "4565"


@cache
def tax_id_triple_mapper ( tax_id: str ) -> ConstantTripleMapper:
	"""
	Creates a constant triple mapper that creates the taxId property for a node, ie, 
	something like: `$nodeId taxId "3702"`

	Returned values are cached.

	See :class:`NCBITaxIDs` for the IDs of the species commonly used in KnetMiner.
	"""
	return ConstantTripleMapper ( property = "taxId", constant_value = tax_id )
