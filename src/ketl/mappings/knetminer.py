"""
Mappers and helpers for common types used in KnetMiner.
"""

from ketl.tabmap.core import ColumnValueMapper, RowTripleMapper, RowValueMapper, SparkDataFrameMapper, SparkDataFrameMapperBase
import ketl.tabmap.helpers as tbhelpers

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


# TODO: remove, it's the old version that creates a separated node
def _create_accession_tabmapper ( 
	acc_source_mapper: str|RowValueMapper, acc_mapper: str|RowValueMapper,
	source_id_mapper: RowValueMapper
) -> tuple[SparkDataFrameMapper, SparkDataFrameMapper]:	
	"""
	Makes a pair of :class:`SparkDataFrameMapper`(s), one that maps accession-related data to
	an Accession node, the other that maps to the relationship linking
	the owner node to the accession.

	## Parameters
	- `acc_source_mapper`: the mapper for the accession source. If it's a string, it will be the 
	  column ID where to take the source value from. If it's a :class:`RowValueMapper`, 
		it will use the mapper to gather the source value. If it's a string with the '!' prefix
		(eg, "!ENSEMBL"), it will be used as a constant value for the source.
	- `acc_mapper`: the mapper for the accession. Similarly to `acc_source_mapper`, it can 
		be a string (column ID), a :class:`RowValueMapper`, or a constant value with the '!' prefix.
	- `source_id_mapper`: the ID mapper of the source node, to be used to create relationships that 
	  link the accession nodes to their respective source nodes.

	## Returns
	A tuple of two :class:`SparkDataFrameMapper`(s), the first creates Accession Nodes, the second
	creates linking triples between values returned by `source_id_mapper` and accessions.
	"""

# TODO: remove, it's the old version that creates a separated node
def _add_accession_tabmapper ( 
	df_mapper: SparkDataFrameMapper, 
	acc_source_mapper: str|RowValueMapper, acc_mapper: str|RowValueMapper,
	source_id_mapper: RowValueMapper
) -> SparkDataFrameMapperBase:
	"""
	Helper to add accession mapping to an existing :class:`SparkDataFrameMapper`.

	## Returns

	A :class:`SparkDataFrameMapperBase` that joins the original tab mapper with the two 
	accession-related mappers obtained by :func:`_create_accession_tabmapper()`.
	"""
	acc_tbmap, acc_link_tbmap = _create_accession_tabmapper ( acc_source_mapper, acc_mapper, source_id_mapper )
	all_mappers = tbhelpers.df_mappers_chain ( df_mapper, acc_tbmap, acc_link_tbmap )
	return all_mappers
