"""
Helpers used to build tabular source mappers and related entities.
"""

from collections.abc import Callable
from typing import Any

from ketl.core import ConstantTripleMapper, GraphTriple, ValueConverter, ValueMapper
import ketl.helpers as khelpers
from ketl.tabmap.core import ColumnValueMapper, RowTripleMapper, RowValueMapper

from pyspark.sql.types import DataType


def row_value_mapper ( 
	fun: Callable [ [ dict[ str, Any ] ], Any|None ]
) -> RowValueMapper:
	"""
	Builds a :class:`ketl.tabmap.RowValueMapper` from a function that extracts a value from a row dictionary.

	The function `fun` follows the contract of the :meth:`ketl.tabmap.RowValueMapper.value` method, ie, it 
	receives a non-empty row dictionary and None results are ignored.

	Similarly, the other parameters are passed to the constructor of :class:`ketl.tabmap.RowValueMapper`.
	"""
	class FunRowValueMapper ( RowValueMapper ):
		def __init__ ( self ):
			super().__init__ ()

		def value ( self, row_dict: dict [ str, Any ], converter: ValueConverter = None ) -> Any | None:
			value = fun ( row_dict )
			if converter: value = converter.convert ( value )
			return value
	
	return FunRowValueMapper ()


def row_triple_mapper (
	fun: Callable [ [ dict[ str, Any ] ], Any|None ],
	property: str
) -> RowTripleMapper:
	"""
	A simple helper that chains :func:`row_value_mapper` and :meth:`ketl.tabmap.RowValueMapper.to_triple_mapper`.
	"""
	return row_value_mapper ( fun ).to_triple_mapper ( property )


def edge_source_row_triple_mapper ( 
	extractor: Callable [ [ dict[ str, Any ] ], Any | None ] | RowValueMapper | str
) -> RowTripleMapper:
	"""
	Helper to build a :class:`ketl.tabmap.RowTripleMapper` for an edge source (ie, for a triple with the 
	:attr:`GraphTriple.FROM_KEY` property).

	The extractor can be a function to pick a value from a row dictionary, an existing row value mapper
	to be extended with a 'from'-related triple mapper, or a string that represents a column ID and 
	is used to return a :class:`ketl.tabmap.ColumnValueMapper`-based triple mapper.
	"""
	row_val_map = None
	if isinstance ( extractor, RowValueMapper ):
		row_val_map = extractor
	elif isinstance ( extractor, str ):
		row_val_map = ColumnValueMapper ( column_id = extractor )
	else:
		row_val_map = row_value_mapper ( fun = extractor )

	return row_val_map.to_triple_mapper ( property = GraphTriple.FROM_KEY )


def edge_target_row_triple_mapper (
	extractor: Callable [ [ dict[ str, Any ] ], Any | None ] | RowValueMapper | str
) -> RowTripleMapper:
	"""
	Helper to build a :class:`ketl.tabmap.RowTripleMapper` for an edge target (ie, for a triple with the 
	:attr:`GraphTriple.TO_KEY` property).

	This works the same as :func:`edge_source_row_triple_mapper`.
	"""
	row_val_map = None
	if isinstance ( extractor, RowValueMapper ):
		row_val_map = extractor
	elif isinstance ( extractor, str ):
		row_val_map = ColumnValueMapper ( column_id = extractor )
	else:
		row_val_map = row_value_mapper ( fun = extractor )

	return row_val_map.to_triple_mapper ( property = GraphTriple.TO_KEY )


def edge_id_row_value_mapper (
	type_id: str,
	from_column_id: str,
	to_column_id: str,
) -> RowValueMapper:
	"""
	Builds an edge ID mapper based on two columns reporting the edge endpoints.
	"""
	def extractor ( row: dict [ str, Any ] ) -> str:
		from_id, to_id = row.get ( from_column_id ), row.get ( to_column_id )
		if not from_id or not to_id:
			raise ValueError ( f"Empty value in edge from/to column '{from_column_id}', '{to_column_id}'" )
		return edge_id ( type_id, from_id, to_id )
	
	return row_value_mapper ( extractor )\
		.with_column_ids ( [ from_column_id, to_column_id ] )
		

def edge_auto_id_row_value_mapper (
	property_mappers: list[ ConstantTripleMapper | RowValueMapper ]
) -> RowValueMapper:
	"""
	Helper to build edge IDs by searching :attr:`GraphTriple.TYPE_KEY`, :attr:`GraphTriple.FROM_KEY` and :attr:`GraphTriple.TO_KEY` 
	into the provided property mappers and using these mappers to build the edge ID. The values provided
	by these mappers are used with :func:`edge_id`.
	"""
	def find_mapper ( prop_key: str ) -> ValueMapper:
		"""
		Finds a mapper by property key
		"""
		mapper = next ( ( pm for pm in property_mappers if pm.property == prop_key ), None )
		if not mapper:
			raise ValueError ( f"edge_auto_id_row_value_mapper(): can't find property mapper for '{prop_key}'" )
		return mapper

	def extractor ( 
		mapper: ConstantTripleMapper | RowValueMapper, row: dict [ str, Any ]
	) -> str:
		"""
		Extracts an ID portion based on its mapper
		"""
		if isinstance ( mapper, ConstantTripleMapper ):
			return mapper.value ()
		elif isinstance ( mapper, ValueMapper ):
			return mapper.value ( row )
		raise ValueError ( f"edge_auto_id_row_value_mapper(): unsupported mapper type {type(mapper)}" )

  # Main
	#

	if not property_mappers:
		raise ValueError ( "edge_auto_id_row_value_mapper(): no property mappers given" )
	
	# Get the ID component mappers
	(type_map, from_map, to_map) = (
		find_mapper ( GraphTriple.TYPE_KEY ),
		find_mapper ( GraphTriple.FROM_KEY ),
		find_mapper ( GraphTriple.TO_KEY )
	)

	# Add them if available from the underlying mappers.
	col_ids = [ 
		col for pm in ( type_map, from_map, to_map ) if isinstance ( pm, RowValueMapper )
		for col in pm.column_ids
	]

	# Component extractors to be used by the final extractor to compose the edge ID
	(type_extractor, from_extractor, to_extractor) = (
		lambda row: extractor ( type_map, row ),
		lambda row: extractor ( from_map, row ),
		lambda row: extractor ( to_map, row )
	)

	# Here it is
	return row_value_mapper (
		fun = lambda row: edge_id ( type_extractor ( row ), from_extractor ( row ), to_extractor ( row ) )
	).with_column_ids ( col_ids )


def edge_id ( type_id: str, from_id: str, to_id: str ) -> str:
	"""
	Simple helper to build a common edge ID from a composite key, eg, `encodesProtein:GENE001-PROTEIN001`. 
	"""
	if not ( type_id and from_id and to_id ):
		raise ValueError ( f"Cannot build edge ID from empty type/from/to IDs" )
	return f"{type_id}:{from_id}-{to_id}"
