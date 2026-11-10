"""
Helpers used to build mappers and other entities from the KETL core.
"""

from collections.abc import Callable
from functools import cache
from typing import Any

import pandas as pd

from ketl.core import (ConstantTripleMapper, GraphTriple, PropertyMapperMixin,
                       ValueConverter)

@cache
def type_triple_mapper ( type_value: str ) -> ConstantTripleMapper:
	"""
	Helper to build a :class:`ketl.ConstantTripleMapper` for the :py:attr:`ketl.GraphTriple.TYPE_KEY` 
	property, that is, for a node/edge label/type.

	The returned mapper is cached.
	"""
	return ConstantTripleMapper ( property = GraphTriple.TYPE_KEY, constant_value = type_value )


def string_value_wrapper ( prefix: str = "", postfix: str = "" ) -> Callable[ [dict[str, Any]], str|None ]:
	"""
	Helper to build a value filter that adds a prefix to a value extracted by a :class:`ketl.core.ValueMapper`.
	The filter also converts non-string values to strings, and discards None or empty values (by returning None).
	"""
	def wrapper ( value: Any ) -> str | None:
		if value is None: return None
		if not isinstance ( value, str ):
			value = str ( value )
		if not value: return None
		return prefix + value + postfix
	
	return wrapper

def converter_if_needed ( property: str | PropertyMapperMixin, converter: ValueConverter|None = None ) -> ValueConverter | None:
	"""
	Helper to return a converter for property keys that aren't special keys (eg, ID) and are likely
	to be used with custom node/edge properties.

	If the converter is omitted, it uses the default (when necessary), returned by 
	:meth:`ketl.core.ValueConverter.get_default()`.
  """
	if isinstance ( property, PropertyMapperMixin ):
		property = property.property
	if not property:
		raise ValueError ( "converter_if_needed() requires a non-empty property key or a PropertyMapperMixin" )
	if property in ( GraphTriple.ID_KEY, GraphTriple.TYPE_KEY, GraphTriple.FROM_KEY, GraphTriple.TO_KEY ):
		return None
	return converter or ValueConverter.get_default ()


def pd_triples_by_id ( triple_pd_df: pd.DataFrame, id: str ) -> pd.DataFrame:
	"""
	Filters a Pandas dataframe representing :class:`ketl.core.GraphTriple` instances by the `id` column.
	This is typically used in tests.

	Returns the original dataframe of triples, filtered by the `id` column (use `copy()` as necessary).
	
	You might want to combine this with :fun:`pd_triples_value()`.
	"""
	return triple_pd_df [ triple_pd_df [ "id" ] == id ]



def pd_triples_value ( triple_pd_df: pd.DataFrame, key: str, do_unserialize: bool = True, do_drop_duplicates: bool = True ) -> list[Any]:
	"""
	Returns the values in a Pandas dataframe representing :class:`ketl.core.GraphTriple` instances
	for a given property key, optionally unserialising them with the default 
	:class:`ketl.core.ValueConverter`, optionally removing the duplicates (which are always removed when
	triples are turned into knowledge graphs).

	This is typically used in tests.

	You might want to combine this with :fun:`pd_triples_by_id()`.
	"""
	result = triple_pd_df [ triple_pd_df [ "key" ] == key ] [ "value" ]
	if do_drop_duplicates:
		result = list ( result.drop_duplicates () )
	if do_unserialize:
		converter = ValueConverter.get_default ()
		result = [ converter.unserialize ( v ) for v in result ]
	return result
