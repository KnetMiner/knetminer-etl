"""
Helpers used to build mappers and other entities from the KETL core.
"""

from collections.abc import Callable
from sys import prefix
from typing import Any

from ketl.core import ConstantTripleMapper, GraphTriple

def type_triple_mapper ( type_value: str ) -> ConstantTripleMapper:
	"""
	Helper to build a :class:`ketl.ConstantTripleMapper` for the :py:attr:`ketl.GraphTriple.TYPE_KEY` 
	property, that is, for a node/edge label/type.
	"""
	return ConstantTripleMapper ( property = GraphTriple.TYPE_KEY, constant_value = type_value )


def string_value_wrapper ( prefix: str = "", postfix: str = "" ) -> Callable[ [Any], str|None ]:
	"""
	Helper to build a value filter that adds a prefix to a value extracted by a :class:`ketl.core.ValueMapper`.
	The filter also converts non-string values to strings, and discards None or empty values (by returning None).
	"""
	def filter_fun ( value: Any ) -> Any | None:
		if value is None: return None
		if not isinstance ( value, str ):
			value = str ( value )
		if not value: return None
		return prefix + value + postfix
	
	return filter_fun