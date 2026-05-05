"""
Helpers used to build mappers and other entities from the KETL core.
"""

from ketl.core import ConstantTripleMapper, GraphTriple

def type_triple_mapper ( type_value: str ) -> ConstantTripleMapper:
	"""
	Helper to build a :class:`ketl.ConstantTripleMapper` for the :py:attr:`ketl.GraphTriple.TYPE_KEY` 
	property, that is, for a node/edge label/type.
	"""
	return ConstantTripleMapper ( property = GraphTriple.TYPE_KEY, constant_value = type_value )
