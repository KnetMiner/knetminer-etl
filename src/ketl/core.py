"""
KnetMiner ETL Tools

`ketl.*` is the root for a set of modules that we use to build KnetMiner knowledge graphs (KG).
Much of the implemented functionality is based on Spark.
"""

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Callable

from pyspark.sql.types import DataType
from pyspark.sql import DataFrame

@dataclass ( frozen = True )
class GraphProperty:
	"""
	Core data structure to represent a knowledge graph node or relation property.

	
	# Attributes:
	
	- `key (str)`: The property key/name.

	- `value (Any)`: The property value. **WARNING**: in most of the KETL componenents, this
	stores a string/JSON representation of the actual value, as required by Spark DataFrames.
	This is not a GraphProperty requirement or GraphTriple requirement, just how we use these
	structures in most cases. See :class:`ketl.JSONBasedValueConverter`. This applies to regular 
	properties ONLY, not to special keys like :py:attr:`ketl.GraphProperty.TYPE_KEY`.
	"""

	TYPE_KEY = "@type"
	""""
	Represents the node label/type or relationship type
	"""

	FROM_KEY, TO_KEY = "@from", "@to"
	"""
	Represent the relationship keys used for the endpoint nodes
	"""

	key: str
	value: Any


@dataclass ( frozen = True )
class GraphTriple ( GraphProperty ):
	"""
	Core data structure to represent a knowledge graph property plus the ID of the node or relationship
	it belongs to.

	## Attributes
	- `id (str)`: The node or relationship ID.

	- key/value inherited from :class:`ketl.GraphProperty`. As said above, values are usually stored
	as string/JSON representations.
	"""

	ID_KEY = "id"

	DATAFRAME_SCHEMA_LIST = [ ID_KEY, "key", "value" ]

	id: str

	def __init__ ( self, id: str, key: str, value: Any ):
		object.__setattr__ ( self, 'id', id )
		object.__setattr__ ( self, 'key', key )
		object.__setattr__ ( self, 'value', value )	

	def __repr__ ( self ) -> str:
		return f"GraphTriple(id={self.id}, key={self.key}, value={self.value})"


class PGElementType ( StrEnum ):
	"""PG-Format types to mark nodes/edges in JSONL.pg."""
	NODE = "node"
	EDGE = "edge"

# TODO: remove
#
PreSerializer = Callable[[Any], Any]
"""Type alias for a pre-serialiser function."""
PreSerializers = list [ PreSerializer ] | PreSerializer
"""Type alias for a possibly-multiple serialisers."""


class ValueConverter ( ABC ):
	"""
	KnetMiner ETL value converter base class.

	A value converter is used in the the ETL to convert a node/edge **property** value coming from a data source
  into a string representation, as it's required by Spark.
	When producing a final knowledge graph (in the PG-Format JSONL.pg or alike), 
	the same converter is used to convert the string representation back into the actual value.
	
	We provide with a the default implementation :class:`ketl.JSONBasedValueConverter` (see there).

	## Attributes:

	- `name`: The converter name (usually the class name).

	TODO: this could be a Python protocol.
	"""

	def __init__ ( self ):
		self.name: str = self.__class__.__name__

	@abstractmethod
	def serialize ( self, v: Any ) -> str | None:
		"""
		Serialises the input into a string representation, to be used by the Spark components that
		the KETL uses.

		Initially, we had a default implementation that called the :attr:`pre_serializer`. 
		However, this isn't good, since the method should return a string, while the pre-serialiser 
		returns Any type.

		So, now we don't have any default implementation, use `self.pre_serialize()` when you extend 
		this method (which should be always needed), to convert the value coming from the pre-serialiser
		into a string representation, and based on the specific converter you're defining.
		"""

	@abstractmethod
	def unserialize ( self, v: str ) -> Any:
		"""
		Goes from the serialized value back to the original value. To be used in cases like JSON export
		or database population.
		"""

	@classmethod
	def get_default ( cls ) -> "ValueConverter":
		"""
		Factory method to get the default value converter. If not set (eg, at the begin of an app), this
		is :class:`ketl.JSONBasedValueConverter`.
		"""
		if not hasattr ( cls, "_default" ):
			cls._default = JSONBasedValueConverter ()
		return cls._default
	
	@classmethod
	def set_default ( cls , default_converter: "ValueConverter" ):
		"""
		Factory method to configure the default value converter.
		"""
		cls._default = default_converter


class IdentityValueConverter ( ValueConverter ):
	"""
	TODO: probably to be removed. Proper serialisers are needed for properties and they aren't for
	special keys.

	Identity value converter for KETL.

	This converter does nothing, that is, it returns the input value as-is both in serialisation
	and unserialisation.

	We use this for special properties like 'id' or :py:attr:`ketl.GraphProperty.TYPE_KEY`, 
	where we just need to keep the original string. 

	Note that this still supports pre-serialization via :attr:`pre_serializer`.
	"""

	def serialize ( self, v: Any ) -> str:
		if v is None: return None
		return str ( v )

	def unserialize ( self, s: str ) -> Any:
		return s

class JSONBasedValueConverter ( ValueConverter ):
	"""
	JSON-based value converter for KETL.

	This uses :func:`json.dumps` and :func:`json.loads` to serialise/unserialise values to/from string 
	representations.

	We usually set this as a default, which means, for instance, that all the strings in a
	:class:`GraphTriple` data frame are quoted with '"'.
	"""

	def serialize ( self, v: Any ) -> str | None:
		if v is None: return None
		# Saving empty values isn't worth
		if isinstance ( v, str ) and v == "": return None
		result = json.dumps ( v )
		# Ditto
		if result is None or result == "": return None
		return result

	def unserialize ( self, s: str ) -> Any:
		"""See the class description for details."""
		if s is None: return None
		return json.loads ( s )



class ValueMapper ( ABC ):
	"""
	Abstract root class for KETL mappers.

	A mapper is intended to map a single unit of data, such as a column value in a row or a field value
	in a JSON object.

	This is a simple scaffold, offering basic initialisation.

	TODO: clarify null/empty behaviour, aggregating mappers, interaction with serialisation.
	"""
	pass


class PropertyMapperMixin ( ABC ):
	"""
	Mixin for mappers that map to :class:`ketl.GraphProperty` or :class:`ketl.GraphTriple`.

	The purpose of this is to mark a mapper that owns the :attr:`property` attribute.
	"""

	def _init ( self, property: str ):
		"""
		Initialises the mixin. This must be called by subclasses (typically from their constructors), 
		before using methods like `triple()`.

		TODO: bad name, change to something like _init_property
		"""
		self.property = property


class ConstantTripleMapper ( ValueMapper, PropertyMapperMixin ):
	"""
	A :class:`ketl.ValueMapper` to add constant value properties to a knowledge graph.

	An example of where this is useful is when you are generating nodes or relationships all 
	having the same :py:attr:`ketl.GraphProperty.TYPE_KEY`.

	We manage constants through this mapper, so that we can ensure the serialisation and pre-serialisation
	of configured Python data types (or custom types).

	## Attributes
	- `property`: The property key/name.
	- `constant_value`: The constant value to use to generate constant properties.
	- `spark_data_type`: see :class:`ketl.ValueMapper`.
	"""
	def __init__ (
		self,
		property: str,
		constant_value: Any
	):
		super().__init__ ()
		self._init ( property )
		self.constant_value = constant_value

	def triple ( self, triple_id: str, converter: ValueConverter = None ) -> GraphTriple | None:
		"""
		Does the mapping job, by building a :class:`ketl.GraphTriple` with the constant property/value.
		and the provided triple ID.

		The `converter` parameter is forwarded to the `value()` method. If the latter returns None
		(possibly, after serialisation), then None is returned here too. As said below, this shouldn't be
		a common case.
		"""
		v = self.value ( converter )
		if v is None: return None # TODO: does it make sense? Should it raise an error instead?
		return GraphTriple ( triple_id, self.property, v )
	
	def value ( self, converter: ValueConverter = None ) -> Any | None:
		"""
		Returns the constant value. Note that, given how `ValueMapper` deals with None and empty values,
		this returns empty strings unchanged.

		This method can turn an initial value into a serialised string when the `converter` parameter is provided. 
		Usually, an aggregating mapper will decide to serialise or not, based on whether it's dealing with a 
		node/edge user property or a special key like type.

		When serialisation is applied, an empty string might end up returning None. Note that, while this is
		possible, it would be usually weird that a mapping configuration has a null or empty constant.
		"""
		if converter is not None: return converter.serialize ( self.constant_value )
		return self.constant_value


@dataclass ( frozen = True )
class SparkDataFrameTypes:
	"""
	Utility to manage Spark data at column level.

	For the moment, we only provide a way to cast columns to specific Spark data types, in case
	the automatic inference of the schema doesn't work.

	"""

	@dataclass ( frozen = True )
	class ColumnSpec:
		"""
		We use this despite we have just one thing to configure, because we might extend it.
		"""
		spark_type: DataType | None = None
		"""
		The Spark data type to cast the column to. If None, no casting is applied.
		This is used in :meth:`ketl.core.SparkDataFrameTypes.cast_df`
		"""

	column_specs: dict [ str, ColumnSpec ] = field ( default_factory = dict )

	def cast_df ( self, df: DataFrame ) -> DataFrame:
		"""
		Returns a new DF where the hereby-specified columns are casted to the requested types.

		If the DF doesn't contain one of the specified columns, there is no effect, so a single
		set could be used for multiple DFs.
		"""
		df_col_map = {}
		for col in df.columns:
			if not col in self.column_specs: continue
			col_spec = self.column_specs [ col ]
			if not col_spec.spark_type: continue
			spark_type = col_spec.spark_type
			df_col_map [ col ] = df [ col ].cast ( spark_type )
		
		if not df_col_map: return df
		return df.withColumns ( df_col_map )
