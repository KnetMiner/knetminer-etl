"""
KnetMiner ETL Tools

`ketl.*` is the root for a set of modules that we use to build KnetMiner knowledge graphs (KG).
Much of the implemented functionality is based on Spark.
"""

import json
from abc import ABC, abstractmethod
from enum import StrEnum
from typing import Any, Callable, TextIO

from dataclasses import dataclass
from brandizpyes.ioutils import dump_output
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DataType

from ketl.spark_utils import DataFrameCheckpointManager


@dataclass ( frozen = True )
class GraphProperty:
	"""
	Core data structure to represent a knowledge graph node or relation property.

	Attributes:
		key (str): The property key/name.
		value (Any): The property value. **WARNING**: in most of the KETL componenents, this
		  stores a string/JSON representation of the actual value, as required by Spark DataFrames.
			This is not a GraphProperty requirement or GraphTriple requirement, just how we use these
			structures in most cases. See :class:`ketl.JSONBasedValueConverter`.
			This applies to regular properties ONLY, not to special keys like :py:attr:`ketl.GraphProperty.TYPE_KEY`.
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

	Attributes:
		id (str): The node or relationship ID.

		key/value inherited from :class:`ketl.GraphProperty`. As said above, values are usually stored
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


PreSerializer = Callable[[Any], Any]
"""Type alias for a pre-serialiser function."""

PreSerializers = list [ PreSerializer ] | PreSerializer
"""Type alias for a possibly-multiple serialisers."""

class ValueConverter ( ABC ):
	"""
	KnetMiner ETL value converter base class.

	A value converter is used in the the ETL to convert a value from a data source into a string representation,
	as it's required by Spark. When producing a final knowledge graph (in the PG-Format JSONL.pg or alike), 
	the same converter is used to convert the string representation back into the actual value.
	
	We provide with a the default implementation :class:`ketl.JSONBasedValueConverter` (see there).

	Attributes:
		name: The converter name (usually the class name).

		pre_serializers: An optional function or list of functions applied to a value before serialization.
		This can be set via :meth:`add_pre_serializers()` to customize the pre-processing of values. 
		For instance, it can be used to set defaults, or normalize known values (eg, trimming whitespaces).
		If it's a list, it's elements are chained in the list order (see :meth:`add_pre_serializers()`).
		If it's `None`, a default is set that returns None for null or falsy ('', [], etc) values.
	"""
	def __init__ ( self, pre_serializers: PreSerializers | None = None ):
		self.name: str = self.__class__.__name__
		self.pre_serializer = None
		if not pre_serializers:
			pre_serializers = lambda v: v or None
		self.add_pre_serializers ( pre_serializers )

	@abstractmethod
	def serialize ( self, v: Any ) -> str | None:
		"""
		Serialises the input into a string representation, to be used by the Spark components that
		the KETL uses.

		Initially, we had a default implementation that called the :attr:`pre_serializer`. 
		However, this isn't good, since the method should return a string, while the pre-serialiser 
		returns Any type.

		So, now we don't have any implementation, use `self.pre_serialize()` when you extend this
		method (which should be always needed).
		"""

	@abstractmethod
	def unserialize ( self, v: str ) -> Any:
		"""
		Goes from the serialized value back to the original value. To be used in cases like JSON export
		or database population.
		"""

	def pre_serialize ( self, v: Any ) -> Any:
		"""
		Helper that applies the pre-serializer, or, if this isn't set, returns v as-is.
		"""
		return self.pre_serializer ( v ) if self.pre_serializer else v

	def add_pre_serializers ( self, pre_serializers: PreSerializers | None = None ):
		"""
		Chains a list of pre-serializer functions to the one already set for this converter (else, just sets it).
		This also supports a single pre-serializer function, which will be added as such.

		Serialisers can be set either via this method or by directly setting the `pre_serializer` attribute.
		When a serialser is added to an existing one, a new serializer is set for the converter that calls
		the initial pre-serializer first, and then the new one on the result. Thus, this method is roughly 
		equivalent to:

		.. code-block:: python
		  for pre_serializer in pre_serializers:
				if not self.pre_serializer:
					self.pre_serializer = pre_serializer
				else:
					old_pre_serializer = self.pre_serializer
		  		self.pre_serializer = lambda v: pre_serializer ( old_pre_serializer ( v ) )
		"""	
		if not pre_serializers: return
		# TODO: more_itertools.always_iterable
		if not isinstance ( pre_serializers, list ):
			pre_serializers = [ pre_serializers ]

		for pre_serializer in pre_serializers:
			if not self.pre_serializer:
				self.pre_serializer = pre_serializer
				continue
			old_pre_serializer = self.pre_serializer
			self.pre_serializer = lambda v: pre_serializer ( old_pre_serializer ( v ) )

class IdentityValueConverter ( ValueConverter ):
	"""
	Identity value converter for KETL.

	This converter does nothing, that is, it returns the input value as-is both in serialisation
	and unserialisation.

	We use this for special properties like 'id' or :py:attr:`ketl.GraphProperty.TYPE_KEY`, 
	where we just need to keep the original string. 

	Note that this still supports pre-serialization via :attr:`pre_serializer`.
	"""
	def __init__ ( self, pre_serializers: PreSerializers | None = None  ):
		super().__init__( pre_serializers )

	def serialize ( self, v: Any ) -> str:
		return self.pre_serialize ( v )

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
	def __init__ ( self, pre_serializers: PreSerializers | None = None  ):
		super().__init__( pre_serializers )


	def serialize ( self, v: Any ) -> str | None:
		"""See the class description for details."""
		v = self.pre_serialize ( v )

		if v is None: return None
		return json.dumps ( v )

	def unserialize ( self, s: str ) -> Any:
		"""See the class description for details."""
		if s is None: return None
		return json.loads ( s )



class Mapper ( ABC ):
	"""
	Abstract root class/mixin for KETL mappers.

	A mapper is intended to map a single unit of data, such as a column value in a row or a field value
	in a JSON object.

	This is a simple scaffold that provides the :attr:`value_converter` attribute and its initialisation.

	TODO: this clashes with names like SparkDataFrameMapper, we need to rename things to capture the distinction
	between small chunks (like rows or cells) and whole containers (like files or DFs).
	
	"""
	def __init__ ( 
		self, 
		value_converter: ValueConverter | None = None,
		pre_serializers: PreSerializers | None = None,
		spark_data_type: DataType | None = None
	):
		"""
		Initialises the mapper with the value_converter (uses :class:`ketl.JSONBasedValueConverter` if null), 
		and sets up such converter with the pre_serializers (if any).

		**WARNING**: if you pass your own converter and this is already equipped with its pre-serialiser(s),
		you should not pass the same pre-serialiser(s) again here, else they will chained a second time
		to the already existing ones.

		This applies to subclasses too, that is, your initialisation should work like this:

		```python
		class MyMapper ( Mapper ):
			def __init__ ( self ):
				super().__init__ (
					value_converter if value_converter else MyValueConverter ( pre_serializers ),
					# Don't pass them twice when they're already passed to the default converter 
					pre_serializers if not value_converter else None						
				)
		```

		:attr:`spark_data_type` is an optional Spark data type to be used in tasks like
		reading from a CSV. Essentially, it allows for setting an explicit schema.

		TODO: StructType.nullable
		"""
		self.spark_data_type = spark_data_type

		if not value_converter:
			self.value_converter = JSONBasedValueConverter ( pre_serializers )
			return
		
		self.value_converter = value_converter
		if pre_serializers: self.value_converter.add_pre_serializers ( pre_serializers )


	def serialize ( self, value: Any ) -> str | None:
		"""
		Helper for serialising a value using the configured :attr:`value_converter`.
		
		You should use this in methods like `value()`, after having extracted a value from a data source.
		"""
		return self.value_converter.serialize ( value )



class PropertyMapperMixin ( ABC ):
	"""
	Mixin for mappers that map to :class:`ketl.GraphProperty` or :class:`ketl.GraphTriple`.

	The purpose of this is to mark a mapper that owns the :attr:`property` attribute.
	"""

	def _init ( self, property: str ):
		"""
		Initialises the mixin. This must be called by subclasses (typically from their constructors), 
		before using methods like `triple()`.
		"""
		self.property = property


class ConstantPropertyMapper ( Mapper, PropertyMapperMixin ):
	"""
	A :class:`ketl.Mapper` to add constant value properties to a knowledge graph.

	An example of where this is useful is when you are generating nodes or relationships all 
	having the same :py:attr:`ketl.GraphProperty.TYPE_KEY`.

	We manage constants through this mapper, so that we can ensure the serialisation and pre-serialisation
	of configured Python data types (or custom types).

	Attributes:
		property (str): The property key/name.
		constant_value (Any): The constant value to use to generate constant properties.
		value_converter (ValueConverter): see :class:`ketl.Mapper`.
	"""
	def __init__ (
		self,
		property: str,
		constant_value: Any,
		value_converter: Callable[[Any], str] = None,
		pre_serializers: PreSerializers | None = None,
		spark_data_type: DataType | None = None
	):
		super().__init__ ( 
			value_converter, 
			pre_serializers,
			spark_data_type
		)
		self._init ( property )
		self.constant_value = constant_value

	def triple ( self, triple_id: str ) -> GraphTriple | None:
		"""
		Does the mapping job, by building a :class:`ketl.GraphTriple` with the constant property/value.
		and the provided triple ID.
		"""
		v = self.value()
		if v is None: return None # TODO: does it make sense?
		return GraphTriple ( triple_id, self.property, v )
	
	def value ( self ) -> str | None:
		"""
		Returns the constant value, after the expected serialisation of the configured :attr:`value_converter`,
		which is obtained through :meth:`ketl.Mapper.serialize()`.
		"""
		return self.serialize ( self.constant_value )
	
	@classmethod
	def for_type ( cls, type_value: str ):
		"""
		Helper to build a :class:`ketl.ConstantPropertyMapper` for the :py:attr:`ketl.GraphTriple.TYPE_KEY` property,
		that is, for a node/edge label/type.

		We use the :class:`ketl.IdentityValueConverter` here, since types aren't properties and tools
		like the Neo4j uploader expect them to be unquoted strings.
		"""
		return cls ( GraphTriple.TYPE_KEY, type_value, IdentityValueConverter () )
	



def triples_2_pg_df ( 
	triples_df_or_path: DataFrame | str,
	triples_type: PGElementType,
	spark: SparkSession | None = None,
	out_path: str | None = None
) -> DataFrame:
	"""
	Converts a DataFrame of triples (ie, with the columns 'id', 'key') into a DataFrame reflecting the PG-Format.
	
	## Parameters:
	:param triples_df_or_path: The input DataFrame with the triples, or the path to a (Parquet) file where to
	load it from. When a path is given, `spark` must be provided too. 

	:param triples_type (PGElementType): The type of elements represented by the triples (nodes or edges).

	:param spark: when `triples_df_or_path` is a path, the Spark session used to load the Parquet file 
	(mandatory param in that case).

	:param out_path: optional path where to save the resulting DataFrame as a Parquet file, through
	:class:`ketl.spark_utils.DataFrameCheckpointManager`.
		
	## Returns:
	A data frame reflecting the structure of PG-Format, ie, with the columns:

	- type (string): equals to `triples_type`
	- id (string): from triples_df.id
	- labels (string array): an array, populated with the merge from triples_df.key == :py:attr:`ketl.GraphProperty.TYPE_KEY` values
	- from (string), to (string): present when triples_type is `PGElementType.EDGE`, and values taken from 
		triples_df.key == :py:attr:`ketl.GraphProperty.FROM_KEY`|:py:attr:`ketl.GraphProperty.TO_KEY`. 
		An error is raised if these are missing or there are multiple values for any set of triples 
		sharing an 'id'.
	- properties: a dictionary with all the other properties, with the map values being all lists of unique values
		(internally collected as sets), and all values still being string representations/serializations of
		the actual values. These are unserialised by :func:`ketl.pg_df_2_pgjsonl`.
	"""

	# Sanity checks
	if not isinstance(triples_type, PGElementType):
		raise ValueError ( f"triples_2_pg_df(): invalid triples_type: {triples_type}" )

	# The DF that collects the node labels/types
	triples_df = DataFrameCheckpointManager.df_load ( triples_df_or_path, spark )

	type_df = triples_df.filter ( F.col ( "key" ) == GraphProperty.TYPE_KEY )
	type_labels_df = type_df.groupBy ( "id" ).agg ( F.collect_set ( "value" ).alias ( "labels" ) )

	# The DFs about 'from' and 'to'
	if triples_type == PGElementType.EDGE:
		from_df = triples_df.filter ( F.col ( "key" ) == GraphProperty.FROM_KEY )
		to_df = triples_df.filter ( F.col ( "key" ) == GraphProperty.TO_KEY )

		from_values_df = from_df.groupBy ( "id" ).agg ( F.first ( "value" ).alias ( "from" ) )
		to_values_df = to_df.groupBy ( "id" ).agg ( F.first ( "value" ).alias ( "to" ) )

	# The property DF in 3 steps:
	# 

	# 1 Filter property rows
	properties_rows = triples_df.filter (
		~F.col ( "key" ).isin ( GraphProperty.TYPE_KEY, GraphProperty.FROM_KEY, GraphProperty.TO_KEY )
	)

	# 2. Agggregate them per node/edge and per property key
	property_values = properties_rows.groupBy ( "id", "key" ).agg (
		F.collect_set ( "value" ).alias ( "values" )
	)

	# 3. Build a map per node/edge with key/values pairs
	properties = property_values.groupBy ( "id" ).agg (
		F.map_from_entries (
			F.collect_list ( F.struct ( F.col ( "key" ), F.col ( "values" ) ) )
		).alias ( "properties" )
	)

	
	# The result DataFrame, start from IDs, then join the other elements built above
	#

	result_df = triples_df.select ( "id" ).distinct()

	# Labels
	result_df = result_df.join ( type_labels_df, on = "id", how = "left" ) 

  # edge endpoints
	if triples_type == PGElementType.EDGE:
		result_df = result_df.join ( from_values_df, on = "id", how = "left" )
		result_df = result_df.join ( to_values_df, on = "id", how = "left" )

	# properties (defaults to {})
	result_df = result_df.join ( properties, on = "id", how = "left" )
	result_df = result_df.withColumn (
		"properties",
		F.when ( F.col ( "properties" ).isNull(), F.create_map() ).otherwise ( F.col( "properties" ) )
	)

	# node/edge identifier
	result_df = result_df.withColumn ( "type", F.lit ( triples_type.value ) )

	# TODO, validations (as separate function):
	# - id
	# - #labels > 0
	# - from/to present if EDGE, and not null

	# Save if requested
	if out_path:
		DataFrameCheckpointManager.df_save ( result_df, out_path )

	# Eventually!
	return result_df


def pg_df_2_pg_jsonl ( 
	pg_df_or_path: DataFrame | str,
	spark: SparkSession | None = None,
	out_path: str | TextIO | None = None,
	value_converters: dict[str, ValueConverter] | None = None 
) -> str | None:
	"""
	Writes a DataFrame in the JSONL/PG format to a file if `out_path` is provided.

	## Parameters:
	- pg_df_or_path: when a DataFrame, the input DF in PG-Format (see :func:`ketl.triples_2_pg_df`).
		When a string, the path to a Parquet file storing the same kind of data frame, which is loaded
		through `spark`.

	- spark: when `pg_df_or_path` is a path, the Spark session used to load the Parquet file. So, it 
		is mandatory in that case.

	- out_path: has the same semantics of the corresponding parameter passed to :func:`ketl.dump_output`, that is:
		writes to a file path, or a file-like object, or to a to-be-returned string buffer.

	- value_converters (dict[str, ValueConverter], optional): A dictionary of value converters, to be used
		to unserialise from string representations in the property values back to the real values. If a converter
		isn't set for a property key, we use the default :class:`JSONBasedValueConverter` (see its docstring for
		details).

	## Returns:
		The JSONL/PG content as a string if `out_path` is None, otherwise None.

	"""

	def writer ( fh: TextIO ):
		"""
		The writer for :func:`ketl.dump_output`.
		
		Just iterates over the DataFrame rows and dumps PG-Format JSON objects.
		"""
		
		# pg_df_or_path is now a DF
		for row in pg_df_or_path.toLocalIterator ():
			# Unserialize property values
			properties = {}
			default_converter = JSONBasedValueConverter ()
			for k, vlist in row.properties.items():
				converter = value_converters.get ( k, default_converter )
				properties [ k ] = [ converter.unserialize ( v ) for v in vlist ]

			pg_elem = {
				"type": row.type,
				"id": row.id,
				"labels": row.labels,
				"properties": properties
			}
			if row.type == PGElementType.EDGE.value:
				pg_elem [ "from" ] = row [ "from" ]
				pg_elem [ "to" ] = row [ "to" ]

			fh.write ( json.dumps ( pg_elem ) + "\n" )
	
	pg_df_or_path = DataFrameCheckpointManager.df_load ( pg_df_or_path, spark )

	if not value_converters: value_converters = {}
	return dump_output ( writer, out_path )

