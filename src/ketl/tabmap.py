"""
Tabular/CSV mapping tools for KnetMiner ETLs
"""

import logging
from abc import abstractmethod
from typing import Any, Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import explode, udf
from pyspark.sql.types import (ArrayType, DataType, StringType, StructField,
                               StructType)

from ketl import (ConstantPropertyMapper, GraphTriple, IdentityValueConverter,
                  Mapper, PreSerializers, PropertyMapperMixin, ValueConverter)
from ketl.spark_utils import df_save

log = logging.getLogger ( __name__ )


class RowValueMapper ( Mapper ):
	"""
	Row-oriented value mapper.

	Maps a row to a value, based on one or more column values.
	"""
	def __init__ ( 
		self,
		column_ids: list [ str ],
		value_converter: ValueConverter | None = None,
		pre_serializers: PreSerializers | None = None,
		spark_data_type: DataType | None = None
	):
		"""
		## Parameters

		- column_ids: the list of column IDs that this mapper depends on. This is necessary for components
		  like :class:`ketl.tabmap.SparkDataFrameMapper`, which need to know which columns to select from 
			an input DataFrame.
		
		- value_converter, pre_serializers, spark_data_type: see :class:`ketl.Mapper`.
		"""

		super().__init__ ( value_converter, pre_serializers, spark_data_type )
		self.column_ids = column_ids

	@abstractmethod
	def value ( self, row_dict: dict [ str, Any ] ) -> str | None:
		"""
		The method that does the job of mapping a row to a value.
		"""

	
	@classmethod
	def from_extractor ( 
		cls,
		extractor: callable [ dict [ str, Any ], Any ],
		column_ids: list [ str ],
		value_converter: ValueConverter | None = None,
		pre_serializers: PreSerializers | None = None,
		spark_data_type: DataType | None = None
	) -> RowValueMapper:
		"""
		Factory method to create a :class:`ketl.tabmap.RowMapper` from a value extractor function.

		See tests for examples of usage.
		
		## Parameters

		:param extractor: a function that takes a row dictionary and returns the desired value.
		The RowMapper instance that we create takes care of passing the value returned by this
		function to :attr:`value_converter` for serialisation.
		"""

		class ExtractorRowValueMapper ( RowValueMapper ):
			def __init__ ( self ):
				super().__init__ ( column_ids, value_converter, pre_serializers, spark_data_type )

			def value ( self, row_dict: dict [ str, Any ] ) -> str:
				value = extractor ( row_dict )
				return self.serialize ( value )
		
		return ExtractorRowValueMapper ()
	# /from_extractor


	@classmethod
	def for_edge_id_auto (
		cls,
		property_mappers: list [ ConstantPropertyMapper | PropertyMapperMixin ],
		prefix: str = None
	) -> RowValueMapper:
		"""
		Factory method to create a :class:`ketl.PropertyMapperMixin` for relationship/edge IDs
		by searching :attr:`GraphTriple.TYPE_KEY`, :attr:`GraphTriple.FROM_KEY` and :attr:`GraphTriple.TO_KEY` 
		into the provided property mappers and using these mappers to build the edge ID, the same way
		as :meth:`for_edge_id()`.
		"""
		def find_mapper ( prop_key: str ) -> PropertyMapperMixin:
			mapper = next ( ( pm for pm in property_mappers if pm.property == prop_key ), None )
			if not mapper:
				raise ValueError ( f"RowValueMapper.for_edge_id_auto: can't find property mapper for '{prop_key}'" )
			return mapper
		
		def extractor ( 
			mapper: ConstantPropertyMapper | PropertyMapperMixin, row: dict [ str, Any ]
		) -> str:
			if isinstance ( mapper, ConstantPropertyMapper ):
				return mapper.value ()
			elif isinstance ( mapper, RowTripleMapperMixin ):
				return mapper.value ( row )
			raise ValueError ( f"RowValueMapper.for_edge_id_auto: unsupported mapper type {type(mapper)}" )

		if not property_mappers:
			raise ValueError ( "RowValueMapper.for_edge_id_auto: no property mappers given" )
		
		# Get the ID component mappers
		(type_map, from_map, to_map) = (
			find_mapper ( GraphTriple.TYPE_KEY ),
			find_mapper ( GraphTriple.FROM_KEY ),
			find_mapper ( GraphTriple.TO_KEY )
		)

		# Required in the final result
		col_ids = [ 
			col for pm in ( type_map, from_map, to_map ) if isinstance ( pm, RowTripleMapperMixin )
			for col in pm.column_ids
		]

		# Component extractors to be used by the final extractor to compose the edge ID
		(type_extractor, from_extractor, to_extractor) = (
			lambda row: extractor ( type_map, row ),
			lambda row: extractor ( from_map, row ),
			lambda row: extractor ( to_map, row )
		)

		# Here it is
		return cls.from_extractor (
			extractor = lambda row: cls.build_edge_id (
				type_extractor ( row ),
				from_extractor ( row ),
				to_extractor ( row ),
				prefix
			),
			column_ids = col_ids,
			value_converter = IdentityValueConverter ()
		)
	# /for_edge_id_auto


	@classmethod
	def for_edge_id ( 
		cls,
		relation_type: str,
		from_column_id: str,
		to_column_id: str,
		prefix: str = None,
		value_converter: ValueConverter | None = None,
		pre_serializers: PreSerializers | None = None,
	) -> RowValueMapper:
		"""
		Factory method to create a :class:`ketl.tabmap.RowValueMapper` for relationship/edge IDs.
		The ID this creates is a combination of the relation type and the from/to node IDs.

		## Parameters

		* :param relation_type: the relationship type, eg, "encodes-protein"
		* :param from_column_id: the column ID that contains the source node ID
		* :param to_column_id: the column ID that contains the destination node ID
		* :param prefix: an optional prefix to add to the edge ID
	
		* For the other parameters, see :class:`ketl.Mapper`.
			As for :class:`IdColumnMapper`, the default value converter is :class:`ketl.IdentityValueConverter`.
		"""
		return cls.from_extractor (
			extractor = lambda row: cls.build_edge_id (
				relation_type, row[ from_column_id ], row[ to_column_id ], prefix
			),
			column_ids = [ from_column_id, to_column_id ],
			value_converter = value_converter if value_converter else IdentityValueConverter ( pre_serializers ),
			pre_serializers = pre_serializers if not value_converter else None
		)
	
	@classmethod
	def build_edge_id ( cls, relation_type: str, from_id: str, to_id: str, prefix: str = None ) -> str:
		"""
		Simple helper to build a common edge ID from the common edge composite key.
		"""
		if not prefix: prefix = ""
		return f"{prefix}{relation_type}_{from_id}_{to_id}"
# /RowValueMapper


class RowTripleMapperMixin ( RowValueMapper, PropertyMapperMixin ):
	"""
	Row-oriented property mapper.
	
	This is similar to :class:`ketl.PropertyMapperMixin`, and it can be used to make grapth triple
	mappers tabular format mappers.
	"""
	
	def triple ( self, triple_id: str, row_dict: dict [ str, Any ] ) -> GraphTriple | None:
		"""
		Builds a :class:`ketl.GraphTriple` for this row, based on the value returned by :meth:`value()`.
		Returns `None` the value mapping returns `None` (after massages like serialisation).
		"""

		prop_value = self.value ( row_dict )
		if prop_value is None: return None
		return GraphTriple ( triple_id, self.property, prop_value )

	@classmethod
	def from_extractor (
		cls,
		extractor: callable [ dict [ str, Any ], Any ],
		property: str,
		column_ids: list [ str ],
		value_converter: ValueConverter | None = None,
		pre_serializers: PreSerializers | None = None,
		spark_data_type: DataType | None = None
	) -> RowTripleMapperMixin:
		class ExtractorRowTripleMapper ( RowTripleMapperMixin ):
			def __init__ ( self ):
				super().__init__ ( 
					column_ids, value_converter, pre_serializers, spark_data_type
				)
				self._init ( property )

			def value ( self, row_dict: dict [ str, Any ] ) -> str:
				value = extractor ( row_dict )
				if value is None: return None
				return self.serialize ( value )
			
		return ExtractorRowTripleMapper ()
	
	@classmethod
	def for_from ( 
		cls, 
		extractor: callable [ dict [ str, Any ], Any ],
		column_ids: list [ str ],
	) -> RowTripleMapperMixin:
		return cls.from_extractor ( 
			extractor, GraphTriple.FROM_KEY, column_ids, IdentityValueConverter () )

	@classmethod
	def for_to ( 
		cls, 
		extractor: callable [ dict [ str, Any ], Any ],
		column_ids: list [ str ],
	) -> RowTripleMapperMixin:
		return cls.from_extractor ( 
			extractor, GraphTriple.TO_KEY, column_ids, IdentityValueConverter ()
		)
# /RowTripleMapperMixin


class ColumnValueMapper ( RowValueMapper ):
	"""
	Column-oriented value mapper. 
	
	This is to be used to map a column value from a table row.
	This basic mapper only returns the mapped value, see :class:`ketl.tabmap.ColumnMapper` for an extension
	that builds a :class:`ketl.GraphTriple`.

	"""
	def __init__ ( 
		self, 
		column_id: str, 
		value_converter: ValueConverter | None = None,
		pre_serializers: PreSerializers | None = None,
		spark_data_type: DataType | None = None
	):
		super().__init__ ( [ column_id ], value_converter, pre_serializers, spark_data_type )

	def value ( self, row_dict: dict [ str, Any ] ) -> str:
		"""
		Maps a data frame row (in the form `col: <value>`) to the target column value.

		We expect a row dictionary here, rather than a row array, since that's easier to
		manage in case you need to deal with other columns.

		If :attr:`value_converter` is set, it's used to serialise the value, possibly with 
		  the pre-serialisers that were given to it by the this class' constructor.
		"""
		if self.column_id not in row_dict: return None
		target_value = self.serialize ( row_dict [ self.column_id ] )
		return target_value # None maps to None, cases like '' have to be managed by the value_mapper
	
	@property
	def column_id ( self ) -> str:
		"""
		Read-only property for the column ID that this mapper maps from.

		This is a convenience property that wraps the first element of :attr:`column_ids`.
		"""
		return self.column_ids [ 0 ]
# /ColumnValueMapper


class IdColumnMapper ( ColumnValueMapper ):
	"""
	A :class:`ketl.tabmap.ColumnValueMapper` to be used for mapping the values of a column to triple IDs.

	This is the same as :class:`ketl.tabmap.ColumnValueMapper`, except its default value converter is
	:class:`ketl.IdentityValueConverter`, and sanity checks against null/empty values are added to
	`value()`.
	"""
	def __init__ ( 
		self, 
		column_id: str, 
		value_converter: ValueConverter | None = None,
		pre_serializers: PreSerializers | None = None,
		spark_data_type: DataType | None = None
	):
		# As explained in Mapper.__init__, pre-serialisers need to be checked against not being
		# added twice.
		super().__init__ ( 
			column_id, 
			value_converter if value_converter else IdentityValueConverter ( pre_serializers ),
			pre_serializers if not value_converter else None,
			spark_data_type
		)

	def value ( self, row_dict: dict [ str, Any ] ) -> str:
		v = super ().value ( row_dict )
		if not v: raise ValueError ( f"IdColumnMapper: null/empty ID value for column '{self.column}' in row {row_dict}" )
		return v
# /IdColumnMapper


class ColumnMapper ( ColumnValueMapper, RowTripleMapperMixin ):
	"""
	Column-oriented property mapper.

	An extension of :class:`ketl.tabmap.ColumnValueMapper` that combine with :class:`ketl.tabmap.RowTripleMapper`
	to build an entire triple out of a column value.
	"""
	def __init__ ( 
		self,
		column_id: str,
		property: str = None, 
		value_converter: ValueConverter | None = None,
		pre_serializers: PreSerializers | None = None,
		spark_data_type: DataType | None = None
	):
		ColumnValueMapper.__init__ ( 
			self, column_id, value_converter, pre_serializers, spark_data_type 
		)
		self._init ( property if property else column_id )
		
	
	@classmethod
	def for_from ( 
		cls,
		column_id: str,
		value_converter: ValueConverter | None = None,
		pre_serializers: PreSerializers | None = None
	) -> ColumnMapper:
		"""
		Factory method to create a :class:`ketl.tabmap.ColumnMapper` for the :attr:`ketl.GraphTriple.FROM_KEY` property, ie, 
		a relationship source node pointer.

		As for :class:`IdColumnMapper`, the default value converter is :class:`ketl.IdentityValueConverter`.
		"""
		return cls ( 
			column_id,
			GraphTriple.FROM_KEY, 
			value_converter = value_converter if value_converter else IdentityValueConverter ( pre_serializers ),
			pre_serializers = pre_serializers if not value_converter else None
		)
	
	@classmethod
	def for_to (
		cls,
		column_id: str,
		value_converter: ValueConverter | None = None,
		pre_serializers: PreSerializers | None = None
	) -> ColumnMapper:
		"""
		Factory method to create a :class:`ketl.tabmap.ColumnMapper` for the :attr:`ketl.GraphTriple.TO_KEY` property, ie, 
		a relationship destination node pointer.

		As for :class:`IdColumnMapper`, the default value converter is :class:`ketl.IdentityValueConverter`.
		"""
		return cls (
			column_id,
			GraphTriple.TO_KEY,
			value_converter = value_converter if value_converter else IdentityValueConverter ( pre_serializers ),
			pre_serializers = pre_serializers if not value_converter else None
		)
# /ColumnMapper


class SparkDataFrameMapper:
	"""
	The Spark DataFrame mapper.
	
	This is the main class to map a Spark DataFrame into a set of KG node/relationship properties
	(ie, triples).

	It uses a :class:`ketl.tabmap.ColumnValueMapper` to build the triple ID, and a list of :class:`ketl.tabmap.ColumnMapper`
	to build the node/relationshipt properties.
	
	The output is a new DataFrame with the three columns  
	:py:attr:`ketl.GraphTriple.DATAFRAME_SCHEMA_LIST`.
	
	Of course, this is compatible with Spark and can be further processed.

	TODO: validate against required properties, eg, type, from/to.
	"""

	class AutoEdgeId:
		"""
		Marker to tell that the id mapper should be auto-generated, using the prefix provided here
		"""
		def __init__ ( self, prefix: str = None ):
			self.prefix = prefix


	def __init__ ( 
		self, 
		id_mapper: RowValueMapper | AutoEdgeId | None, 
		row_mappers: list[ RowTripleMapperMixin ] = None,
		const_prop_mappers: list[ ConstantPropertyMapper ] = None ):
		"""
		@param id_mapper: the :class:`ketl.tabmap.ColumnValueMapper` to build the triple ID.
		  If it's None, it will use :meth:`ketl.tabmap.RowValueMapper.for_edge_id_auto`.
			If it's :class:`ketl.tabmap.SparkDataFrameMapper.AutoIdMapper`, it will use auto-edge with
			the given prefix.
			**WARNING**: these only makes sense for relationship/edge mappers, not for nodes.

		@param column_mapper_list: a list of :class:`ketl.tabmap.ColumnMapper` to build the triple/relationship properties.
		  The order of this must match the order of the columns in the input DataFrame.

		@param const_properties: a set of constant properties to add to each triple set (ie, each node or relationship).
		  This is useful to add properties like :py:attr:`ketl.GraphProperty.TYPE_KEY`.
		"""
		self.id_mapper = id_mapper

		self.row_mappers = row_mappers if row_mappers else []
		self.const_prop_mappers = const_prop_mappers if const_prop_mappers else []

		if not self.id_mapper or isinstance ( self.id_mapper, SparkDataFrameMapper.AutoEdgeId ):
			prefix = \
			  self.id_mapper.prefix if isinstance ( self.id_mapper, SparkDataFrameMapper.AutoEdgeId ) \
				else None
			
			self.id_mapper = RowValueMapper.for_edge_id_auto (
				self.const_prop_mappers + self.row_mappers,
				prefix = prefix
			)

		self._row_mapper_keys = [ col_id for cmap in self.row_mappers for col_id in cmap.column_ids ]
		self._row_mapper_keys.extend ( self.id_mapper.column_ids )


	def map ( self, df: DataFrame ) -> DataFrame:
		def map_spark_row ( *selected_row ) -> list [ list [ str, str, str ] ]:
			"""
			Internal UDF to map a Spark row into a list of :class:`ketl.GraphTriple` (equivalents).

			@param selected_cols: list of DF columns from the DF, where the elements reflect the keys
			in column_mappers.

			@return: a list of 3-element lists, corresponding to :class:`ketl.GraphTriple`, one row per 
			property.
			"""
			row_dict = dict ( zip ( self._row_mapper_keys, selected_row ) )
			log.debug ( f"map_spark_row() row_dict: {row_dict}" )

			# The node or relationship ID
			triple_id = self.id_mapper.value ( row_dict )

			if not triple_id: return []

			mapped_row = [] # id, key, value
			for row_mapper in self.row_mappers:
				triple = row_mapper.triple ( triple_id, row_dict )
				if triple is None: continue
				mapped_row.append ( [ triple_id, triple.key, triple.value ] )

			# And now the constants
			for const_mapper in self.const_prop_mappers:
				triple = const_mapper.triple ( triple_id )
				if triple is None: continue
				mapped_row.append ( [ triple_id, triple.key, triple.value ] )

			return mapped_row
		
		# Here we go

		out_schema = ArrayType (
			StructType ([
					StructField ( "id", StringType(), False ),
					StructField ( "key", StringType(), False ),
					StructField ( "value", StringType(), True )
			])
		)

		# As per Spark documentation.
		map_spark_row_udf = udf ( map_spark_row, out_schema )
		selected_cols = [ df [ col ] for col in self._row_mapper_keys ]

		out_df = df.withColumn ( "triples", map_spark_row_udf ( *selected_cols ) ) \
		  .select ( explode ( "triples" ).alias ( "triple" ) ) \
			.select ( f"triple.id", "triple.key", "triple.value" ) 
		  # Explode the 'triplet' struct into its columns

		return out_df
# /SparkDataFrameMapper


class TabFileMapper:
	"""
	A tabular file mapper.

	Maps files like TSV/CSV into a data frame of :class:`ketl.GraphTriple` rows, 
	by using :class:`ketl.tabmap.SparkDataFrameMapper`.
	"""

	DEFAULT_SPARK_OPTIONS = {
		"header": True,
		"delimiter": "\t",
		"inferSchema": True,
		"comment": "#"
	}
	
	def __init__ ( 
		self,

		id_mapper: IdColumnMapper,
		row_mappers: list[ ColumnMapper ] = None,
		const_prop_mappers: list[ ConstantPropertyMapper ] | None = None,

		spark_options: Dict[str, Any] | None = None,
	):
		"""
		TODO: we don't support files without headers. While we plan to do it at some point, 
		files like that are stupidly lazy, you shouldn't create them and if you get them 
		from third parties, you should write your own scripts to fix them. For the time being, 
		if you set "header" to False in `spark_options`, you'll get an error.

		## Parameters:
		 
		- id_mapper, column_mappers, const_prop_mappers: passed to :class:`ketl.tabmap.SparkDataFrameMapper`, 
		  to map the file columns to triples.

		- spark_options: options passed to :meth:`SparkSession.read.options`. If null, we use 
			:attr:`DEFAULT_SPARK_OPTIONS`. If specified, it will override those defaults.
		"""

		self.data_frame_mapper = SparkDataFrameMapper ( 
			id_mapper, row_mappers, const_prop_mappers
		)
		self.spark_options = spark_options

	def map ( self, spark: SparkSession, file_path: str, out_path: str | None = None ) -> DataFrame:
		"""
		Does the job and returns a Spark data frame representing the mapped triples.

		As said above, this is essentially a wrapper of :meth:`ketl.tabmap.SparkDataFrameMapper.map`.

		## Parameters:

		- spark: it needs a Spark session to work with. TODO: an session initialiser to be use in workflow
		  descriptors such as Snakefiles.

		- file_path: the path to the input tabular file (CSV/TSV).

		- out_path: if given, the mapped data frame is saved (as parquet) using the checkpointing	functions 
		  in `ketl.spark_utils`, as an intermediate that allows for building incremental workflows in Snakemake or
			similar frameworks. 


		## Notes

		* The "inferSchema" option and :attr:`ketl.Mapper.spark_data_type`: no matter, the value of this option,
		  we always load the columns in the mappers only, discarding any other columns that may be in the file.
			If this option is false, we additionally use the :attr:`ketl.Mapper.spark_data_type` attributes to
			cast the original columns to the desired types. If a column mapper doesn't have this attribute set,
			we leave the original column untouched (ie, loaded by Spark with its defaults).

		* **WARNING**: it's possible to map the same column header more than once, with multiple column mappers having
			the same `column` attribute. However, in this case, the `spark_data_type` must be consistent and we enforce
			it.
		"""

		# Fix the options, override defaults if requested
		#
		opts = TabFileMapper.DEFAULT_SPARK_OPTIONS.copy ()
		if self.spark_options:
			opts.update ( self.spark_options )
		
		if not opts.get ( "header", True ):
			raise ValueError ( "TabFileMapper: doesn't support files without headers yet" )
		
		log.info ( f"Mapping tab file \"{file_path}\"" )
		df = spark.read.options ( **opts ).csv ( file_path )

		# Work out an explicit schema.
		#
		# We need to add withColumn() transformations to the initial DF.
		# We can't just set a schema based on out mappers, since this would force the schema
		# in the same order as the mappers, eg, if column 1 is 'foo', but the first mapper is 
		# for 'bar', we would be broken.
		# 

		all_row_mappers = self.data_frame_mapper.row_mappers + [ self.data_frame_mapper.id_mapper ]

		if not opts.get ( "inferSchema", True ):
			# Check spark_data_type consistency
			spark_data_types = {}
			for row_mapper in all_row_mappers:
				for col_id in row_mapper.column_ids:
					if col_id not in spark_data_types:
						spark_data_types [ col_id ] = row_mapper.spark_data_type
						continue
					if spark_data_types [ col_id ] != row_mapper.spark_data_type:
						raise ValueError (
							f"TabFileMapper: inconsistent spark_data_type values for the column '{col_id}'"
						)

		# Good, now build the schema with the listed columns, with casting as required.
		df_col_map = {}
		for row_mapper in all_row_mappers:
			for col_id in row_mapper.column_ids:
				if col_id not in df.columns:
					raise ValueError ( f"TabFileMapper: input file missing required column '{col_id}'" )
				df_col = df [ col_id ]
				if not opts.get ( "inferSchema", True ) and row_mapper.spark_data_type:
					df_col = df_col.cast ( row_mapper.spark_data_type )

				df_col_map [ col_id ] = df_col

		df = df.withColumns ( df_col_map )

		# And eventually do the mapping
		triple_df = self.data_frame_mapper.map ( df )

		# Save to file if required
		if out_path:
			# Remember, we can't log much more than this, since most of Spark is declarative and things
			# really happen only upon an actual action like this
			#
			log.info ( f"Saving mapped tab file to \"{out_path}\"" )
			df_save ( triple_df, out_path )
		
		return triple_df
	# /map
# /TabFileMapper