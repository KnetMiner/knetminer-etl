"""
Tabular/CSV mapping tools for KnetMiner ETLs
"""

import logging
from abc import abstractmethod
from typing import Any, Dict, Iterator

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

import ketl.helpers as khelper
from ketl.core import (ConstantTripleMapper, GraphTriple, PropertyMapperMixin,
                       SparkDataFrameTypes, ValueConverter, ValueMapper)
from ketl.spark.utils import df_save

log = logging.getLogger ( __name__ )


class RowValueMapper ( ValueMapper ):
	"""
	Row-oriented value mapper (Abstract class).

	Maps a row to a value, based on one or more column values.
	"""
	def __init__ ( self ):
		# We don't call super(), since this is used in multiple inheritance
		ValueMapper.__init__ ( self )
		self.column_ids: list [ str ] | None = None

	@abstractmethod
	def value ( self, row_dict: dict [ str, Any ], converter: ValueConverter = None ) -> Any | None:
		"""
		The method that does the job of mapping a row to a value.

		As for all the value mappers, this should return a value that, possibly, is serialised by means
		of a :class:`ketl.ValueConverter` before saving to a final target (triple data frame, knowledge
		graph format, graph database, etc). If the value returned by this method is None or an empty string,
		it's typically ignored.

		The row_dict parameter is always guaranteed to be non-empty, since the aggregate mappers like
		:class:`ketl.tabmap.SparkDataFrameMapper` skip empty rows.

		If `converter` is set, the method turns an initial value into a serialised string (see the docstring
		of :class:`ketl.ValueConverter`). This should be set for regular properties and not for 
		properties like node IDs.

		The method can be affected by :meth:`with_value_wrapper` (again, see `ValueMapper`).
		"""

	def with_column_ids ( self, column_ids: list [ str ] ) -> "RowValueMapper":
		"""
		The list of column IDs that this mapper depends on. This is used by components
		like :class:`ketl.tabmap.SparkDataFrameMapper`, to know the tabular input schema. As explained
		there, if column IDs aren't specified, the mappers load all the columns in the input, with 
		a possible impact on performance.

		This is a fluent style setter, it returns `self`.
		"""
		self.column_ids = column_ids
		return self

	def to_triple_mapper ( self, property: str ) -> "RowTripleMapper":
		"""
		Helper to build a :class:`ketl.tabmap.RowTripleMapper` from this row value mapper, by combining it with the provided property.

		This is useful to build triple mappers from value mappers, without having to re-implement the value mapping logic
		in the triple mapper.
		"""
		parent = self
		class TripleMapperWrapper ( RowTripleMapper ):
			def __init__ ( self ):
				super().__init__ ( property )

			def value ( self, row_dict: dict [ str, Any ], converter: ValueConverter = None ) -> Any | None:
				return parent.value ( row_dict, converter )		
		return TripleMapperWrapper ().with_column_ids ( self.column_ids )
	
# /RowValueMapper


class RowTripleMapper ( RowValueMapper, PropertyMapperMixin ):
	"""
	Row-oriented property mapper (Abstract class).
	
	This is similar to :class:`ketl.PropertyMapperMixin`, and it can be used to make graph triple
	mappers from tabular format mappers.
	"""
	def __init__ ( 
		self,
		property: str,
	):
		"""
		For details about the parameters, see :class:`ketl.tabmap.RowValueMapper` 
		and :class:`ketl.PropertyMapperMixin`.
		"""
		super().__init__ ()
		self._init ( property )

	def triple ( self, triple_id: str, row_dict: dict [ str, Any ], converter: ValueConverter = None ) -> GraphTriple | None:
		"""
		Builds a :class:`ketl.GraphTriple` for this row, based on the value returned by :meth:`value()`.
		Returns `None` the value mapping returns `None` (after massages like serialisation).

		The `converter` parameter is forwarded to the `value()` method. If the latter returns None
		(possibly, after serialisation), then None is returned here too.
		"""

		prop_value = self.value ( row_dict, converter )
		if prop_value is None: return None
		return GraphTriple ( triple_id, self.property, prop_value )
# /RowTripleMapper


class ColumnValueMapper ( RowValueMapper ):
	"""
	Column-oriented value mapper. 
	
	This is to be used to map a column value from a table row.
	This basic mapper only returns the mapped value, see :class:`ketl.tabmap.ColumnTripleMapper` for an extension
	that builds a :class:`ketl.GraphTriple`.

	"""
	def __init__ ( 
		self,
		column_id: str, 
	):
		RowValueMapper.__init__ ( self )
		self.with_column_ids ( [ column_id ] )

	def value ( self, row_dict: dict [ str, Any ], converter: ValueConverter = None ) -> Any | None:
		"""
		Maps a data frame row (in the form `col: <value>`) to the target column value.

		We expect a row dictionary here, rather than a row array, since that's easier to
		manage in case you need to deal with other columns.

		As said above, if this return None or an empty string, mappers usually ignore it.

		Moreover, this assumes `row_dict` to be non-empty.
		"""
		if self.column_id not in row_dict: return None
		value = row_dict.get ( self.column_id )
		if converter: value = converter.serialize ( value )
		return value
	
	@property
	def column_id ( self ) -> str:
		"""
		Read-only property for the column ID that this mapper maps from.

		This is a convenience property that wraps the first element of :attr:`column_ids`.
		"""
		return self.column_ids [ 0 ]
	
	def to_triple_mapper ( self, property: str = None ) -> "ColumnTripleMapper":
		"""
		This is like :class:`ketl.tabmap.RowValueMapper.to_triple_mapper`, except the property defaults
		to :attr:`column_id`, if not provided, and the returned mapper is the more specific column triple
		mapper.
		"""
		result = ColumnTripleMapper ( self.column_id, property )
		if self._value_wrapper is not None:
			# We need to propagate this too
			result.with_value_wrapper ( self._value_wrapper )
		return result

# /ColumnValueMapper



class ColumnTripleMapper ( ColumnValueMapper, RowTripleMapper ):
	"""
	Column-oriented triple mapper. 
	
	This is to be used to map a column value from a table row into a graph triple.

	You can instantiate this directly, or use :meth:`ketl.tabmap.ColumnValueMapper.to_triple_mapper` 
	"""
	def __init__ ( 
		self,
		column_id: str, 
		property: str|None = None
	):
		"""
		If the property (ID) is omitted, it defaults to `column_id`.
		"""
		# We don't call super() here, cause we don't want to mess up with the damn MRO.
		# Introduce an initialisation helper in ColumnValueMapper if needed.
		ColumnValueMapper.__init__ ( self, column_id )

		# Similarly, We're not calling the RowTripleMapper's constructor
		self._init ( property if property else column_id )


class SparkDataFrameMapperBase:
	"""
	Base class to define the essential elements in a mapper that maps a :class:`pyspark.sql.DataFrame` 
	into another mapper, consisting of a set of :class:`ketl.GraphTriple` rows.

	## Attributes
	- `mapper_name`: a name for this mapper, used in logging and error messages. Defaults to the class name.
	"""

	def __init__ ( self ):
		self.mapper_name = self.__class__.__name__
		
	@abstractmethod
	def map ( self, df: DataFrame ) -> DataFrame:
		pass

	def with_mapper_name ( self, mapper_name: str ) -> "SparkDataFrameMapperBase":
		"""
		Fluent style setter for the mapper name. This is used in logging and error messages.
		"""
		self.mapper_name = mapper_name
		return self

	def to_tab_file_mapper ( self, spark_options: Dict[str, Any] | None = None ) -> "GenericTabFileMapper":
		"""
		Helper to build a :class:`ketl.tabmap.GenericTabFileMapper` from this Spark DataFrame mapper.
		"""
		return GenericTabFileMapper ( 
			data_frame_mapper = self,
			spark_options = spark_options
		)


class SparkDataFrameMapper ( SparkDataFrameMapperBase ):
	"""
	The Spark DataFrame mapper.
	
	This is the main class to map a Spark DataFrame into a set of KG node/relationship properties
	(ie, triples).

	It uses a :class:`ketl.tabmap.ColumnValueMapper` to build the triple ID, and a list of :class:`ketl.tabmap.ColumnTripleMapper`
	to build the node/relationship properties.
	
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
		mapper_components: list[ RowTripleMapper|ConstantTripleMapper ] | None,
	):
		"""
		## Parameters

		- `id_mapper`: the :class:`ketl.tabmap.RowValueMapper` to build the triple ID.
		If it's AutoEdgeId or None, it will use :func:`ketl.tabmap.helper.edge_auto_id_row_value_mapper`
		with the given prefix. **WARNING**: these only makes sense for relationship/edge mappers, not for nodes.

		- `mapper_components`: a list of row value mappers or constant triple mappers, to be used to build the
		result. This constructor creates `row_mappers` and `const_prop_mappers` from this list, to be used internally
		and as a convenience to the outside.
		"""

		super().__init__ ()

		self.id_mapper = id_mapper

		# Let's setup the mappers
		self._set_mapper_components ( mapper_components )

		# Now, let's see what we have for the ID mapper
		#
		if not self.id_mapper: self.id_mapper = SparkDataFrameMapper.AutoEdgeId ()
		if isinstance ( self.id_mapper, SparkDataFrameMapper.AutoEdgeId ):
			prefix = self.id_mapper.prefix
			
			# TODO: This is a dirty trick to avoid circular imports. We should rearrange the tabmap modules,
			# but I'm not sure I should send this class and TabFileMapper in a separate module, 
			# just because of this issue.
			from ketl.tabmap.helpers import edge_auto_id_row_value_mapper

			self.id_mapper = edge_auto_id_row_value_mapper ( self.mapper_components )

			if prefix:
				self.id_mapper.with_value_wrapper ( khelper.string_value_wrapper ( prefix = prefix ) )

		# This prepares the feature, the rest has to be initialised by map()
		self.use_column_ids = False
		self._row_mapper_keys = None


	def map ( self, df: DataFrame ) -> DataFrame:
		def map_pandas_batches ( pdf_iter: Iterator [ pd.DataFrame ] ) -> Iterator [ pd.DataFrame ]:
			"""
			Internal function used with :meth:`pyspark.sql.DataFrame.mapInPandas`, to map batches of
			Spark rows (as pandas data frames) into batches of :class:`ketl.GraphTriple` rows.

			This is a 1:N transformation (a single input row can yield zero or more triple rows), which
			is what `mapInPandas` is for: it avoids the array-then-explode pattern that a plain (scalar)
			UDF would need, and moves data between the JVM and Python in Arrow-serialised batches, rather
			than one pickled row at a time.

			@param pdf_iter: an iterator of pandas data frames, each one a batch of input rows, with
			columns corresponding to `self._row_mapper_keys`.

			@return: an iterator of pandas data frames, each with columns `id`, `key`, `value`,
			corresponding to :class:`ketl.GraphTriple`, one row per property.
			"""
			for pdf in pdf_iter:
				mapped_rows = [] # id, key, value

				for selected_row in pdf.itertuples ( index = False, name = None ):
					row_dict = {
						k: ( None if pd.isna ( v ) else v )
						for k, v in zip ( self._row_mapper_keys, selected_row )
					}
					log.debug ( f"map_pandas_batches() row_dict: {row_dict}" )

					try:
						# The node or relationship ID
						triple_id = self.id_mapper.value ( row_dict )

						if not triple_id: continue

						for row_mapper in self._row_mappers:
							triple = row_mapper.triple ( triple_id, row_dict, khelper.converter_if_needed ( row_mapper ) )
							if triple is None: continue
							mapped_rows.append ( [ triple_id, triple.key, triple.value ] )

						# And now the constants
						for const_mapper in self._const_prop_mappers:
							triple = const_mapper.triple ( triple_id, khelper.converter_if_needed ( const_mapper ) )
							if triple is None: continue
							mapped_rows.append ( [ triple_id, triple.key, triple.value ] )

					except Exception as ex:
						raise RuntimeError ( f"Error: {ex} while mapping the row: {row_dict}" ) from ex

				yield pd.DataFrame ( mapped_rows, columns = [ "id", "key", "value" ] )


		### The body
		#

		try:
			self._init_row_mapper_keys ( df )

			out_schema = StructType ([
				StructField ( "id", StringType(), False ),
				StructField ( "key", StringType(), False ),
				StructField ( "value", StringType(), True )
			])

			selected_df = df.select ( *self._row_mapper_keys )
			out_df = selected_df.mapInPandas ( map_pandas_batches, schema = out_schema )

			return out_df

		except Exception as ex:
			raise RuntimeError ( f"Error: {ex} while mapping with the mapper '{self.mapper_name}'" ) from ex


	def with_use_column_ids ( self, use_column_ids: bool = True ) -> "SparkDataFrameMapper":
		"""
		If set, :meth:`RowValueMapper.column_ids` are used to project only the columns from the input DataFrame that are
		needed. By default (this flag set to False), all such columns are loaded, passed to the mappers and then they
		decide which ones to use. In most cases you'll be using all or almost all the input's columns, and even
		if that isn't the case, the default behaviour can be a performance issue only with very wide tables.
		
		This is a fluent style setter, it returns `self`.
		"""
		self.use_column_ids = use_column_ids
		return self
	
	def _init_row_mapper_keys ( self, df: DataFrame ) -> None:
		if not self.use_column_ids:
			# We use all the columns, so we just take them from the DF schema.
			self._row_mapper_keys = df.schema.names
			return
		
		# First, enforce that all the mappers have them
		for row_mapper in self._row_mappers:
			if not row_mapper.column_ids:
				raise ValueError ( f"SparkDataFrameMapper: use_column_ids is True, but mapper {row_mapper} doesn't have column_ids set" )

		self._row_mapper_keys = [ col_id for cmap in self._row_mappers for col_id in cmap.column_ids ]
		self._row_mapper_keys.extend ( self.id_mapper.column_ids )
	

	@property
	def row_mappers ( self ) -> list [ RowTripleMapper ]:
		"""
		Read-only property telling the row mappers used in this mapper.
		This is set by :meth:`mapper_components` (or by the constructor).
		"""
		return self._row_mappers
	
	@property
	def const_prop_mappers ( self ) -> list [ ConstantTripleMapper ]:
		"""
		Read-only property telling the constant property mappers used in this mapper.
		This is set by :meth:`mapper_components` (or by the constructor).
		"""
		return self._const_prop_mappers
	
	@property
	def mapper_components ( self ) -> list [ RowTripleMapper | ConstantTripleMapper ]:
		"""
		Read-only property telling all the mappers used in this mapper, both row and constant ones.
		"""
		return self._row_mappers + self._const_prop_mappers
	
	def _set_mapper_components ( self, mappers: list [ RowTripleMapper | ConstantTripleMapper ] | None ) -> None:
		"""
		Sets all the row value and constant mappers. After this, they're available both as `all_mappers` 
		and `row_mappers` + `const_prop_mappers`.
		"""
		if mappers is None: mappers = []
		self._row_mappers = [ m for m in mappers if isinstance ( m, RowTripleMapper ) ] 
		self._const_prop_mappers = [ m for m in mappers if isinstance ( m, ConstantTripleMapper ) ]

	def to_tab_file_mapper ( self, spark_options: Dict[str, Any] | None = None ) -> "TabFileMapper":
		"""
		Override of :meth:`SparkDataFrameMapperBase.to_tab_file_mapper`, to return the more specific type
		:class:`ketl.tabmap.TabFileMapper`.
		"""
		return TabFileMapper ( 
			id_mapper = self.id_mapper,
			mapper_components = self.mapper_components,
			spark_options = spark_options
		)

# /SparkDataFrameMapper


class GenericTabFileMapper:
	"""
	A tabular file mapper.

	Maps files like TSV/CSV into a data frame of :class:`ketl.GraphTriple` rows, 
	by using :class:`ketl.tabmap.SparkDataFrameMapperBase`.

	Most of the times, you'll be using :class:`ketl.tabmap.TabFileMapper`, which is a subclass 
	of this one that creates and uses a :class:`ketl.tabmap.SparkDataFrameMapper` to do the job, 
	ie, it's based on mapping a file to a single node or edge type.

	In contrast, this class is more generic and can be used to wrap data frame mappers like
	the one obtained from chaining multiple :class:`ketl.tabmap.SparkDataFrameMapper` 
	(see the `helpers` module) 

	"""

	DEFAULT_SPARK_OPTIONS = {
		"header": True,
		"delimiter": "\t",
		"inferSchema": True,
		"comment": "#"
	}

	def __init__ (
		self,
		data_frame_mapper: SparkDataFrameMapperBase,
		spark_options: Dict[str, Any] | None = None,
	):
		"""
		## Parameters:
		 
		- data_frame_mapper: a :class:`ketl.tabmap.SparkDataFrameMapperBase` that does the job of mapping
			the input data frame into a data frame of triples.

		- spark_options: options passed to :meth:`SparkSession.read.options`. If null, we use 
			:attr:`DEFAULT_SPARK_OPTIONS`. If specified, it will override those defaults.

		## Attributes:

		- `data_frame_types`: when set with a :class:`ketl.SparkDataFrameTypes`, it uses the 
		defined col -> spec mappings to deal with the data frame that is loaded from the file. As said
		elsewhere, at the moment we use this to allow for casting the input columns into desired types. 
		**WARNING**: this is only applied if the 'inferSchema' option is set to False.

		TODO: we don't support files without headers. While we plan to do it at some point, 
		files like that are stupidly lazy, you shouldn't create them and if you get them 
		from third parties, you should write your own scripts to fix them. For the time being, 
		if you set "header" to False in `spark_options`, you'll get an error.
		"""
		self.data_frame_mapper = data_frame_mapper
		self.spark_options = spark_options
		self.spark_data_frame_types: SparkDataFrameTypes | None = None


	def map ( self, spark: SparkSession, file_path: str, out_path: str | None = None ) -> DataFrame:
		"""
		Does the job and returns a Spark data frame representing the mapped triples.

		As said above, this is essentially a wrapper of :meth:`ketl.tabmap.SparkDataFrameMapper.map`.

		## Parameters:

		- spark: it needs a Spark session to work with. TODO: a session initialiser to be use in workflow
		  descriptors such as Snakefiles.

		- file_path: the path to the input tabular file (CSV/TSV).

		- out_path: if given, the mapped data frame is saved (as parquet) using the checkpointing	functions 
		  in `ketl.spark.utils`, as an intermediate that allows for building incremental workflows in Snakemake or
			similar frameworks. 
		"""

		# Fix the options, override defaults if requested
		#
		opts = GenericTabFileMapper.DEFAULT_SPARK_OPTIONS.copy ()
		if self.spark_options:
			opts.update ( self.spark_options )
		
		if not opts.get ( "header", True ):
			raise ValueError ( "TabFileMapper: doesn't support files without headers yet" )
		
		log.info ( f"Mapping tab file \"{file_path}\"" )
		df = spark.read.options ( **opts ).csv ( file_path )

		# Work out an explicit schema.
		#
		if not opts.get ( "inferSchema", True ):
			if self.spark_data_frame_types:
				log.info ( f"Casting the input file to the specified SparkDataFrameTypes" )
				df = self.spark_data_frame_types.cast_df ( df )
				log.debug ( f"Schema after casting: {df.schema}" )

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

# /GenericTabFileMapper


class TabFileMapper (GenericTabFileMapper):
	"""
	A tabular file mapper based on :class:`ketl.tabmap.SparkDataFrameMapper`.

	This is an extension of :class:`ketl.tabmap.GenericTabFileMapper` that creates and uses 
	a :class:`ketl.tabmap.SparkDataFrameMapper`, assuming you're mapping to a single node or edge type.

	As said above, this is the most common case.
	"""

	def __init__ ( 
		self,

		id_mapper: RowValueMapper | SparkDataFrameMapper.AutoEdgeId | None, 
		mapper_components: list[ RowTripleMapper|ConstantTripleMapper ] | None,

		spark_options: Dict[str, Any] | None = None,
	):
		"""
		## Parameters:
		 
		- id_mapper, mapper_components: passed to :class:`ketl.tabmap.SparkDataFrameMapper`, 
		  to map the file columns to triples.

		- spark_options: see the :class:`ketl.tabmap.GenericTabFileMapper` constructor.
			
		"""

		self.data_frame_mapper = SparkDataFrameMapper ( 
			id_mapper, mapper_components
		)
		super().__init__ ( self.data_frame_mapper, spark_options )

	@property
	def id_mapper ( self ) -> RowValueMapper:
		"""
		Convenience property to get the ID mapper from the internal :class:`ketl.tabmap.SparkDataFrameMapper`.
		Note that if this mapper was initialised with :class:`ketl.tabmap.SparkDataFrameMapper.AutoEdgeId`, 
		this will return the corresponding mapper that was created to support this mode. 
		"""
		return self.data_frame_mapper.id_mapper
	

	@property
	def row_mappers ( self ) -> list [ ColumnTripleMapper ]:
		"""
		Convenience property to get the row mappers from the internal :class:`ketl.tabmap.SparkDataFrameMapper`.
		"""
		return self.data_frame_mapper.row_mappers
	
	@property
	def const_prop_mappers ( self ) -> list [ ConstantTripleMapper ]:
		"""
		Convenience property to get the constant property mappers from the internal :class:`ketl.tabmap.SparkDataFrameMapper`.
		"""
		return self.data_frame_mapper.const_prop_mappers
	
	@property
	def mapper_components ( self ) -> list [ RowTripleMapper | ConstantTripleMapper ]:
		"""
		Convenience property to get all the mappers from the internal :class:`ketl.tabmap.SparkDataFrameMapper`.
		"""
		return self.data_frame_mapper.mapper_components
		
# /TabFileMapper