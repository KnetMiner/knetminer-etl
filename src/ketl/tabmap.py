"""
Tabular/CSV mapping tools for KnetMiner ETLs

"""
import logging
from typing import Any, Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import explode, udf
from pyspark.sql.types import (ArrayType, DataType, StringType, StructField,
                               StructType)

from ketl import (ConstantPropertyMapper, GraphTriple, IdentityValueConverter,
                  Mapper, PreSerializers, ValueConverter)
from ketl.spark_utils import DataFrameCheckpointManager

log = logging.getLogger ( __name__ )

class ColumnValueMapper ( Mapper ):
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
		super().__init__ ( value_converter, pre_serializers, spark_data_type )
		self.column = column_id

	def value ( self, row_dict: dict [ str, Any ] ) -> str:
		"""
		Maps a data frame row (in the form `col: <value>`) to the target column value.

		We expect a row dictionary here, rather than a row array, since that's easier to
		manage in case you need to deal with other columns.

		If :attr:`value_converter` is set, it's used to serialise the value, possibly with 
		  the pre-serialisers that were given to it by the this class' constructor.
		"""
		if self.column not in row_dict: return None
		target_value = self.value_converter.serialize ( row_dict [ self.column ] )
		return target_value # None maps to None, cases like '' have to be managed by the value_mapper


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


class ColumnMapper ( ColumnValueMapper ):
	"""
	Column-oriented property mapper.

	An extension of :class:`ketl.tabmap.ColumnValueMapper` that builds an entire
	:class:`ketl.GraphProperty`, in addition to a plain mapped string value.
	"""
	def __init__ ( 
		self,
		column_id: str,
		property: str = None, 
		value_converter: ValueConverter | None = None,
		pre_serializers: PreSerializers | None = None,
		spark_data_type: DataType | None = None
	):
		super ().__init__ ( column_id, value_converter, pre_serializers, spark_data_type )
		self.property = property if property else column_id
		
	def triple ( self, triple_id: str, row_dict: dict [ str, Any ] ) -> GraphTriple | None:
		"""
		Builds a :class:`ketl.GraphTriple` for this column/property using the current row.
		Returns `None` if the current column value is null, empty or the current column mapper returns
		`None`, that is, if `self.value( row_dict )` returns `None`.
		"""
		prop_value = self.value ( row_dict )
		if prop_value is None: return None
		return GraphTriple ( triple_id, self.property, prop_value )


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
	def __init__ ( 
		self, 
		id_mapper: IdColumnMapper, 
		column_mappers: list[ ColumnMapper ] = None,
		const_prop_mappers: list[ ConstantPropertyMapper ] = None ):
		"""
		@param id_mapper: the :class:`ketl.tabmap.ColumnValueMapper` to build the triple ID.

		@param column_mapper_list: a list of :class:`ketl.tabmap.ColumnMapper` to build the triple/relationship properties.
		  The order of this must match the order of the columns in the input DataFrame.

		@param const_properties: a set of constant properties to add to each triple set (ie, each node or relationship).
		  This is useful to add properties like :py:attr:`ketl.GraphProperty.TYPE_KEY`.
		"""
		self.id_mapper = id_mapper
		self.column_mappers = column_mappers if column_mappers else []
		self.const_prop_mappers = const_prop_mappers if const_prop_mappers else []
		self.column_mapper_keys = [ cmap.column for cmap in self.column_mappers ]
		self.column_mapper_keys.append ( self.id_mapper.column )

	def map ( self, df: DataFrame ) -> DataFrame:
		def map_spark_row ( *selected_row ) -> list [ list [ str, str, str ] ]:
			"""
			Internal UDF to map a Spark row into a list of :class:`ketl.GraphTriple` (equivalents).

			@param selected_cols: list of DF columns from the DF, where the elements reflect the keys
			in column_mappers.

			@return: a list of 3-element lists, corresponding to :class:`ketl.GraphTriple`, one row per 
			property.
			"""
			row_dict = dict ( zip ( self.column_mapper_keys, selected_row ) )
			log.debug ( f"map_spark_row() row_dict: {row_dict}" )

			# The node or relationship ID
			triple_id = self.id_mapper.value ( row_dict )

			if not triple_id: return []

			mapped_row = [] # id, key, value
			for cmap in self.column_mappers:
				triple = cmap.triple ( triple_id, row_dict )
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
		selected_cols = [ df [ col ] for col in self.column_mapper_keys ]

		out_df = df.withColumn ( "triples", map_spark_row_udf ( *selected_cols ) ) \
		  .select ( explode ( "triples" ).alias ( "triple" ) ) \
			.select ( f"triple.id", "triple.key", "triple.value" ) 
		  # Explode the 'triplet' struct into its columns

		return out_df


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
		column_mappers: list[ ColumnMapper ] = None,
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
			id_mapper, column_mappers, const_prop_mappers
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

		- out_path: if given, a :class:`DataFrameCheckpointManager` is used to save the mapped data frame
		  to a parquet file, as an intermediate that allows for building incremental workflows in Snakemake or
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

		all_cmappers = self.data_frame_mapper.column_mappers + [ self.data_frame_mapper.id_mapper ]

		if not opts.get ( "inferSchema", True ):
			# Check spark_data_type consistency
			spark_data_types = {}
			for cmap in all_cmappers:
				if cmap.column not in spark_data_types:
					spark_data_types [ cmap.column ] = cmap.spark_data_type
					continue
				if spark_data_types [ cmap.column ] != cmap.spark_data_type:
					raise ValueError (
						f"TabFileMapper: inconsistent spark_data_type values for the column '{cmap.column}'"
					)

		# Good, now build the schema with the listed columns, with casting as required.
		df_col_map = {}
		for cmap in all_cmappers:
			df_col = df [ cmap.column ]
			if not opts.get ( "inferSchema", True ) and cmap.spark_data_type:
				df_col = df_col.cast ( cmap.spark_data_type )

			df_col_map [ cmap.column ] = df_col

		df = df.withColumns ( df_col_map )

		# And eventually do the mapping
		triple_df = self.data_frame_mapper.map ( df )

		# Save to file if required
		if out_path:
			# Remember, we can't log much more than this, since most of Spark is declarative and things
			# really happen only upon an actual action like this
			#
			log.info ( f"Saving mapped tab file to \"{out_path}\"" )
			DataFrameCheckpointManager.save_intermediate ( triple_df, out_path )
		
		return triple_df
