import json
from typing import TextIO

from brandizpyes.ioutils import dump_output
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ketl import (GraphProperty, JSONBasedValueConverter, PGElementType,
                  ValueConverter)
from ketl.spark_utils import df_load, df_save


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

	:param out_path: optional path where to save the resulting DataFrame as a Parquet file, using
	the checkpointing functions in `ketl.spark_utils`.

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
	triples_df = df_load ( triples_df_or_path, spark )

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
		df_save ( result_df, out_path )

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

	pg_df_or_path = df_load ( pg_df_or_path, spark )

	if not value_converters: value_converters = {}
	return dump_output ( writer, out_path )