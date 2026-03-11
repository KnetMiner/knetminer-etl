import asyncio
from itertools import islice
import json
import logging
from pathlib import Path
from typing import Any, Callable, Iterable, TextIO

from brandizpyes.io import dump_output
import concurrent
import neo4j
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ketl import (GraphProperty, JSONBasedValueConverter, PGElementType,
                  ValueConverter)
from ketl.spark_utils import df_load, df_save

import neo4j
import os

from brandizpyes.io import reader_helper
from brandizpyes.logging import ProgressLogger

log = logging.getLogger ( __name__ )


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


def pg_jsonl_neo_loader ( 
	pg_jsonl_source: str|Path|Iterable[str]|tuple[ str|Path|TextIO|Iterable[str],str|Path|TextIO|Iterable[str] ],		
	neo_driver: neo4j.AsyncDriver,
	loader_batch_size: int = 2500,
	loader_max_concurrency: int = max ( 1, os.cpu_count() - 1 )
) -> int:
	"""
	Loads a JSONL/PG file (see :func:`ketl.pg_df_2_pg_jsonl`) into a Neo4j database, through the provided driver.

	The function assumes the database is empty (or at least, not having nodes/edges in the sources), it first loads
	the nodes and then the edges, which must refer loaded nodes.

	The loading is done in parallel and in batches of graph elements.
	
	## Parameters:

	- pg_jsonl_source: the source of the JSONL/PG data.It can be a single `Path` object, or a tuple of two 
	sources, where the first element is the source for nodes and the second for edges.
	When a single source is given, it's interpreted as the prefix to two files, with suffixes 
	'-nodes.jsonl' and '-edges.jsonl'. In all the other cases, you must provide the two node/edge sources explicitly.
	In all these cases, a source is passed to :func:`ketl.reader_helper`, so it can be a file path, a file-like object,
	or a string (None doesn't make sense here).

	Returns the number of nodes+edges loaded.

	TODO: it doesn't need separated files, since the JSONL items report their type.
	"""

	progress_logger = ProgressLogger (
		logger = log,
		progress_resolution = 10000,
		log_message_template = "%d knowledge graph elements loaded"
	)
	# At the moment, it's not needed, since we report from the main loop only.
	# Set it if you want to report from the parallel batch parsers
	# progress_logger.set_is_thread_safe ( False ) 

	async def main_loop ( pg_elems_source: TextIO|Iterable[str], batch_loader: Callable[ [list[dict[str, Any]]], int ] ) -> int:
		"""
		The generic reader, passed to :func:`ketl.reader_helper`.

		This reads the source of PG nodes or edges, batches them into batches of `loader_batch_size` elements, 
		and sends each batch to the provided `batch_loader`, which is responsible for loading the batch into the 
		Neo4j database and returning the number of loaded elements.

		Multiple batches are processed asynchronously, with a maximum concurrency of `loader_max_concurrency`, 
		to avoid memory overflows and database pressure.

		When the input is exhausted, waits for all the batch loaders to complete and returns the total number 
		of loaded elements.
		"""
		executor = concurrent.futures.ThreadPoolExecutor ( max_workers = loader_max_concurrency )
		n_loaded = 0

		while True:
			batch = list ( islice ( pg_elems_source, loader_batch_size ) )
			if not batch: break

			loop = asyncio.get_running_loop()
			pg_elems: list[dict[str, Any]] = await loop.run_in_executor ( executor, lambda: json.loads ( "[" + ",".join ( js_line for js_line in batch ) + "]" ) )

			n_loaded += await batch_loader ( pg_elems )
			progress_logger.update ( n_loaded ) 

		return n_loaded


	async def nodes_load ( batch: list[dict[str, Any]] ) -> int:
		"""
		The batch loader for nodes.

		Receives a batch of PG nodes (as dictionaries), and creates them using Cypher.
		Also, manages retries in case of transaction collisions (TODO).

		Returns the number of loaded nodes.
		"""
		query = """
		UNWIND $nodes AS node_js
		WITH node_js.id AS nid, node_js.labels AS nlabels, node_js.properties AS nprops
		UNWIND nlabels AS nlabel
		CREATE (n) 
		SET n.id = nid
		SET n += nprops
		SET n :$(nlabel)
		"""
		async with neo_driver.session() as session:
			await session.execute_write ( lambda tx: tx.run ( query, nodes = batch ) )
		return len ( batch )
		

	async def edges_load ( batch: list[dict[str, Any]] ) -> int:
		"""
		The batch loader for edges.

		Similarly to the node loader, receives a batch of PG edges (as dictionaries), 
		and creates them via Cypher, managing retries as needed.

		Returns the number of loaded edges.
		"""
		pass

	# First, let's normalise the input source(s)
	nodes_source, edges_source = None, None
	if not isinstance ( pg_jsonl_source, tuple ):
		if not isinstance ( pg_jsonl_source, Path ):
			pg_jsonl_source = Path ( pg_jsonl_source )
		nodes_source = pg_jsonl_source.with_name ( pg_jsonl_source.stem + "-nodes.jsonl" )
		edges_source = pg_jsonl_source.with_name ( pg_jsonl_source.stem + "-edges.jsonl" )
	elif len ( pg_jsonl_source ) == 2:
		nodes_source, edges_source = pg_jsonl_source
	else:
		raise ValueError ( f"pg_jsonl_neo_loader(), invalid pg_jsonl_source: {pg_jsonl_source}" )

	if not ( nodes_source or edges_source ):	
		raise ValueError ( f"pg_jsonl_neo_loader(), both nodes_source and edges_source are empty" )

	n_nodes = n_edges = 0

	if not nodes_source:
		log.warning ( f"pg_jsonl_neo_loader(), no nodes source provided, only edges will be loaded" )
	else:
		log.info ( f"|== Loading Nodes" )
		progress_logger.log_message_template = "%d knowledge graph nodes loaded"
		n_nodes = reader_helper ( 
			lambda src: asyncio.run ( main_loop ( src, nodes_load ) ),
			nodes_source
		)

	if not edges_source:
		log.warning ( f"pg_jsonl_neo_loader(), no edges source provided, only nodes will be loaded" )
	else:
		log.info ( f"|== {n_nodes} nodes loaded. Loading Edges" )
		progress_logger.log_message_template = "%d knowledge graph edges loaded"
		n_edges = reader_helper (
			lambda src: asyncio.run ( main_loop ( src, edges_load ) ), 
			edges_source
		)		
		log.info ( f"|== {n_edges} edges loaded." )

	log.info ( f"A total of {n_nodes + n_edges} elements loaded, all done." )
	
	return n_nodes + n_edges
