import asyncio
from itertools import islice
import json
import logging
from pathlib import Path
import re
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


class NeoLoaderDefaults:
	def __new__(cls, *args, **kwargs):
		raise TypeError("Can't instantiate a constant container")
	
	BATCH_SIZE = 2500
	MAX_CONCURRENCY = max(1, os.cpu_count() - 1)
	DO_NODES = True
	DO_EDGES = True

async def async_pg_jsonl_neo_loader ( 
	pg_jsonl_source: str|Path|TextIO|Iterable[Any]|None,		
	neo_driver: neo4j.AsyncDriver,
	do_nodes = NeoLoaderDefaults.DO_NODES,
	do_edges = NeoLoaderDefaults.DO_EDGES,
	loader_batch_size: int = NeoLoaderDefaults.BATCH_SIZE,
	loader_max_concurrency: int = NeoLoaderDefaults.MAX_CONCURRENCY
) -> int:
	"""
	Loads a JSONL/PG file (see :func:`ketl.pg_df_2_pg_jsonl`) into a Neo4j database, through the provided driver.

	The function assumes the database is empty (or at least, not having nodes/edges in the sources), it first loads
	the nodes and then the edges, which must refer loaded nodes.

	The loading is done in parallel and in batches of graph elements.

	This is the async version, which is needed in cases where the caller is already in an async context
	(eg, tests). For a synchronous wrapper, see :func:`ketl.pg_jsonl_neo_loader`.

	The loader works by first loading the nodes (JSONL lines with "type" == "node"), 
	and then it reads the source again and works on the edges ("type" == "edge"). This is necessary to 
	ensure the edges link existing nodes.

	As long as this consistency is preserved, this behaviour can be changed using the flags `do_nodes` 
	and `do_edges`, see below for details. When both are set (default), the `pg_jsonl_source` **cannot** be an
	iterator or None (which defaults to stdin), for the pretty obvious reason that these kinds of sources
	can't be read twice. You can pass a file-like object in this mode, but it must be a random access one,
	ie, its `seek()` method must work correctly.
	

	## Parameters:

	- pg_jsonl_source: the source of the JSONL/PG data. 
	This is passed to :func:`ketl.reader_helper`, so it can be a file path, a file-like object,
	a string of PG-JSON data, None (to get data from the stdin). **WARNING**: as said above, when 
	both `do_nodes` and `do_edges` are set, the `pg_jsonl_source` can't be neither an iterator 
	nor None/stdin.

	- neo_driver: the Neo4j async driver to use for loading the data.

	- do_nodes, do_edges: whether to load nodes and edges, respectively. You can choose to load node definitions
	or edge definitions only, which can be useful for testing or to have more incremental updates in a SnakeMake
	pipeline. **WARNING**: you **can't** load edges that refer to nodes not loaded in the database, see above.

	- loader_batch_size: Neo4j loads one batch of nodes or edges per transaction, and this is the batch size.
	  
	- loader_max_concurrency: the maximum number of concurrent batch loaders. The loader has this number of 
	parallel JSON parsers and this number of concurrent batch loading transactions (see the internal `main_loop` 
	function for details). It uses the common default of all the available CPU cores minus one (left to the main 
	thread). You should be fine with this, except, maybe, in cases like tests or debugging.	

	## Returns:

	The number of nodes+edges loaded.

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

	async def main_loop ( pg_elems_source: TextIO|Iterable[str], is_nodes_mode: bool, batch_loader: Callable[ [list[dict[str, Any]]], int ] ) -> int:
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

		type_filter = "node" if is_nodes_mode else "edge"
		type_re = f"""("type"|'type'):\\s*("{type_filter}"|'{type_filter}')"""
		type_re = re.compile ( type_re )

		# Switch to a filtered iterable:
		
		# TODO: remove me
		def line_debugger ( line: str ) -> str:
			log.debug ( f"line is {line}" )
			return line and re.search ( type_re, line )
		
		pg_elems_source = ( line for line in pg_elems_source if line and re.search ( type_re, line ) )
		
		# TODO: remove me 
		# pg_elems_source = ( line for line in pg_elems_source if line_debugger ( line ) )

		while True:
			batch = list ( islice ( pg_elems_source, loader_batch_size ) )
			if not batch: break

			loop = asyncio.get_running_loop()
			# Parse the JSON lines in parallel
			pg_elems: list[dict[str, Any]] = await loop.run_in_executor ( executor, lambda: json.loads ( "[" + ",".join ( js_line for js_line in batch ) + "]" ) )

			# Do the loading asynchronously. So, we have parallel parsers added to the main thread running the source
			# scanning plus the loaders in the event loop.
			n_loaded += await batch_loader ( pg_elems )
			progress_logger.update ( n_loaded ) 

		return n_loaded


	async def nodes_load ( nodes_batch: list[dict[str, Any]] ) -> int:
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
			await session.execute_write ( lambda tx: tx.run ( query, nodes = nodes_batch ) )
		return len ( nodes_batch )
		

	async def edges_load ( edges_batch: list[dict[str, Any]] ) -> int:
		"""
		The batch loader for edges.

		Similarly to the node loader, receives a batch of PG edges (as dictionaries), 
		and creates them via Cypher, managing retries as needed.

		Returns the number of loaded edges.
		"""
		query = """
		UNWIND $edges AS edge_js
		WITH edge_js.id AS eid, edge_js.labels[0] AS etype, 
		  edge_js.properties AS eprops, edge_js.from AS from_id, edge_js.to AS to_id
		MATCH (from { id: from_id } )
		MATCH (to { id: to_id } )
		CREATE (from)-[e:$(etype)]->(to)
		SET e.id = eid
		SET e += eprops
		"""
		async with neo_driver.session() as session:
			await session.execute_write ( lambda tx: tx.run ( query, edges = edges_batch ) )
		return len ( edges_batch )
	
	# Some consistency check
	source_has_seek = hasattr ( pg_jsonl_source, "seek" ) and callable ( pg_jsonl_source.seek )
	if do_edges and do_nodes:
		if not ( isinstance ( pg_jsonl_source, (str, Path) ) or source_has_seek ):
			raise ValueError ( 
				f"pg_jsonl_neo_loader(), attempt to load both nodes and edges with a non-rewindable source." 
				+ " Pass me a string, a path or a rewindable file-like object." 
			)

	n_nodes = n_edges = 0

	if not do_nodes:
		log.info ( f"pg_jsonl_neo_loader(), do_nodes not set, skipping node loading" )
	else:
		log.info ( f"|== Loading Nodes" )
		progress_logger.log_message_template = "%d knowledge graph nodes loaded"
		n_nodes = await reader_helper ( 
			lambda src: main_loop ( src, is_nodes_mode = True, batch_loader = nodes_load ),
			pg_jsonl_source
		)

	if not do_edges:
		log.info ( f"pg_jsonl_neo_loader(), do_edges not set, skipping edge loading" )
	else:
		msg = f"{n_nodes} nodes loaded. Loading Edges" if do_nodes else "Loading Edges"
		log.info ( f"|== {msg}" )
		
		# Rewind as needed
		if do_nodes and source_has_seek:
			try:
				pg_jsonl_source.seek ( 0 )
			except IOError as ex:
				raise IOError ( f"pg_jsonl_neo_loader(), failed to rewind the source for edge loading: {ex}", cause = ex )

		progress_logger.log_message_template = "%d knowledge graph edges loaded"
		n_edges = await reader_helper (
			lambda src: main_loop ( src, is_nodes_mode = False, batch_loader = edges_load ),
			pg_jsonl_source
		)		
		log.info ( f"|== {n_edges} edges loaded." )

	log.info ( f"A total of {n_nodes + n_edges} elements loaded, all done." )
	
	return n_nodes + n_edges


def pg_jsonl_neo_loader ( 
	pg_jsonl_source: str|Path|TextIO|Iterable[Any]|None,		
	neo_driver: neo4j.AsyncDriver,
	do_nodes = NeoLoaderDefaults.DO_NODES,
	do_edges = NeoLoaderDefaults.DO_EDGES,
	loader_batch_size: int = NeoLoaderDefaults.BATCH_SIZE,
	loader_max_concurrency: int = NeoLoaderDefaults.MAX_CONCURRENCY
) -> int:
	"""
	Loads a JSONL/PG file (see :func:`ketl.pg_df_2_pg_jsonl`) into a Neo4j database, through the provided driver.

	This is only a synchronous wrapper for :func:`ketl.async_pg_jsonl_neo_loader`, the meat is there, 
	including detailed documentation.

	This wrapper simply uses `asyncio.run` to run the async version, so it can be used to start a new async event loop and 
	run the async stuff in it. **DO NOT** call this function from an already running async context, since you'll likely 
	stumble upon errors. In that case, use :func:`ketl.async_pg_jsonl_neo_loader` instead.
	"""
	return asyncio.run ( async_pg_jsonl_neo_loader ( 
		pg_jsonl_source, neo_driver, do_nodes, do_edges, loader_batch_size, loader_max_concurrency
	))
