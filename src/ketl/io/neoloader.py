import asyncio
import concurrent
import json
from itertools import islice
import os

from ketl.io import log

import neo4j
from brandizpyes.io import reader_helper
from brandizpyes.logging import ProgressLogger
import re
from pathlib import Path
from typing import Any, Callable, Iterable, TextIO


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