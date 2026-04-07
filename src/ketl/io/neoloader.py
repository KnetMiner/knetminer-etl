import asyncio
import concurrent
from dataclasses import dataclass, field
from enum import StrEnum
import json
from itertools import islice
import logging
import os

import neo4j
from brandizpyes.io import async_reader_helper
from brandizpyes.logging import ProgressLogger
import re
from pathlib import Path
from typing import Any, Callable, Iterable, TextIO
from collections.abc import KeysView


log = logging.getLogger ( __name__ )


class NeoLoaderDefaults:
	def __new__(cls, *args, **kwargs):
		raise TypeError("Can't instantiate a constant container")

	BATCH_SIZE = 2500
	MAX_CONCURRENCY = max(1, os.cpu_count() - 1)
	DO_NODES = True
	DO_EDGES = True


@dataclass ()
class NeoLoaderPropertyConfig:
	"""
	A container of configuration options for a PG property, which is used by the NeoLoader to 
	tune the loading from the PG format.
	"""

	class MultiValueMode ( StrEnum ):
		"""
		How to deal with multiple values.

		PG-JSONL always represents a property as an array, so we need to know if it actually contains
		an **set** of values, a single value (ie, a singleton in PG-JSONL) or you want to auto-detect it.

		PG-JSONL arrays are always considered **sets**, ie, the order is irrelevant and might be changed
		during a process like Neo4j loading, duplicates should not be present and might be removed. 
		This is due to the fact that values can come from multiple ETL flows and considering order or
		duplicates is usually both hard and not needed.
		
		If you really need to store these features, consider other approaches, eg, nodes with properties
		like index, value, repetitions.
		"""

		SINGLE = "single"
		"""
		The property is always single-valued, if an array is met that contains more than one value, an error is raised.
		"""		
		
		MULTIPLE = "multiple"
		"""
		The property is always multiple-values, all the values are converted into a set or list, even
		for singletons.
		"""

		AUTO = "auto"
		"""
		The property can either have single or multiple values, depending on the size of the array
		that is found in PG-JSONL, it is converted into a single value if it is a singleton, or into 
		a set/list if it has a bigger array.

		This is the default, but we don't recommend it, since you'll have to deal with this polymorphism
		in the target database, and your semantics is rarely like this.
		"""

	multi_value_mode: MultiValueMode = MultiValueMode.AUTO

@dataclass
class NeoLoaderConfig:
	# TODO: field() is needed for mutable defaults, but it feels like mumbo-jumbo, maybe
	# we should go back to regular classes.
	property_configs: dict[str, NeoLoaderPropertyConfig] = field( default_factory = dict )
	"""
	A dictionary of property ID -> configuration for that property.
	See :class:`NeoLoaderPropertyConfig` for details.
	"""

	default_property_config: NeoLoaderPropertyConfig = field( default_factory = NeoLoaderPropertyConfig )
	"""
	Default fallback configuration for properties not present in `property_configs`.
	This might useful in tests, to change the default config for all properties in one go.
	"""

	loader_batch_size: int = NeoLoaderDefaults.BATCH_SIZE
	"""
	Neo4j loads one batch of nodes or edges per transaction, and this is the batch size.
	"""
	
	loader_max_concurrency: int = NeoLoaderDefaults.MAX_CONCURRENCY
	"""
	The maximum number of concurrent batch loaders. The loader has this number of 
	parallel JSON parsers and this number of concurrent batch loading transactions (see the main_loop` 
	function inside `async_pg_jsonl_neo_loader` for details). It uses the common default of all the 
	available CPU cores minus one (left to the main thread). You should be fine with this, except, 
	maybe, in cases like tests or debugging.	
	"""

	def get_property_config ( self, property_id: str ) -> NeoLoaderPropertyConfig:
		if not property_id in self.property_configs:
			return self.default_property_config
		return self.property_configs [ property_id ]
	
	def get_property_ids ( self ) -> KeysView[str]:
		return self.property_configs.keys()


async def async_pg_jsonl_neo_loader (
	pg_jsonl_source: str|Path|TextIO|Iterable[Any]|None,
	neo_driver: neo4j.AsyncDriver,
	do_nodes = NeoLoaderDefaults.DO_NODES,
	do_edges = NeoLoaderDefaults.DO_EDGES,
	config: NeoLoaderConfig = NeoLoaderConfig(),
	done_base_path: str|Path|None = None
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


	## Parameters

	- pg_jsonl_source: the source of the JSONL/PG data. 
	This is passed to :func:`ketl.async_reader_helper`, so it can be a file path, a file-like object,
	a string of PG-JSON data, None (to get data from the stdin). **WARNING**: as said above, when 
	both `do_nodes` and `do_edges` are set, the `pg_jsonl_source` can't be neither an iterator 
	nor None/stdin.

	- neo_driver: the Neo4j async driver to use for loading the data.

	- do_nodes, do_edges: whether to load nodes and edges, respectively. You can choose to load node definitions
	or edge definitions only, which can be useful for testing or to have more incremental updates in a SnakeMake
	pipeline. **WARNING**: you **can't** load edges that refer to nodes not loaded in the database, see above.

	- config: the configuration object for the NeoLoader, containing settings like batch size and maximum concurrency.

	- done_base_path: if not None, the base path for the "done" files to be created when the loading is done.
	This is useful in SnakeMake pipelines, to tell a rule that the loading is done. When nodes loading is completed,
	`{base_path}.nodes` is created and `{base_path}.edges is created when the edges are loaded, which means
	you can continue a previous loading from the edges only. Being flag files, their content is irrelevant.


	## Returns

	The number of nodes+edges loaded.

	
	## Notes

	Note that we don't use any :class:`ketl.core.ValueConverter` here and no unserialisation from strings, 
	since we assume the PG-JSONL already has values in a format that can always be translated to Neo4j. 
	This is motivated by the fact that conversions are made by upstream functions in KETL, eg, 
	:func:`ketl.io.core.pg_df_2_pg_jsonl`.
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
		The generic reader, passed to :func:`ketl.async_reader_helper`.

		This reads the source of PG nodes or edges, batches them into batches of `loader_batch_size` elements, 
		and sends each batch to the provided `batch_loader`, which is responsible for loading the batch into the 
		Neo4j database and returning the number of loaded elements.

		Multiple batches are processed asynchronously, with a maximum concurrency of `loader_max_concurrency`, 
		to avoid memory overflows and database pressure.

		When the input is exhausted, waits for all the batch loaders to complete and returns the total number 
		of loaded elements.
		"""

		def parse_pg_elem_property ( prop_id: str, pg_value: list[Any], elem_id: str ) -> dict[str, Any]:
			"""
			Parses a PG property value, applying the configuration for that property as needed.

			At the moment, the only configuration is how to deal with multiple values, but more can be added in the future.
			"""
			if pg_value is None: return None # TODO: can it happen?
			if not isinstance ( pg_value, list ):
				raise ValueError ( f"pg_jsonl_neo_loader(), property '{prop_id}' in element '{elem_id}' has a non-list value" )

			# Remove None values from the list. TODO: can it happen?
			pg_value = [ v for v in pg_value if v is not None ]
			if len ( pg_value ) == 0: return None

			prop_config = config.get_property_config ( prop_id )
			result = None

			if len ( pg_value ) == 1:
				if prop_config.multi_value_mode in ( NeoLoaderPropertyConfig.MultiValueMode.SINGLE, NeoLoaderPropertyConfig.MultiValueMode.AUTO ):
					result = pg_value [ 0 ]
				else:
					# it's multiple mode
					result = pg_value
			else:
				# > 1 case
				if prop_config.multi_value_mode == NeoLoaderPropertyConfig.MultiValueMode.SINGLE:
					raise ValueError ( f"pg_jsonl_neo_loader(), multiple values aren't allowed for property '{prop_id}' in element '{elem_id}'" )
				# else, we're in auto or multiple
				result = list ( set ( pg_value ) ) # Remove duplicates, and convert to list for better Neo4j compatibility.

			return result

		def parse_jsonl_batch ( batch: list[str] ) -> list[dict[str, Any]]:
			# Parse it all, it's faster
			js_batch = json.loads ( "[" + ",".join ( js_line for js_line in batch ) + "]" )

			# Apply the property configurations to each element
			for elem in js_batch:
				elem_id = elem [ "id" ]
				# We need a copy, since we might change the dict
				for prop_id in list ( elem.get ( "properties", {} ).keys () ):
					pg_value = elem [ "properties" ][ prop_id ]
					new_value = parse_pg_elem_property ( prop_id, pg_value, elem_id )
					if new_value is None:
						# Just don't store it
						elem [ "properties" ].pop ( prop_id )
					elif new_value != pg_value:
						elem [ "properties" ][ prop_id ] = new_value
					# else, keep it
			return js_batch

		executor = concurrent.futures.ThreadPoolExecutor ( max_workers = config.loader_max_concurrency )
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
			batch = list ( islice ( pg_elems_source, config.loader_batch_size ) )
			if not batch: break

			loop = asyncio.get_running_loop()
			# Parse/transform the JSON lines in parallel
			pg_elems: list[dict[str, Any]] = await loop.run_in_executor ( executor, parse_jsonl_batch, batch )

			# Do the loading asynchronously. So, we have parallel parsers added to the main thread running the source
			# scanning plus the loaders in the event loop.
			n_loaded += await batch_loader ( pg_elems )
			progress_logger.update ( n_loaded )

		write_done_file ( is_nodes_mode )
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


	def write_done_file ( is_nodes_mode: bool ) -> None:
		"""
		Manages the `done_base_path` option, as described in the docstring above.
		"""
		if done_base_path is None: return

		done_path = Path ( done_base_path )
		done_suffix = "nodes" if is_nodes_mode else "edges"
		done_path = done_path.with_name ( done_path.name + "." + done_suffix )
		try:
			done_path.touch ( exist_ok = True )
		except IOError as ex:
			raise IOError ( f"pg_jsonl_neo_loader(), failed to create the done file '{done_path}': {ex}", cause = ex )


	#### The top-level orchestrator
	#

	# Some consistency check
	source_has_seek = hasattr ( pg_jsonl_source, "seek" ) and callable ( pg_jsonl_source.seek )
	if do_edges and do_nodes:
		if not ( isinstance ( pg_jsonl_source, (str, Path) ) or source_has_seek ):
			raise ValueError (
				f"pg_jsonl_neo_loader(), attempt to load both nodes and edges with a non-rewindable source."
				+ " Pass me a string, a path or a rewindable file-like object."
			)

	# Some heading in the log
	source_str = None
	if isinstance ( pg_jsonl_source, Path ):
		source_str = f'"{pg_jsonl_source}"'
	elif pg_jsonl_source is None:
		source_str = "<stdin>"
	else:
		source_str = f" a {type ( pg_jsonl_source )} object"

	log.info ( f"|==== pg_jsonl_neo_loader(), loading from {source_str}" )

	n_nodes = n_edges = 0

	if not do_nodes:
		log.info ( f"pg_jsonl_neo_loader(), do_nodes not set, skipping nodes loading" )
	else:
		log.info ( f"|== Loading Nodes" )
		progress_logger.log_message_template = "%d knowledge graph nodes loaded"
		# See the async_reader_helper docstring on why we need the async flavour of this helper
		# when the reader is async.
		n_nodes = await async_reader_helper (
			lambda src: main_loop ( src, is_nodes_mode = True, batch_loader = nodes_load ),
			pg_jsonl_source
		)
		log.info ( f"|== A total of {n_nodes} node(s) loaded." )

	if not do_edges:
		log.info ( f"pg_jsonl_neo_loader(), do_edges not set, skipping edges loading" )
	else:
		log.info ( f"|== Loading Edges" )

		# Rewind as needed
		if do_nodes and source_has_seek:
			try:
				pg_jsonl_source.seek ( 0 )
			except IOError as ex:
				raise IOError ( f"pg_jsonl_neo_loader(), failed to rewind the source for edge loading: {ex}", cause = ex )

		# Restart the progress logger from 0 edges and with a new message
		progress_logger.reset ()
		progress_logger.log_message_template = "%d knowledge graph edges loaded" 
		n_edges = await async_reader_helper (
			lambda src: main_loop ( src, is_nodes_mode = False, batch_loader = edges_load ),
			pg_jsonl_source
		)
		log.info ( f"|== A total of {n_edges} edge(s) loaded." )

	log.info ( f"|==== pg_jsonl_neo_loader(), a total of {n_nodes + n_edges} element(s) loaded. All done!" )

	return n_nodes + n_edges


def pg_jsonl_neo_loader (
	pg_jsonl_source: str|Path|TextIO|Iterable[Any]|None,
	neo_driver: neo4j.AsyncDriver,
	do_nodes = NeoLoaderDefaults.DO_NODES,
	do_edges = NeoLoaderDefaults.DO_EDGES,
	config: NeoLoaderConfig = NeoLoaderConfig(),
	done_base_path: str|Path|None = None
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
		pg_jsonl_source, neo_driver, do_nodes, do_edges, config, done_base_path
	))
