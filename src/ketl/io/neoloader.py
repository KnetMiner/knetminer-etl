"""
# The PG-JSONL Neo4j Loader

This module provides functions to load the JSONL serialisations of the [PG format](https://pg-format.github.io/).

Both an async version (:func:`ketl.async_pg_jsonl_neo_loader`) and a synchronous wrapper 
(:func:`ketl.pg_jsonl_neo_loader`) are defined, as well as a CLI interface (:func:`ketl.pg_jsonl_neo_loader_cli`).

Other components in the module allows for configuring how node and relationship properties should mapped into
Neo4j (eg, multiple-value handling), loading tuning-related settings and reading configuration options from 
YAML files.

TODO: This is worth a standalone project, to be put in its own github repo and made
independent on ketl (of course, ketl would become a dependendant).

TODO: for future documentaton, see some related work (last update, 20260410):
- [pg-format-py](https://github.com/pg-format/pg-format-py), very simple, can't work for large inputs.
- [Parallel Spark Neo4j Loader](https://github.com/neo4j-field/neo4j-parallel-spark-loader), interesting, focuses
on injesting from Spark DFs. We aim at mapping to PG first and then loading, but TODO: we should test our loader
with a DF as source.
"""

import asyncio
import concurrent
import json
import logging
import os
import re
from collections.abc import KeysView
from dataclasses import dataclass, field
from datetime import timedelta
from enum import StrEnum
from itertools import islice
from pathlib import Path
import sys
from typing import Any, Awaitable, Callable, Iterable, TextIO

import neo4j
import tenacity
from brandizpyes.io import async_reader_helper
from brandizpyes.logging import ProgressLogger

import argparse

from ketl.config import load_config

log = logging.getLogger ( __name__ )


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
	@classmethod
	def from_config ( cls, config: dict ) -> "NeoLoaderPropertyConfig":
		"""
		Instantiates a new config from a configuration dictionary, in the format shown 
		in `/tests/resources/test-config.yml`

		Example:
		```yaml
		default_property_config:
			multi_value_mode: multiple
		```

		Note that this should receive only the object describing a property, **not** the outer key/value
		pair.
		"""
		if not config: return cls()
		params = config.copy ()

		if "multi_value_mode" in params:
			params [ "multi_value_mode" ] = cls.MultiValueMode ( params [ "multi_value_mode" ] )

		return cls ( **params )

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

	loader_batch_size: int = 2500
	"""
	Neo4j loads one batch of nodes or edges per transaction, and this is the batch size.
	"""
	
	loader_max_concurrency: int = max(1, os.cpu_count() - 1)
	"""
	The maximum number of concurrent batch loaders. The loader has this number of 
	parallel JSON parsers and this number of concurrent batch loading transactions (see the main_loop` 
	function inside `async_pg_jsonl_neo_loader` for details). It uses the common default of all the 
	available CPU cores minus one (left to the main thread). You should be fine with this, except, 
	maybe, in cases like tests or debugging.	
	"""

	max_transaction_retries: int = 10
	"""
	Some DB writes might need to be retried when certain internal errors happen (eg, collisions between 
	concurrent transactions). To deal with it, we retry failed transactions after a random pause 
	(see :attr:`max_retry_pause`) and up to this no of times.

	Likely, you're fine with the default in most common cases, we change it in special cases 
	like tests.
	"""

	max_retry_pause: timedelta = timedelta ( minutes = 2 )
	"""
	The max pause between transaction retries, see :attr:`max_transaction_retries`.
	The pause between retries is between a minimum of 2s and this maximum.

	Again, you should be fine with the default, unless you're banging you hea... 
	doing special things like tests.
	"""

	def get_property_config ( self, property_id: str ) -> NeoLoaderPropertyConfig:
		if not property_id in self.property_configs:
			return self.default_property_config
		return self.property_configs [ property_id ]
	
	def get_property_ids ( self ) -> KeysView[str]:
		return self.property_configs.keys()
	
	@classmethod
	def from_config ( cls, config: dict ) -> "NeoLoaderConfig":
		"""
		Instantiates a new config from a configuration dictionary, in the format shown 
		in `/tests/resources/test-config.yml`

		Example:
		```yaml
		neoloader:
			default_property_config:
				multi_value_mode: multiple
			property_configs:
				has_pvalue:
					multi_value_mode: single
			loader_batch_size: 3000      
			loader_max_concurrency: 8
			max_transaction_retries: 3
			max_retry_pause:
				seconds: 10
				minutes: 0
		```

		Note that this should receive the contents of the `neoloader` key, **not** the whole
		configuration object.
		"""

		if not config: return cls()
		params = config.copy ()

		# Let's convert the keys that can't be given as-is to the constructor
		if "default_property_config" in params:
			params [ "default_property_config" ] = NeoLoaderPropertyConfig.from_config ( params [ "default_property_config" ] )
		if "property_configs" in params:
			params [ "property_configs" ] = { 
				prop_id: NeoLoaderPropertyConfig.from_config ( prop_config ) 
				for prop_id, prop_config in params [ "property_configs" ].items ()
			}
		if "max_retry_pause" in params:
			max_retry_pause = params [ "max_retry_pause" ]
			try:
				params [ "max_retry_pause" ] = timedelta ( **max_retry_pause )
			except Exception as ex:
				raise ValueError ( f"Invalid max_retry_pause configuration: {max_retry_pause}" ) from ex
		return cls ( **params )


async def async_pg_jsonl_neo_loader (
	pg_jsonl_source: str|Path|TextIO|Iterable[Any]|None,
	neo_driver: neo4j.AsyncDriver,
	do_nodes = True,
	do_edges = True,
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

	- `pg_jsonl_source`: the source of the JSONL/PG data. 
	This is passed to :func:`ketl.async_reader_helper`, so it can be a file path, a file-like object,
	a string of PG-JSON data, None (to get data from the stdin). **WARNING**: as said above, when 
	both `do_nodes` and `do_edges` are set, the `pg_jsonl_source` can't be neither an iterator 
	nor None/stdin.

	- `neo_driver`: the Neo4j async driver to use for loading the data.

	- `do_nodes`, `do_edges`: whether to load nodes and edges, respectively. You can choose to load node definitions
	or edge definitions only, which can be useful for testing or to have more incremental updates in a SnakeMake
	pipeline. **WARNING**: you **can't** load edges that refer to nodes not loaded in the database, see above.

	- `config`: the configuration object for the NeoLoader, containing settings like batch size and maximum concurrency.

	- `done_base_path`: if not None, the base path for the "done" files to be created when the loading is done.
	This is useful in SnakeMake pipelines and alike, to tell a rule that the loading is done. When nodes loading 
	is completed, `{base_path}.nodes` is created and `{base_path}.edges is created when the edges are loaded, 
	which means you can continue a previous loading from the edges only. Being flag files, their content is irrelevant.
	
	**Note**: if `done_base_path` is given, there are other behaviours that are triggered:
	* '.nodes' or '.edges' trailers are removed from the parameter. That's because workflows like SnakeMake
	needs to specify the whole path to expect as a rule output, while here we want to be able to add
	these trailers based on the kind of loading we're doing.
	* if the '.nodes' or '.edges' files already exist, we **don't do the corresponding loading**. This
	allows for writing a single rule in a system like SnakeMake that only expects the '.edges' file, 
	we internally do a bit of progressive loading.
  

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

		# TODO: ProcessPoolExecutor, requires nested functions to be moved on top
		n_loaded = 0

		type_filter = "node" if is_nodes_mode else "edge"
		type_re = f"""("type"|'type'):\\s*("{type_filter}"|'{type_filter}')"""
		type_re = re.compile ( type_re )

		# Switch to a filtered iterable
		pg_elems_source = ( line for line in pg_elems_source if line and re.search ( type_re, line ) )

		# We have seen that for up to 1M nodes, the ThreadPoolExeutor is actually slightly faster,
		# presumably because there isn't much CPU work, the batch size isn't very big, and a few
		# operations (eg, json.loads()) releases the GIL.
		# However, since things might be different with different inputs, we're leaving the 
		# InterpreterPoolExecutor. TODO: at least for now.
		with concurrent.futures.InterpreterPoolExecutor ( max_workers = config.loader_max_concurrency ) \
		as executor:
			while True:
				batch = list ( islice ( pg_elems_source, config.loader_batch_size ) )
				if not batch: break

				loop = asyncio.get_running_loop()
				# Parse/transform the JSON lines in parallel
				pg_elems: list[dict[str, Any]] = await loop.run_in_executor (
					executor, _parse_jsonl_batch, batch, config
				)

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


	# It's known that async/parallel edge creations cause transaction collisions, 
	# so, we added this retry thing.
	@tenacity.retry ( 
		stop = tenacity.stop_after_attempt ( config.max_transaction_retries ),
		# Wait a random time of 2s - max time between retries
		wait = tenacity.wait_random ( min = timedelta ( seconds = 2 ), max = config.max_retry_pause ),
		reraise = neo4j.exceptions.TransientError,
		before_sleep = lambda retry_state: log.warning ( 
			f"pg_jsonl_neo_loader(), edge batch loading failed due to: {retry_state.outcome.exception()}, " + 
			f"attempting {config.max_transaction_retries - retry_state.attempt_number} more time(s)"
		)
	)
	async def edges_load ( edges_batch: list[dict[str, Any]] ) -> int:
		"""
		The batch loader for edges.

		Similarly to the node loader, receives a batch of PG edges (as dictionaries), 
		and creates them via Cypher, managing retries as needed.

		Returns the number of loaded edges.
		"""
		# TODO: this ignores non-existing nodes :-(
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
		if not ( isinstance ( pg_jsonl_source, (str, Path, Iterable) ) or source_has_seek ):
			raise ValueError (
				f"pg_jsonl_neo_loader(), attempt to load both nodes and edges with a non-rewindable source."
				+ " Pass me a string, a path or a rewindable file-like object."
			)
		
	# Some more initialisation
	(is_nodes_done_flag, is_edges_done_flag) = (False, False)
	if done_base_path is not None:
		done_base_path = str ( done_base_path )
		if done_base_path.endswith ( ".nodes" ) or done_base_path.endswith ( ".edges" ):
			done_base_path = done_base_path.rsplit ( ".", 1 )[ 0 ]
		
		is_nodes_done_flag = Path ( done_base_path + ".nodes" ).exists ()
		is_edges_done_flag = Path ( done_base_path + ".edges" ).exists ()

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
	elif is_nodes_done_flag:
		log.info ( f"pg_jsonl_neo_loader(), node done file \"{done_base_path}.nodes\" exists, skipping nodes loading" )
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
	elif is_edges_done_flag and is_nodes_done_flag:
		# Edge loading must happen anyway if the nodes have just been reloaded
		# Unfortunately, this won't work in a rule checking .edges only, but we have it just in case
		log.info ( f"pg_jsonl_neo_loader(), edge done file \"{done_base_path}.edges\" exists, skipping edges loading" )
	else:
		log.info ( f"|== Loading Edges" )

		# Rewind as needed
		if do_nodes:
			if isinstance ( pg_jsonl_source, Iterable ) and not isinstance ( pg_jsonl_source, (str, Path) ):
				try:
					# This is mainly to cover lists passed by tests, an iterable can be restarted by getting its
					# iterator.
					pg_jsonl_source = iter ( pg_jsonl_source )
				except Exception as ex:
					raise ValueError ( f"pg_jsonl_neo_loader(), failed to get an iterator from the source for edge loading: {ex}", cause = ex )
			elif source_has_seek:
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
	do_nodes = True,
	do_edges = True,
	config: NeoLoaderConfig = NeoLoaderConfig(),
	done_base_path: str|Path|None = None
) -> int:
	"""
	Loads a JSONL/PG file (see :func:`ketl.pg_df_2_pg_jsonl`) into a Neo4j database, through the provided driver.

	This is only a synchronous wrapper for :func:`ketl.async_pg_jsonl_neo_loader`, the meat is there, 
	including detailed documentation.

	This wrapper runs the async loader in a fresh event loop, created in a fresh new thread. This 
	is a safe way to avoid horrible conflicts with already running event loops in the current thread
	or contexts (we have had quite some pain with pytest and Snakemake, we know other workflow systems,
	Jupyter and more can cause similar issues).
	
	**WARNING**: this also means that calling this function too many times **is not efficient**. 
	If you need that, call the async version from your own sync/async management context (good luck...).

	TODO: this might actually be unsafe when the driver is created in a different event loop, add
	support to a [factory function](https://chatgpt.com/share/69f20131-2034-832e-89cd-625d7b35e3e3)
	for the driver. 
	"""
	
	with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
		future = executor.submit ( 
			asyncio.run,
			async_pg_jsonl_neo_loader (
				pg_jsonl_source, neo_driver, do_nodes, do_edges, config, done_base_path
			)				
		)
		return future.result()


def create_neo_driver_from_config ( config: dict, is_async: bool = False ) -> neo4j.Driver|neo4j.AsyncDriver:
	"""
	Instantiates a Neo4j driver from a configuration dictionary, in the format shown 
	in `/tests/resources/test-config.yml`

	Example:

	```yaml
	neo4j:
		uri: bolt://neo.somewhere.net:7687
		auth:
			user: neo4j
			password: ${NEO4J_PASSWORD}
		connection_timeout: 15
	```

	Note that this should receive the contents of the `neo4j` key, **not** its container.
	"""
	params = config.copy () if config else {
		"uri": "bolt://localhost:7687",
		"auth": {
			"user": "neo4j",
			"password": "neo4j"
		}
	}

	if "auth" in params:
		params [ "auth" ] = ( params [ "auth" ].get ( "user" ), params [ "auth" ].get ( "password" ) )

	if is_async:
		return neo4j.AsyncGraphDatabase.driver ( **params )
	
	return neo4j.GraphDatabase.driver ( **params )


def pg_jsonl_neo_loader_cli ( args: list[str], do_sys_exit: bool = True ) -> None:
	"""
	A CLI wrapper for the NeoLoader.

	See :func:`ketl.async_pg_jsonl_neo_loader` for details.

	`do_sys_exit` is mostly for testing, when set, the function exits with the appropriate status code, 
	otherwise, it just returns the status code.
	"""

	async def run_loader_and_close_driver ( loader_call: Awaitable[int], neo_driver: neo4j.AsyncDriver ) -> int:
		"""
		Wraps a call to the loader into a try/finally that closes the driver at the end.
		
		This isn't needed in most cases, since the CLI exits straight after the loader and the 
		driver auto-closes. Yet, we added it as a precaution.

		Note that we can't just close an async driver, nor can we await driver.close() in a non 
		async context, due to the coloured functions rubbish.
		"""
		try:
			return await loader_call ()
		finally:
			await neo_driver.close ()

	parser = argparse.ArgumentParser ( 
		description = "=== The PG-JSONL Neo4j Loader ===", 
		exit_on_error = do_sys_exit, suggest_on_error = True,
		add_help = False # Cause we need our own handler, see below
	)
	parser.add_argument (
		"source", 
		help = "Source path of the PG-JSONL data. Reads from stdin if null (NOT compatible with loading " 
		     + "both nodes and edges, requires either --no-nodes or --no-edges).", 
		nargs = "?",
		metavar = "<path>" 
	)
	parser.add_argument ( "--neo-uri", "-r", help = "URI of the Neo4j database (overrides value in --config)", metavar = "<URI>" )
	parser.add_argument ( "--neo-user", "-u", help = "Neo4j user (overrides value in --config)", metavar = "<user>" )
	parser.add_argument ( "--neo-password", "-p", help = "Neo4j password (overrides value in --config)", metavar = "<password>" )
	parser.add_argument ( "--no-nodes", "-n", help = "Skip nodes (default: loads them). WARNING: edges MUST refer to existing nodes", action = "store_true" )
	parser.add_argument ( "--no-edges", "-e", help = "Skip edges (default: loads them).", action = "store_true")
	parser.add_argument ( "--done-path", "-f", help = "Base path for the flag file indicating nodes/edges were loaded (.nodes/.edges are appended)", metavar = "<base path>" )
	parser.add_argument ( "--config", "-c", help = "Path to the configuration file, allows for fine-tuning, see examples in /tests/resources", metavar = "<path>" )

	parser.add_argument ('--help', '-h', action = "store_true", help="Shows this help message and exits (with status 2)" )

	parsed_args, unknown = parser.parse_known_args ( args )
	if parsed_args.help or unknown:
		parser.print_help ()
		if do_sys_exit: sys.exit ( 2 )
		else: return 2

	exit_code = 0

	# Get the config
	config = load_config ( Path ( parsed_args.config ) ) if parsed_args.config \
	else {} # else use the defaults

  # Neo defaults
	if not "neo4j" in config:
		config [ "neo4j" ] = {
			"uri": "bolt://localhost:7687"
		}
	if not "auth" in config [ "neo4j" ]:
		config [ "neo4j" ] [ "auth" ] = {
			"user": "neo4j",
			"password": "neo4j"
		}
	
	# These CLI args override the config (ie, the defaults or the loaded file)
	if parsed_args.neo_uri:
		config [ "neo4j" ] [ "uri" ] = parsed_args.neo_uri
	if parsed_args.neo_user:
		config [ "neo4j" ] [ "auth" ] [ "user" ] = parsed_args.neo_user
	if parsed_args.neo_password:
		config [ "neo4j" ] [ "auth" ] [ "password" ] = parsed_args.neo_password

	neo_driver = create_neo_driver_from_config ( config.get ( "neo4j" ), is_async = True )
	
	# If not available, it just creates a default object
	config = NeoLoaderConfig.from_config ( config.get ( "neoloader" ) )

	# If both nodes and edges, the loader will fail, we're delegating the error handling to it
	source = Path ( parsed_args.source ) if parsed_args.source else None

	try:
		# As said above, we need to wrap this as follow.
		#
		loader_call = lambda: async_pg_jsonl_neo_loader (
			source,
			neo_driver, 
			do_edges = not parsed_args.no_edges,
			do_nodes = not parsed_args.no_nodes,
			done_base_path = parsed_args.done_path,
			config = config
		)
		asyncio.run ( run_loader_and_close_driver ( loader_call, neo_driver ) )
	except Exception as ex:
		exit_code = 1
		log.error ( f"pg_jsonl_neo_loader_cli(), loading failed: {ex}", exc_info = True )
	
	if do_sys_exit:
		sys.exit ( exit_code )
	else:
		return exit_code



def _parse_jsonl_batch ( batch: list[str], config: NeoLoaderConfig ) -> list[dict[str, Any]]:
	"""
	Helper used by :func:`async_pg_jsonl_neo_loader` to parse a batch of PG-JSONL lines into 
	a list of dictionaries. This might be passed to `ProcessPoolExecutor`, so it can't be 
	a nested function.
	"""
	js_batch = json.loads ( "[" + ",".join ( js_line for js_line in batch ) + "]" )

	# Apply the property configurations to each element
	for elem in js_batch:
		elem_id = elem [ "id" ]
		# We need a copy, since we might change the dict
		for prop_id in list ( elem.get ( "properties", {} ).keys () ):
			pg_value = elem [ "properties" ][ prop_id ]
			new_value = _parse_pg_elem_property ( prop_id, pg_value, elem_id, config )
			if new_value is None:
				# Just don't store it
				elem [ "properties" ].pop ( prop_id )
			elif new_value != pg_value:
				elem [ "properties" ][ prop_id ] = new_value
			# else, keep it
	return js_batch


def _parse_pg_elem_property ( 
	prop_id: str, pg_value: list[Any], elem_id: str, config: NeoLoaderConfig 
) -> dict[str, Any]:
	"""
	Parses a PG property value, applying the configuration for that property as needed.

	At the moment, the only configuration concerns how to deal with multiple values, but more could be added 
	in the future.

	This is used by :func:`_parse_jsonl_batch`, which in turn, might be passed to the `ProcessPoolExecutor`, 
	so it can't be a nested function.
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

if __name__ == "__main__":
	# Of course, we call the CLI loader when the module is invoked, so you can do:
	#
	#   python -m ketl.io.neoloader ...
	#
	pg_jsonl_neo_loader_cli ( sys.argv [ 1: ] )
