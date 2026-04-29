
import json
import logging
import os
import random
from datetime import timedelta
from pathlib import Path
from typing import Any, Generator, Iterable

import neo4j
import pytest
from assertpy import assert_that
from testcontainers.neo4j import Neo4jContainer

from ketl.io.neoloader import (NeoLoaderConfig, NeoLoaderPropertyConfig,
                               pg_jsonl_neo_loader, pg_jsonl_neo_loader_cli)

log = logging.getLogger ( __name__ )

def create_multiple_multi_value_config() -> NeoLoaderConfig:
	"""
	Initially test in multi-valued mode, which just preserves the PG-JSONL input as is.

	This is a function that creates a new config, because some tests need to change this initial config.
	"""
	return NeoLoaderConfig (
		default_property_config = NeoLoaderPropertyConfig ( 
			multi_value_mode = NeoLoaderPropertyConfig.MultiValueMode.MULTIPLE
		)
	)


@pytest.mark.integration
def test_nodes_loading ( 
	pg_data: tuple[ list[ dict ], list[ dict ] ], neo4j_container: Neo4jContainer, neo_driver: neo4j.Driver 
):
	"""
	Tests for :py:func:`ketl.pg_jsonl_neo_loader()`.

	It uses the async driver with the loader and the sync one to verify the results via Cypher.
	"""

	pg_nodes = pg_data[ 0 ]
	pg_nodes_str = "\n".join ( json.dumps ( node ) for node in pg_nodes )

	async_neo_driver: neo4j.AsyncDriver = create_async_neo_driver ( neo4j_container )

	n_nodes = pg_jsonl_neo_loader (
		pg_jsonl_source = pg_nodes_str,
		neo_driver = async_neo_driver,
		do_nodes = True, do_edges = False,
		config = create_multiple_multi_value_config ()
	)

	assert_that ( n_nodes, "Return value from the loader is correct" ).is_equal_to ( len ( pg_nodes ) )

	# Verify via Cypher
	for node in pg_nodes:
		node_query = f"""
		MATCH (n {{ id: '{node[ "id" ]}' }})
		RETURN n
		"""
		with neo_driver.session() as session:
			result = session.run ( node_query )
			record = result.single()
			assert_that ( record, f"Node {node['id']} is found in the database" ).is_not_none ()
			db_node = record[ "n" ]
			assert_that ( db_node.labels, f"Node {node['id']} has correct labels in the database" )\
				.contains_only ( *node[ "labels" ] )
			for prop_key, prop_values in node[ "properties" ].items():
				assert_that ( db_node.get ( prop_key ), f"Node {node['id']} has property '{prop_key}' in the database" ).is_not_none ()
				assert_that ( set ( db_node.get ( prop_key ) ), f"Node {node['id']} has correct values for property '{prop_key}' in the database" )\
					.is_equal_to ( set ( prop_values ) )


@pytest.mark.integration
def test_edges_loading ( 
	pg_data: tuple[ list[ dict ], list[ dict ] ], neo4j_container: Neo4jContainer, neo_driver: neo4j.Driver
):
	"""
	Tests for :py:func:`ketl.pg_jsonl_neo_loader()` with edges.
	"""

	pg_nodes, pg_edges = pg_data

	pg_nodes_str = "\n".join ( json.dumps ( node ) for node in pg_nodes )
	pg_edges_str = "\n".join ( json.dumps ( edge ) for edge in pg_edges )
	pg_all_str = pg_nodes_str + "\n" + pg_edges_str

	async_neo_driver: neo4j.AsyncDriver = create_async_neo_driver ( neo4j_container )

	# Load both the nodes and the edges. Not only is this necessary, but it also verifies that 
	# the loader can do both in one invocation.
	n_edges = pg_jsonl_neo_loader (
		pg_jsonl_source = pg_all_str,
		neo_driver = async_neo_driver,
		do_nodes = True, do_edges = True,
		# As for the nodes, initially test the simplest multi-valued mode
		config = create_multiple_multi_value_config ()
	) - len ( pg_nodes ) # The loader returns the total number of created elements.
	
	assert_that ( n_edges, "Return value from the loader is correct" ).is_equal_to ( len ( pg_edges ) )
	
	# Verify via Cypher
	for edge in pg_edges:
		cy_type = edge[ "labels" ] [ 0 ] 
		edge_query = f"""
		MATCH (from)-[r:`{cy_type}` {{ id: '{edge[ "id" ]}' }}]->(to)
		WHERE from.id = '{edge[ "from" ]}' AND to.id = '{edge[ "to" ]}'
		RETURN r
		"""
		with neo_driver.session() as session:
			result = session.run ( edge_query )
			record = result.single()
			assert_that ( record, f"Edge {edge['id']} is found in the database" ).is_not_none ()
			db_edge = record[ "r" ]
			assert_that ( db_edge.type, f"Edge {edge['id']} has correct type in the database" )\
				.is_equal_to ( edge[ "labels" ][ 0 ] )
			for prop_key, prop_values in edge[ "properties" ].items():
				assert_that ( db_edge.get ( prop_key ), f"Edge {edge['id']} has property '{prop_key}' in the database" )\
					.is_not_none ()
				assert_that ( 
					set ( db_edge.get ( prop_key ) ),
					f"Edge {edge['id']} has correct values for property '{prop_key}' in the database" 
				).is_equal_to ( set ( prop_values ) )

	# Further verify that the created relationships link the expected nodes
	for edge in pg_edges:
		edge_query = f"""
		MATCH (from)-[r {{ id: '{edge[ "id" ]}' }}]->(to)
		WHERE from.id = '{edge[ "from" ]}' AND to.id = '{edge[ "to" ]}'
		RETURN from, to
		"""
		with neo_driver.session() as session:
			result = session.run ( edge_query )
			record = result.single()
			assert_that ( record[ "from" ].get ( "id" ), f"Edge {edge['id']} links the expected source node" )\
				.is_equal_to ( edge[ "from" ] )
			assert_that ( record[ "to" ].get ( "id" ), f"Edge {edge['id']} links the expected target node" )\
				.is_equal_to ( edge[ "to" ] )	


def test_loading_from_file ( 
	pg_data: tuple[ list[ dict ], list[ dict ] ], 
	tmp_path: Path,
	neo4j_container: Neo4jContainer, neo_driver: neo4j.Driver
):
	"""
	Tests loading from a file (the other tests uses in-memory data).
	"""

	pg_nodes, pg_edges = pg_data
	pg_nodes_str = "\n".join ( json.dumps ( node ) for node in pg_nodes )
	pg_edges_str = "\n".join ( json.dumps ( edge ) for edge in pg_edges )
	pg_all_str = pg_nodes_str + "\n" + pg_edges_str

	tmp_input_path = tmp_path / "neo-loader-test-pg.jsonl"
	tmp_input_path.write_text ( pg_all_str )

	async_neo_driver: neo4j.AsyncDriver = create_async_neo_driver ( neo4j_container )

	n_nodes = pg_jsonl_neo_loader (
		pg_jsonl_source = tmp_input_path,
		neo_driver = async_neo_driver,
		do_nodes = True, do_edges = True
	)

	assert_that ( n_nodes, "Return value from the loader is correct" ).is_equal_to ( len ( pg_nodes ) + len ( pg_edges ) )


@pytest.mark.integration
def test_large_input_nodes ( neo4j_container: Neo4jContainer, neo_driver: neo4j.Driver ):
	async_neo_driver: neo4j.AsyncDriver = create_async_neo_driver ( neo4j_container )

	input_size = 50000
	# The input is a stream and not a file, the internal reader is flexible with various input sources
	nodes_stream = ( 
		json.dumps ( {"type": "node", "id": f"N{i}", "labels": ["TestNode"], "properties": {"index": [i]} } ) 
		for i in range ( input_size )
	)
	n_nodes = pg_jsonl_neo_loader (
		pg_jsonl_source = nodes_stream,
		neo_driver = async_neo_driver,
		do_nodes = True, do_edges = False
	)

	assert_that ( n_nodes, "Return value from the loader is correct" ).is_equal_to ( input_size )

	# Cypher count is right
	with neo_driver.session() as session:
		result = session.run ( "MATCH (n:TestNode) RETURN count(n) AS count" )
		record = result.single()
		assert_that ( record, "Count query returns a record" ).is_not_none ()
		count = record[ "count" ]
		assert_that ( count, "Count of loaded nodes in the database is correct" ).is_equal_to ( input_size )

		# And some expected nodes are there
		n_tests = 10
		n_test_win_size = input_size // n_tests
		for n_win in range ( 0, input_size, n_test_win_size ):
			n_id = random.randint ( n_win, n_win + n_test_win_size - 1 )
			result = session.run ( f"MATCH (n:TestNode {{ id: 'N{n_id}' }}) RETURN n" )
			record = result.single()
			assert_that ( record, f"Node N{n_id} is found in the database" ).is_not_none ()


@pytest.mark.integration
def test_multi_value_mode_single ( pg_data: tuple[ list[ dict ], list[ dict ] ], neo4j_container: Neo4jContainer, neo_driver: neo4j.Driver ):
	"""
	Tests a few properties set to work in :class:`NeoLoaderPropertyConfig.MultiValueMode.SINGLE` mode.
	"""

	property_ids = [ "source", "hasChromosomeBegin", "hasChromosomeEnd" ]	

	pg_nodes = pg_data[ 0 ]
	pg_nodes_str = [ json.dumps ( node ) for node in pg_nodes ]

	async_neo_driver: neo4j.AsyncDriver = create_async_neo_driver ( neo4j_container )

	config = create_multiple_multi_value_config ()
	for prop_id in property_ids:
		config.property_configs[ prop_id ] = NeoLoaderPropertyConfig ( multi_value_mode = NeoLoaderPropertyConfig.MultiValueMode.SINGLE )

	n_nodes = pg_jsonl_neo_loader (
		pg_jsonl_source = pg_nodes_str,
		neo_driver = async_neo_driver,
		do_nodes = True, do_edges = False,
		config = config
	)

	assert_that ( n_nodes, "Return value from the loader is correct" ).is_equal_to ( len ( pg_nodes ) )
	# Verify via Cypher that the properties are single-valued
	for node in pg_nodes:
		node_query = f"""
		MATCH (n{{ id: '{node[ "id" ]}' }})
		RETURN n
		"""
		with neo_driver.session() as session:
			result = session.run ( node_query )
			record = result.single()
			node_id = node[ "id" ]
			assert_that ( record, f"Node {node_id} is found in the database" ).is_not_none ()
			db_node = record[ "n" ]
			for prop_id in property_ids:
				if not prop_id in node[ "properties" ]: continue # Only the nodes having it
				db_prop_val = db_node.get ( prop_id )
				original_val = node[ "properties" ][ prop_id ][ 0 ]
				
				assert_that ( db_prop_val, f"Node {node_id} has property '{prop_id}' in the database" ).is_not_none ()
				
				assert_that ( 
					isinstance ( db_prop_val, (str, bytes) ) or not isinstance ( db_prop_val, Iterable ),
					f"Node {node_id} has a single value for property '{prop_id}' in the database" 
				).is_true ()

				assert_that ( 
					type ( db_prop_val ), 
					f"Node {node_id} has property '{prop_id}' with correct type in the database"
				).is_equal_to ( type ( original_val ) )

				assert_that ( db_prop_val, f"Node {node_id} has correct value for property '{prop_id}' in the database" )\
					.is_equal_to ( original_val )
				

@pytest.mark.integration
def test_multi_value_mode_single_fails_for_multi_values ( pg_data: tuple[ list[ dict ], list[ dict ] ], neo4j_container: Neo4jContainer, neo_driver: neo4j.Driver ):
	"""
	Tests that when a property is set to :class:`NeoLoaderPropertyConfig.MultiValueMode.SINGLE` mode, the loading fails for multi-value instances of that property.
	"""

	property_id = "link notes"	

	# Let's test with the edges this time
	pg_nodes, pg_edges = pg_data
	pg_nodes_str = "\n".join ( json.dumps ( node ) for node in pg_nodes )
	pg_edges_str = "\n".join ( json.dumps ( edge ) for edge in pg_edges )
	pg_all_str = pg_nodes_str + "\n" + pg_edges_str

	async_neo_driver: neo4j.AsyncDriver = create_async_neo_driver ( neo4j_container )

	config = create_multiple_multi_value_config ()
	config.property_configs[ property_id ] = NeoLoaderPropertyConfig ( multi_value_mode = NeoLoaderPropertyConfig.MultiValueMode.SINGLE )

	assert_that ( 
		pg_jsonl_neo_loader,
		"Loading fails when a property set to SINGLE mode has multiple values" 
	).raises ( ValueError )\
	.when_called_with (
		pg_jsonl_source = pg_all_str,
		neo_driver = async_neo_driver,
		config = config
	).matches ( f"multiple values aren't allowed for property '{property_id}'" )


@pytest.mark.integration
def test_multi_value_mode_auto ( pg_data: tuple[ list[ dict ], list[ dict ] ], neo4j_container: Neo4jContainer, neo_driver: neo4j.Driver ):
	"""
	Tests the default multi-value mode :class:`NeoLoaderPropertyConfig.MultiValueMode.AUTO`

	(As said above, the other tests use the :class:`NeoLoaderPropertyConfig.MultiValueMode.MULTIPLE` as 
	their default, since this is the simplest case for the loader).
	"""
	pg_nodes, pg_edges = pg_data
	pg_nodes_str = "\n".join ( json.dumps ( node ) for node in pg_nodes )
	pg_edges_str = "\n".join ( json.dumps ( edge ) for edge in pg_edges )
	pg_all_str = pg_nodes_str + "\n" + pg_edges_str

	async_neo_driver: neo4j.AsyncDriver = create_async_neo_driver ( neo4j_container )

	n_elements = pg_jsonl_neo_loader (
		pg_jsonl_source = pg_all_str,
		neo_driver = async_neo_driver,
		do_nodes = True, do_edges = True
	)

	assert_that ( n_elements, "Return value from the loader is correct" ).is_equal_to ( len ( pg_nodes ) + len ( pg_edges ) )

	# Verify via Cypher that if a property has a single value in the input, it is stored as a single value (not as a list) and
	# it's stored as a list otherwise
	for elem in pg_nodes + pg_edges:
		id = elem[ "id" ]
		elem_type = elem[ "type" ]
		cypher = \
			f"""
			MATCH (e {{ id: '{id}' }})
			RETURN e
			"""\
			if elem_type == "node" else \
			f"""
			MATCH (from)-[e {{ id: '{id}' }}]->(to)
			RETURN e
			"""
		with neo_driver.session() as session:
			result = session.run ( cypher )
			record = result.single()
			assert_that ( record, f"Element {id} is found in the database" ).is_not_none ()
			db_elem = record[ "e" ]
			for prop_key, prop_values in elem[ "properties" ].items():
				db_val = db_elem.get ( prop_key )
				assert_that ( db_val, f"Element {id} has property '{prop_key}' in the database" ).is_not_none ()
				if len ( prop_values ) == 1:
					assert_that ( db_val, f"Element {id} has a single value for property '{prop_key}' in the database" )\
						.is_equal_to ( prop_values[0] )
				else:
					assert_that ( set ( db_val ), f"Element {id} has a list value for property '{prop_key}' in the database" )\
						.is_equal_to ( set ( prop_values ) )


@pytest.mark.integration
def test_null_properties_ignored ( pg_data: tuple[ list[ dict ], list[ dict ] ], neo4j_container: Neo4jContainer, neo_driver: neo4j.Driver ):
	"""
	Tests that properties with None or empty list values in the input aren't stored in the database.
	"""
	pg_nodes, pg_edges = pg_data
	
	# Probe elements
	props = {
		"nullProp": None, 
		"emptyProp": [], # ignored too
		# None is always removed, no matter where it is. TODO: document it
		"nullSingletonProp": [ None ],
		"dirtyProp": [ None, "value" ],
		"regProp": [ "value" ],
		"regPropMulti": [ "value1", "value2" ]
	}
	pg_nodes += [{ "type": "node", "id": "null-test:01", "labels": [ "TestNode" ], "properties": props }]
	pg_edges += [{ 
		"type": "edge", "id": "null-test:02", "labels": [ "hasTestLink" ], "properties": props,
		"from": pg_nodes[0][ "id" ], "to": pg_nodes[1][ "id" ]
	}]

	pg_nodes_str = "\n".join ( json.dumps ( node ) for node in pg_nodes )
	pg_edges_str = "\n".join ( json.dumps ( edge ) for edge in pg_edges )
	pg_all_str = pg_nodes_str + "\n" + pg_edges_str

	async_neo_driver: neo4j.AsyncDriver = create_async_neo_driver ( neo4j_container )

	n_elements = pg_jsonl_neo_loader (
		pg_jsonl_source = pg_all_str,
		neo_driver = async_neo_driver,
		do_nodes = True, do_edges = True
	)

	assert_that ( n_elements, "Return value from the loader is correct" ).is_equal_to ( len ( pg_nodes ) + len ( pg_edges ) )

	# Verify via Cypher that properties with null values aren't stored, regular properties are stored 
	# as usually. 
	for elem in pg_nodes + pg_edges:
		id = elem[ "id" ]
		elem_type = elem[ "type" ]
		cypher = \
			f"""
			MATCH (e {{ id: '{id}' }})
			RETURN e
			"""\
			if elem_type == "node" else \
			f"""
			MATCH (from)-[e {{ id: '{id}' }}]->(to)
			RETURN e
			"""
		with neo_driver.session() as session:
			result = session.run ( cypher )
			record = result.single()
			assert_that ( record, f"Element {id} is found in the database" ).is_not_none ()
			db_elem = record[ "e" ]
			for prop_key, prop_values in elem [ "properties" ].items():
				# As said above, all None, at any level, are thrown away, hence we need to test without them
				prop_values = [ v for v in prop_values if v is not None ] if prop_values is not None else None
				
				if not prop_values:
					assert_that ( db_elem.get ( prop_key ), f"Element {id} doesn't have property '{prop_key}' in the database" )\
						.is_none ()
					continue

				db_val = db_elem.get ( prop_key )
				assert_that ( db_val, f"Element {id} has property '{prop_key}' in the database" ).is_not_none ()
				if len ( prop_values ) == 1:
					assert_that ( db_val, f"Element {id} has a single value for property '{prop_key}' in the database" )\
						.is_equal_to ( prop_values[0] )
				else:
					assert_that ( set ( db_val ), f"Element {id} has a list value for property '{prop_key}' in the database" )\
						.is_equal_to ( set ( prop_values ) )


@pytest.mark.integration
@pytest.mark.parametrize ( 
	ids = [ "nodes only", "all pg" ],	
	argnames = "is_nodes_only", 
	argvalues = [ True, False ]
)
def test_done_file_creation ( 
	pg_data: tuple[ list[ dict ], list[ dict ] ], 
	neo4j_container: Neo4jContainer,
	tmp_path: Path,
	is_nodes_only: bool
):
	"""
	Tests the `done_base_path` parameter to create a done flag file.
	"""

	pg_nodes, pg_edges = pg_data
	pg_all = pg_nodes if is_nodes_only else pg_nodes + pg_edges
	pg_all = [ json.dumps ( elem ) for elem in pg_all ]

	async_neo_driver: neo4j.AsyncDriver = create_async_neo_driver ( neo4j_container )

	done_file_base_path = tmp_path / "done-flag-test"
	n_elements = pg_jsonl_neo_loader (
		pg_jsonl_source = pg_all,
		neo_driver = async_neo_driver,
		do_nodes = True, do_edges = not is_nodes_only,
		done_base_path = done_file_base_path
	)

	assert_that ( n_elements, "Return value from the loader is correct" ).is_equal_to ( len ( pg_all ) )

	done_file_base_path = Path ( done_file_base_path )
	done_nodes_path = done_file_base_path.with_name ( done_file_base_path.name + ".nodes" )
	# assertpy wants a string here (https://github.com/assertpy/assertpy/issues/157)
	assert_that ( str(done_nodes_path), "Done file for nodes is created" ).exists ()

	if not is_nodes_only:
		done_edges_path = done_file_base_path.with_name ( done_file_base_path.name + ".edges" )
		assert_that ( str(done_edges_path), "Done file for edges is created" ).exists ()


def test_done_file_nodes_only ( 
	pg_data: tuple[ list[ dict ], list[ dict ] ], 
	neo4j_container: Neo4jContainer,
	tmp_path: Path,
):
	"""
	Tests that the `done_base_path` triggers the incremental behaviour, with nothing loaded if the 
	nodes flag file exists.

	It also tests that the .nodes suffix is correctly handled.

	This is the variant where do_edges is False
	"""
	pg_nodes, pg_edges = pg_data
	pg_nodes_str = "\n".join ( json.dumps ( node ) for node in pg_nodes )
	pg_edges_str = "\n".join ( json.dumps ( edge ) for edge in pg_edges )

	async_neo_driver: neo4j.AsyncDriver = create_async_neo_driver ( neo4j_container )

	done_file_path = tmp_path / "done-flag-test.done.nodes"
	done_file_path.touch ()

	n_elements = pg_jsonl_neo_loader (
		pg_jsonl_source = pg_nodes_str + "\n" + pg_edges_str,
		neo_driver = async_neo_driver,
		do_nodes = True, do_edges = False,
		done_base_path = done_file_path
	)

	assert_that ( n_elements, "No elements are loaded when the nodes flag file exists" ).is_equal_to ( 0 )


def test_done_file_all_pg ( 
	pg_data: tuple[ list[ dict ], list[ dict ] ], 
	neo4j_container: Neo4jContainer,
	tmp_path: Path,
):
	"""
	Tests that the `done_base_path` triggers the incremental behaviour, with only the edges
	loaded if the nodes flag file exists and the edges flag file doesn't exist.

	It also tests that the .nodes and .edges suffixes are correctly handled.
	"""
	pg_nodes, pg_edges = pg_data
	pg_nodes_str = "\n".join ( json.dumps ( node ) for node in pg_nodes )
	pg_edges_str = "\n".join ( json.dumps ( edge ) for edge in pg_edges )

	async_neo_driver = create_async_neo_driver ( neo4j_container )
	# Nodes need to be there, so that the edge loading doesn't fail upon missing nodes.
	pg_jsonl_neo_loader (
		pg_jsonl_source = pg_nodes_str + "\n" + pg_edges_str,
		neo_driver = async_neo_driver,
		do_nodes = True, do_edges = False
	)

	done_file_base_path = tmp_path / "done-flag-test.done"
	Path ( str ( done_file_base_path ) + ".nodes" ).touch ()

	# We need a new driver, cause the sync loader creates a new event loop
	async_neo_driver = create_async_neo_driver ( neo4j_container )
	# Then, this should do the edges only (correctly)
	n_elements = pg_jsonl_neo_loader (
		pg_jsonl_source = pg_nodes_str + "\n" + pg_edges_str,
		neo_driver = async_neo_driver,
		do_nodes = True, do_edges = True,
		done_base_path = Path ( str ( done_file_base_path ) + ".edges" )
	)

	assert_that ( n_elements, "Only edges are loaded when the nodes flag only exists" )\
		.is_equal_to ( len ( pg_edges ) )
	
	# Also tests edges aren't reloaded now
	async_neo_driver = create_async_neo_driver ( neo4j_container )
	n_elements = pg_jsonl_neo_loader (
		pg_jsonl_source = pg_nodes_str + "\n" + pg_edges_str,
		neo_driver = async_neo_driver,
		do_nodes = True, do_edges = True,
		done_base_path = Path ( str ( done_file_base_path ) + ".edges" )
	)

	assert_that ( n_elements, "Edges aren't reloaded after the first loading with done flag file" )\
		.is_equal_to ( 0 )


@pytest.mark.integration
@pytest.mark.parametrize ( 
	ids = [ "eventual success", "no success" ],
	argnames = "do_fail", 
	argvalues = [ False, True ] 
)
def test_retry_edge_collisions ( 
	pg_data: tuple[ list[ dict ], list[ dict ] ],
	neo4j_container: Neo4jContainer,
	do_fail: bool
):
	"""
	Tests that the edge loader retries those transactions that fail because they collide with 
	other concurrent transactions.

	This is a common issue with Neo4j (and transactional systems in general), so we added some logic
	to deal with it.
	"""

	class MockNeoDriver ( neo4j.AsyncDriver ):
		"""
		A mock driver that simulates the raising of exceptions during a write transaction and up to 
		a given number of attempts.

		A session's write in this driver fails for n_attempts - 1 and then, if do_fail
		isn't set, it succeeds at the last attempt, else it eventually fails and raises
		an exception that is propagated to the invoker.

		Note that this **is not** designed to work with multiple sessions, only in the test case
		below, which has a small set of edges and can save them with a single session. That's 
		because doing it otherwise would be more complex and we don't think we need to test that
		here.
		"""

		class MockSession ( neo4j.AsyncSession ):
			"""
			The mock session to realise the functionality of :class:`MockNeoDriver`. 
			"""
			def __init__ ( self, real_session: neo4j.AsyncSession, parent_driver: "MockNeoDriver" ):
				self.real_session = real_session
				self.parent_driver = parent_driver
			
			async def execute_write(self, tx_fun, *args, **kwargs):
				self.parent_driver.remaining_attempts -= 1
				if self.parent_driver.remaining_attempts > 0 or do_fail:
					raise neo4j.exceptions.TransientError ( "Simulated transaction collision" )		
				return await self.real_session.execute_write ( tx_fun, *args, **kwargs )

			async def __aenter__ ( self ):
				await self.real_session.__aenter__ ()
				return self
			
			async def __aexit__ ( self, exc_type, exc_val, exc_tb ):
				return await self.real_session.__aexit__ ( exc_type, exc_val, exc_tb )
			
			def __getattr__ ( self, name ):
				return getattr ( self.real_session, name )

		
		def __init__ ( self, real_driver: neo4j.AsyncDriver, n_attempts: int ):
			self.real_driver = real_driver
			self.remaining_attempts = n_attempts
		
		def session ( self, **config: Any ) -> neo4j.AsyncSession:
			self.is_retry_done = True
			return self.MockSession ( self.real_driver.session ( **config ), self )

  ### The test 
	# 

	pg_nodes, pg_edges = pg_data
	pg_nodes_str = [ json.dumps ( node ) for node in pg_nodes ]
	pg_edges_str = [ json.dumps ( edge ) for edge in pg_edges ]

	# Nodes go with the regular driver
	async_neo_driver: neo4j.AsyncDriver = create_async_neo_driver ( neo4j_container )
	pg_jsonl_neo_loader (
		pg_jsonl_source = pg_nodes_str,
		neo_driver = async_neo_driver,
		do_nodes = True, do_edges = False
	)

	# Then the edges with the mock driver
	mock_driver = MockNeoDriver ( create_async_neo_driver ( neo4j_container ), n_attempts = 2 )

	# Defaults retry params are too time-consuming
	config = NeoLoaderConfig ( 
		max_transaction_retries = 3, max_retry_pause = timedelta ( seconds = 3 )
	)


	# If do_fail, expect an exception up here
	if do_fail:
		assert_that ( 
			pg_jsonl_neo_loader,
			"Loader fails after too many edge transaction collisions" 
		).raises ( neo4j.exceptions.TransientError )\
		.when_called_with (
			pg_jsonl_source = pg_edges_str,
			neo_driver = mock_driver,
			do_nodes = False, do_edges = True,
			config = config
		).matches ( "Simulated transaction collision" )
		return

  # Else, it must succeed as usually
	n_edges = pg_jsonl_neo_loader (
		pg_jsonl_source = pg_edges_str,
		neo_driver = mock_driver,
		do_nodes = False, do_edges = True,
		config = config
	)
	assert_that ( n_edges, "Return value from the loader is correct" ).is_equal_to ( len ( pg_edges ) )

	# And it must have consumed the available attempts
	assert_that ( mock_driver.remaining_attempts, "Mock driver has consumed the expected number of attempts" ).is_equal_to ( 0 )


@pytest.mark.integration
@pytest.mark.parametrize (
	argnames = "test_case",
	argvalues = [ "all pg", "no-edges", "error" ]
)
def test_loader_cli ( 
	pg_data: tuple[ list[ dict ], list[ dict ] ], 
	neo4j_container: Neo4jContainer,
	neo_driver: neo4j.Driver,
	tmp_path: Path,
	test_case: str
):
	"""
	Tests the CLI wrapper for the Neo Loader.
	"""

	# First, send the test data to a file. TODO: test stdin, it's supported as well
	pg_nodes, pg_edges = pg_data

	tmp_input_path = tmp_path / "neo-loader-test-pg.jsonl"

	if test_case == "error":
		tmp_input_path.write_text ( "'type': 'node', Good luck with parsing me\n" )
	else:
		pg_nodes_str = "\n".join ( json.dumps ( node ) for node in pg_nodes )
		pg_edges_str = "\n".join ( json.dumps ( edge ) for edge in pg_edges )
		pg_all_str = pg_nodes_str + "\n" + pg_edges_str
		tmp_input_path.write_text ( pg_all_str )

	done_file_base_path = tmp_path / "cli-done-flag-test"

	cfg_path = 	Path ( os.path.dirname ( __file__ ) + "/../../../resources/test-config.yml" )\
		.absolute ()


	# And now, the real thing
	args = [
		"--neo-uri", neo4j_container.get_connection_url(),
		"--neo-user", neo4j_container.username,
		"--neo-password", neo4j_container.password,
		"--done-path", str ( done_file_base_path ),
		"--config", str ( cfg_path ),
	]
	if test_case == "no-edges":
		args.append ( "--no-edges" )

	args.append ( str ( tmp_input_path ) )

	exit_status = pg_jsonl_neo_loader_cli (
		args = args,
		do_sys_exit = False # Doesn't do a system exit, we need it for obvious reasons
	)

	if test_case == "error":
		assert_that ( exit_status, "CLI exits with error status" ).is_not_equal_to ( 0 )
		return

	assert_that ( exit_status, "CLI exits with success status" ).is_equal_to ( 0 )

	# Verify the no of nodes
	node_ids = [ node[ "id" ] for node in pg_nodes ]
	edge_ids = [ edge[ "id" ] for edge in pg_edges ]

	with neo_driver.session() as session:
		result = session.run ( f"MATCH (n) WHERE n.id IN {node_ids} RETURN count(n) AS count" )
		count = result.single() [ "count" ]
		assert_that ( count, "CLI loaded the nodes as expected" ).is_equal_to ( len ( pg_nodes ) )

		(expected_edge_count, assert_descr) = (0, "--no-edges worked") if test_case == "no-edges" \
		else (len ( pg_edges ), "CLI loaded the edges as expected")
		result = session.run ( f"MATCH ()-[r]->() WHERE r.id IN {edge_ids} RETURN count(r) AS count" )
		count = result.single() [ "count" ]
		assert_that ( count, assert_descr ).is_equal_to ( expected_edge_count )

	# And the done file
	done_suffixes = [ "nodes" ]
	if test_case != "no-edges": done_suffixes.append ( "edges" )
	for suffix in done_suffixes:
		done_file_path = str ( done_file_base_path ) + f".{suffix}"
		assert_that ( done_file_path, f"Done file for {suffix} is created" ).exists ()
	

@pytest.fixture ()
def pg_data ( request ) -> tuple[ list[ dict ], list[ dict ] ]:
	"""
	A fixture providing a pair of test nodes and edges to test the Neo4j loader. 
	This is used in multiple tests (eg, node loading, edge loading).

	Prefixes all the IDs with the test function, to avoid conflicts between data created by different tests.

	You'll see that some tests pass array of tuples straight to the loader, while others convert them
	to strings first. This is to ensure both formats are supported.
	"""

	def make_id ( elem_id: str ) -> str:
		return f"{request.node.name}:{elem_id}"
	
	# Coming from the output of pg_df_2_pg_jsonl() 
	pg_nodes = [
		{"type": "node", "id": "ENSMBL0005", "labels": ["Gene"], "properties": {"hasAccession": ["ENSMBL0005"], "source": ["NeoLoaderTest"], "hasChromosomeId": ["10E"], "hasChromosomeEnd": [87971930], "hasGeneName": ["PTEN"], "hasChromosomeBegin": [87863119]}},
		{"type": "node", "id": "ENSMBL0007", "labels": ["Gene"], "properties": {"hasAccession": ["ENSMBL0007"], "source": ["NeoLoaderTest"], "hasChromosomeId": ["12G"], "hasChromosomeEnd": [25250930], "hasGeneName": ["KRAS"], "hasChromosomeBegin": [25205246]}},
		{"type": "node", "id": "QA06", "labels": ["Protein"], "properties": {"hasAccession": ["QA06"], "hasProteinName": ["PTEN", "APC"], "source": ["NeoLoaderTest"]}},
		{"type": "node", "id": "QA07", "labels": ["Protein"], "properties": {"hasAccession": ["QA07"], "hasProteinName": ["KRAS"], "source": ["NeoLoaderTest"]}}
	]
	pg_edges = [
		{"type": "edge", "id": "encodes-protein_ENSMBL0005_QA06", "labels": ["encodes-protein"], "properties": {"source": ["NeoLoaderTest"]}, "from": "ENSMBL0005", "to": "QA06"},
		{"type": "edge", "id": "encodes-protein_ENSMBL0007_QA07", "labels": ["encodes-protein"], "properties": {"source": ["NeoLoaderTest"], "link notes": ["Manually curated", "Revision 1"]}, "from": "ENSMBL0007", "to": "QA07"}
	]

	for elem in pg_nodes + pg_edges:
		elem [ "id" ] = make_id ( elem[ 'id' ] )
		
		if elem [ "type" ] != "edge": continue

		elem [ "from" ] = make_id ( elem[ 'from' ] )
		elem [ "to" ] = make_id ( elem[ 'to' ] )

	return pg_nodes, pg_edges


@pytest.fixture ( scope = "module" )
def neo4j_container() -> Generator[ Neo4jContainer, None, None ]:
	"""
	The test container common to all driver fixtures and all the tests.
	"""
	with Neo4jContainer() as container:
		yield container


def create_async_neo_driver ( neo4j_container: Neo4jContainer ) -> neo4j.AsyncDriver:
	"""
	Returns a new async Neo4j driver connected to the test container.

	**WARNING**: yes, there is a reason why this is an helper to be invoked in all the tests
	needing it, **and not a fixture**: the async driver needs to be created inside the event loop of the test, 
	and not in the fixture's setup. Otherwise, we get hard-to-fix conflicts between the event loop created
	by the test fixture and the one that the test itself might create, directly or indirectly, via the code 
	under test.
	
	For instance, this was happening with :func:`pg_jsonl_neo_loader()`, which does async I/O with the driver, 
	and tests were failing with something like "got Future <Future pending> attached to a different loop".
	"""
	url = neo4j_container.get_connection_url()
	driver = neo4j.AsyncGraphDatabase.driver ( 
		url,
		auth = ( neo4j_container.username, neo4j_container.password )
	)
	return driver


@pytest.fixture ( scope = "module" )
def neo_driver ( neo4j_container: Neo4jContainer ) -> Generator[ neo4j.Driver, None, None ]:
	"""
	Yields a driver connected to the test container.

	Tests use this for verifying written data via synch queries.
	This doesn't have async issues and hence we can manage it through a fixture.
	"""
	yield neo4j_container.get_driver ()
