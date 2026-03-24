
import json
import logging
import random

import neo4j
import pytest

from assertpy import assert_that
from ketl.io.neoloader import pg_jsonl_neo_loader

from testcontainers.neo4j import Neo4jContainer

from typing import Generator


log = logging.getLogger ( __name__ )


@pytest.mark.integration
def test_pg_jsonl_neo_loader_nodes ( 
	pg_nodes: list[ dict ], neo4j_container: Neo4jContainer, neo_driver: neo4j.Driver 
):
	"""
	Tests for :py:func:`ketl.pg_jsonl_neo_loader()`.

	It uses the async driver with the loader and the sync one to verify the results via Cypher.

	TODO: still missing:
	- logs
	- OK edges
	- actual batching (and performance)
	- singleton->single values, not lists
	- OK multiple labels
	- move from io to its own module, and add CLI wrapper to it
	- OK get rid of neo warnings
	- neo retries
	"""

	pg_nodes_str = "\n".join ( json.dumps ( node ) for node in pg_nodes )

	async_neo_driver: neo4j.AsyncDriver = create_async_neo_driver ( neo4j_container )

	n_nodes = pg_jsonl_neo_loader (
		pg_jsonl_source = pg_nodes_str,
		neo_driver = async_neo_driver,
		do_nodes = True, do_edges = False
	)

	assert_that ( n_nodes, "Return value from the loader is correct" ).is_equal_to ( len ( pg_nodes ) )

	# Verify via Cypher
	for node in pg_nodes:
		cy_labels = ":".join ( [ f"`{label}`" for label in node[ "labels" ] ] )
		node_query = f"""
		MATCH (n:{cy_labels} {{ id: '{node[ "id" ]}' }})
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


def test_pg_jsonl_neo_loader_edges ( 
	pg_nodes: list[ dict ], neo4j_container: Neo4jContainer, neo_driver: neo4j.Driver
):
	"""
	Tests for :py:func:`ketl.pg_jsonl_neo_loader()` with edges.
	"""

	pg_edges = [
		{"type": "edge", "id": "encodes-protein_ENSMBL0005_QA06", "labels": ["encodes-protein"], "properties": {"source": ["NeoLoaderTest"]}, "from": "ENSMBL0005", "to": "QA06"},
		{"type": "edge", "id": "encodes-protein_ENSMBL0007_QA07", "labels": ["encodes-protein"], "properties": {"source": ["NeoLoaderTest"], "link notes": ["Manually curated"]}, "from": "ENSMBL0007", "to": "QA07"}
	]

	pg_nodes_str = "\n".join ( json.dumps ( node ) for node in pg_nodes )
	pg_edges_str = "\n".join ( json.dumps ( edge ) for edge in pg_edges )
	pg_all_str = pg_nodes_str + "\n" + pg_edges_str

	async_neo_driver: neo4j.AsyncDriver = create_async_neo_driver ( neo4j_container )

	# Load both the nodes and the edges. Not only is this necessary, but it also verifies that 
	# the loader can do both in one invocation.
	n_edges = pg_jsonl_neo_loader (
		pg_jsonl_source = pg_all_str,
		neo_driver = async_neo_driver,
		do_nodes = True, do_edges = True
	) - len ( pg_nodes ) # The loader returns the total number of created elements.
	
	assert_that ( n_edges, "Return value from the loader is correct" ).is_equal_to ( len ( pg_edges ) )
	
	# Verify via Cypher
	for edge in pg_edges:
		cy_labels = ":".join ( [ f"`{label}`" for label in edge[ "labels" ] ] )
		edge_query = f"""
		MATCH (from)-[r:{cy_labels} {{ id: '{edge[ "id" ]}' }}]->(to)
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


def test_pg_jsonl_neo_loader_large_input_nodes ( neo4j_container: Neo4jContainer, neo_driver: neo4j.Driver ):
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


@pytest.fixture ( scope="module" )
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


@pytest.fixture ( scope = "module" )
def pg_nodes () -> list[ dict ]:
	"""
	A fixture providing nodes to test the Neo4j loader. This is used in multiple tests (eg, node loading, 
	edge loading).
	"""
	# Coming from the output of pg_df_2_pg_jsonl() 
	pg_nodes = [
		{"type": "node", "id": "ENSMBL0005", "labels": ["Gene"], "properties": {"hasAccession": ["ENSMBL0005"], "source": ["NeoLoaderTest"], "hasChromosomeId": ["10E"], "hasChromosomeEnd": [87971930], "hasGeneName": ["PTEN"], "hasChromosomeBegin": [87863119]}},
		{"type": "node", "id": "ENSMBL0007", "labels": ["Gene"], "properties": {"hasAccession": ["ENSMBL0007"], "source": ["NeoLoaderTest"], "hasChromosomeId": ["12G"], "hasChromosomeEnd": [25250930], "hasGeneName": ["KRAS"], "hasChromosomeBegin": [25205246]}},
		{"type": "node", "id": "QA06", "labels": ["Protein"], "properties": {"hasAccession": ["QA06"], "hasProteinName": ["PTEN", "APC"], "source": ["NeoLoaderTest"]}},
		{"type": "node", "id": "QA07", "labels": ["Protein"], "properties": {"hasAccession": ["QA07"], "hasProteinName": ["KRAS"], "source": ["NeoLoaderTest"]}}
	]
	return pg_nodes
