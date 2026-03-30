import json
import logging
import pprint
import sys
import pytest
from assertpy import assert_that
from pyspark.sql import SparkSession

from ketl.core import GraphTriple, PGElementType
from ketl.io.core import pg_df_2_pg_jsonl, triples_2_pg_df


log = logging.getLogger ( __name__ )


@pytest.fixture ( scope = "class", autouse = True )
def forward_spark_session_fixture ( request, spark_session: SparkSession ):
	"""
	Forwards the spark_session fixture to the classes below, which need it at classmethod level 
	to initialise test data.

	Note that autouse = True causes all the classes in the module to get the fixture, which is 
	what we currently need. Switch to @pytest.mark.usefixtures if this changes in the future.
	"""
	request.cls.spark_session = spark_session


@pytest.mark.integration
class TestTriples2PgDf ():
	"""
	Tests for :py:func:`ketl.triples_2_pg_df()`.
	"""

	@classmethod
	def setup_class ( cls ):
		"""
		Defines a bunch of node and edge triples for the tests.
		By calling the parent's method, this also starts a Spark session to be shared in all tests.
		"""
		
		def create_nodes_pg_df ():
			log.info ( "Creating test nodes" )
			# As said elsewhere, property values (ONLY) are usually stored as JSON representations, eg,
			# strings are quoted, numbers are not.
			cls.node_triples = [
				("N001", "name", '"Node 1"'),
				("N001", GraphTriple.TYPE_KEY, "TestNode"),
				("N002", "name", '"Node 2"'),
				("N002", GraphTriple.TYPE_KEY, "TestNode"),
				("N003", "name", '"Node 3"'),
				("N003", GraphTriple.TYPE_KEY, "TestNode"),
				("N003", "nickname", '"Noddy"'),
				("N003", "nickname", '"Noddy2"'),
				("N003", GraphTriple.TYPE_KEY, "NoddyNode"),
				# No properties, should map to empty dict
				("N004", GraphTriple.TYPE_KEY, "EmptyNode" )
			]

			df = cls.spark_session.createDataFrame ( cls.node_triples, schema = GraphTriple.DATAFRAME_SCHEMA_LIST )
			log.info ( "Test Node triples:\n%s", df.toPandas().to_markdown() )

			# And its mapping
			cls.nodes_pg_df = triples_2_pg_df ( df, PGElementType.NODE )
			
		def create_edges_pg_df ():
			log.info ( "Creating test edges" )
			cls.edge_triples = [
				("E001", GraphTriple.FROM_KEY, "N001"),
				("E001", GraphTriple.TO_KEY, "N002"),
				("E001", GraphTriple.TYPE_KEY, "links"),
				("E001", "weight", "0.75"),
				("E002", GraphTriple.FROM_KEY, "N002"),
				("E002", GraphTriple.TO_KEY, "N003"),
				("E002", GraphTriple.TYPE_KEY, "links"),
				("E002", "weight", "0.85"),
				("E002", "weight", "0.95"),  # Multiple weights to test set behavior
				("E003", GraphTriple.FROM_KEY, "N003"),
				("E003", GraphTriple.TO_KEY, "N001"),
				("E003", GraphTriple.TYPE_KEY, "inferredLink"),
				("E003", "description", '"Inferred relationship"'),

				# Empty property edge, should map to empty dict
				("E004", GraphTriple.FROM_KEY, "N004"),
				("E004", GraphTriple.TO_KEY, "N001"),
				("E004", GraphTriple.TYPE_KEY, "links"),
			]

			df = cls.spark_session.createDataFrame ( cls.edge_triples, schema = GraphTriple.DATAFRAME_SCHEMA_LIST )
			log.info ( "TestEdge triples:\n%s", df.toPandas().to_markdown() )

			# Mapping, as above
			cls.edges_pg_df = triples_2_pg_df ( df, PGElementType.EDGE )
		
		create_nodes_pg_df ()
		create_edges_pg_df ()


	def test_nodes_basics ( self ):

		pg_df = self.nodes_pg_df
		log.info ( "triples_2_pg_df(), mapped nodes:\n%s", pg_df.toPandas().to_markdown() )
		# TODO: test with pg_df_2_pgjsonl()
		# log.warning ( "triples_2_pg_df(), mapped nodes:\n%s", pg_df_2_pgjsonl ( pg_df ) )

		assert_that ( pg_df.count(), "Result count is correct" ).is_equal_to ( 4 )

		for row in pg_df.collect():
			assert_that ( row.type, "Row type is 'node'" ).is_equal_to ( str ( PGElementType.NODE ) )

		for row in pg_df.collect():
			expt_label = "EmptyNode" if row.id == "N004" else "TestNode"
			assert_that ( row.labels, f"Node {row.id} has expected label" ).contains ( expt_label )
		
	def test_nodes_multiple_labels ( self ):

		pg_df = self.nodes_pg_df

		n003 = pg_df.filter ( pg_df.id == "N003" ).collect()[0]
		assert_that ( n003.labels, "Node N003 has additional label" ).contains ( "NoddyNode" )

	def test_nodes_properties ( self ):

		pg_df = self.nodes_pg_df

		for row in pg_df.filter( pg_df.id != "N004" ).collect():
			assert_that ( row.properties, f"Node {row.id} has 'name' property" ).contains ( "name" )
			assert_that ( row.properties["name"], f"'name' property is a list in node {row.id}" ).is_instance_of ( list )

		# for i in 0-3, test node 00i has name 'Node 00i'
		for i in range ( 1, 4 ):
			node_id = f"N00{i}"
			expected_name = f'"Node {i}"'
			node = pg_df.filter ( pg_df.id == node_id ).collect()[0]
			assert_that ( node.properties["name"], f"Node {node_id} has correct 'name' property" ).contains ( expected_name )

	def test_nodes_multi_value_props ( self ):

		pg_df = self.nodes_pg_df
		n003 = pg_df.filter ( pg_df.id == "N003" ).collect()[0]

		assert_that ( n003.properties, "Node N003 has 'nickname' property" ).contains ( "nickname" )
		assert_that ( n003.properties["nickname"], "'nickname' property is a list" ).is_instance_of ( list )
		assert_that ( n003.properties["nickname"], "'nickname' property has two values" ).is_length ( 2 )
		assert_that ( n003.properties["nickname"], "Node N003 has 'Noddy' nickname" ).contains ( '"Noddy"' )
		assert_that ( n003.properties["nickname"], "Node N003 has 'Noddy2' nickname" ).contains ( '"Noddy2"' )

	def test_edges_basics ( self ):

		pg_df = self.edges_pg_df
		log.info ( "triples_2_pg_df(), mapped edges:\n%s", pg_df.toPandas().to_markdown() )
		# log.warning ( "triples_2_pg_df(), mapped edges:\n%s", pg_df_2_pgjsonl ( pg_df ) )

		assert_that ( pg_df.count(), "Result count is correct" ).is_equal_to ( 4 )

		# All rows have type 'edge'
		for row in pg_df.collect():
			assert_that ( row.type, "Row type is 'edge'" ).is_equal_to ( str ( PGElementType.EDGE ) )

		# All edges have the right label
		for row in pg_df.collect():
			exp_label = "inferredLink" if row.id == "E003" else "links"
			assert_that ( row.labels, f"Edge {row.id} has expected label" ).contains ( exp_label )

		# All edges have 'from' and 'to' properties
		for row in pg_df.collect():
			assert_that ( row[ 'from' ], f"Edge {row.id} has 'from'" ).is_not_none ()
			assert_that ( row[ 'to' ], f"Edge {row.id} has 'to'" ).is_not_none ()

		# E001 goes from N001 to N002
		e001 = pg_df.filter ( pg_df.id == "E001" ).collect()[0]
		assert_that ( e001[ 'from' ], "Edge E001 has correct 'from'" ).is_equal_to ( "N001" )
		assert_that ( e001[ 'to' ], "Edge E001 has correct 'to'" ).is_equal_to ( "N002" )

	def test_edges_properties ( self ):

		pg_df = self.edges_pg_df
		e001 = pg_df.filter ( pg_df.id == "E001" ).collect()[0]

		assert_that ( e001.properties, "Edge E001 has 'weight' property" ).contains ( "weight" )
		assert_that ( e001.properties[ "weight" ], "'weight' property is a list in edge E001" ).is_instance_of ( list )
		assert_that ( e001.properties[ "weight" ], "'weight' property has one value in edge E001" ).is_length ( 1 )
		assert_that ( e001.properties[ "weight" ], "Edge E001 has correct 'weight' value" ).contains ( "0.75" )
		
	def test_edges_multi_value_props ( self ):

		pg_df = self.edges_pg_df
		e002 = pg_df.filter ( pg_df.id == "E002" ).collect()[0]

		assert_that ( e002.properties, "Edge E002 has 'weight' property" ).contains ( "weight" )
		assert_that ( e002.properties[ "weight" ], "'weight' property is a list in edge E002" ).is_instance_of ( list )
		assert_that ( e002.properties[ "weight" ], "'weight' property has two values in edge E002" ).is_length ( 2 )
		for exp_weight in [ "0.85", "0.95" ]:
			assert_that ( e002.properties[ "weight" ], f"Edge E002 has weight {exp_weight}" ).contains ( exp_weight )

		e003 = pg_df.filter ( pg_df.id == "E003" ).collect()[0]
		assert_that ( e003.properties, "Edge E003 has 'description' property" ).contains ( "description" )
		assert_that ( e003.properties[ "description" ], "'description' property is a list in edge E003" ).is_instance_of ( list )
		assert_that ( e003.properties[ "description" ], "'description' property has one value in edge E003" ).is_length ( 1 )
		assert_that ( e003.properties[ "description" ], "Edge E003 has correct 'description' value" ).contains ( '"Inferred relationship"' )
# /TestTriples2PgDf


@pytest.mark.integration
class TestPgDf2PgJSONL ():
	"""
	Tests for :py:func:`ketl.pg_df_2_pg_jsonl()`.
	"""

	@classmethod
	def setup_class ( cls ):
		"""
		Similarly to :meth:`TestTriples2PgDf.init_test_data`, initialises the class's test data.
		"""

		def create_nodes_pg_df ():
			log.info ( "Creating test PG nodes DataFrame" )
			pg = [
				( "N001", ["TestNode"], { "name": [ '"Node 1"' ] }, "node" ),
				( "N002", ["TestNode"], { "name": [ '"Node 2"' ] }, "node" ),
				( "N003", ["NoddyNode", "TestNode"], { "name": [ '"Node 3"' ], "nickname": [ '"Noddy"', '"Noddy2"' ] }, "node" ),
				( "N004", ["EmptyNode"], {}, "node" ),
			]
			pg_df_schema = [ "id", "labels", "properties", "type" ] # TODO: put in a constant
			cls.nodes_pg_df = cls.spark_session.createDataFrame ( pg, schema = pg_df_schema )
		
		def create_edges_pg_df ():
			log.info ( "Creating test PG edges DataFrame" )
			pg = [
				( "E001", ["links"], { "weight": [ 0.75 ] }, "N001", "N002", "edge" ),
				( "E002", ["links"], { "weight": [ 0.85, 0.95 ] }, "N002", "N003", "edge" ),
				( "E003", ["inferredLink"], { "description": [ '"Inferred relationship"' ] }, "N003", "N001", "edge" ),
				( "E004", ["links"], {}, "N004", "N001", "edge" ),
			]
			pg_df_schema = [ "id", "labels", "properties", "from", "to", "type" ] # TODO: put in a constant
			cls.edges_pg_df = cls.spark_session.createDataFrame ( pg, schema = pg_df_schema )

		def jsonl_str_to_list ( jsonl_str: str ) -> list[ dict ]:
			"""
			Converts a JSONL.pg string back to a list of dicts, one per line.
			"""
			return [ json.loads ( line ) for line in jsonl_str.strip().split ( "\n" ) if line.strip() ]
		
		create_nodes_pg_df ()
		create_edges_pg_df ()

		cls.jsonl_nodes = jsonl_str_to_list ( pg_df_2_pg_jsonl ( cls.nodes_pg_df ) )
		cls.jsonl_edges = jsonl_str_to_list ( pg_df_2_pg_jsonl ( cls.edges_pg_df ) )
	# /setup_class


	def test_node_basics ( self ):
		log.info ( 
			"pg_df_2_pgjsonl(), output nodes:\n%s",
			pprint.pformat ( self.jsonl_nodes ) 
		)
		assert_that ( len ( self.jsonl_nodes ), "JSONL output has correct number of nodes" ).is_equal_to ( 4 )
		assert_that ( [ node[ "type" ] for node in self.jsonl_nodes ], "All nodes are of type 'node'" ).contains_only ( "node" )
		assert_that ( [ node[ "id" ] for node in self.jsonl_nodes ], "All nodes have an ID" ).does_not_contain ( None )
		assert_that ( [ node[ "labels" ] for node in self.jsonl_nodes ], "All nodes have labels" ).does_not_contain ( None )
		assert_that (
			[ node[ "properties" ] for node in self.jsonl_nodes if node[ "id" ] != "N004" ],
			"All nodes that should have properties actually have them"
		).does_not_contain ( {} )
		n004 = next ( node for node in self.jsonl_nodes if node[ "id" ] == "N004" )
		assert_that ( n004[ "properties" ], "Node N004 has empty properties" ).is_equal_to ( {} )

	def test_node_name ( self ):
		n001 = next ( node for node in self.jsonl_nodes if node[ "id" ] == "N001" )
		n001_name = n001[ "properties" ][ "name" ]
		# '"string"' values are unserialised back to 'string'
		assert_that ( n001_name, "Node N001 has correct 'name' property" ).contains ( "Node 1" )

	def test_node_nickname ( self ):
		n003 = next ( node for node in self.jsonl_nodes if node[ "id" ] == "N003" )
		n003_nicknames = set ( n003[ "properties" ][ "nickname" ] )
		assert_that ( n003_nicknames, "Node N003 has correct 'nickname' property" ).is_equal_to ( { "Noddy", "Noddy2" } )

	def test_edge_basics ( self ):
		log.info ( 
			"pg_df_2_pgjsonl(), output edges:\n%s",
			pprint.pformat ( self.jsonl_edges ) 
		)
		assert_that ( len ( self.jsonl_edges ), "JSONL output has correct number of edges" ).is_equal_to ( 4 )
		assert_that ( [ edge[ "type" ] for edge in self.jsonl_edges ], "All edges are of type 'edge'" ).contains_only ( "edge" )
		assert_that ( [ edge[ "id" ] for edge in self.jsonl_edges ], "All edges have an ID" ).does_not_contain ( None )
		assert_that ( [ edge[ "labels" ] for edge in self.jsonl_edges ], "All edges have labels" ).does_not_contain ( None )
		assert_that ( [ edge.get ( "from" ) for edge in self.jsonl_edges ], "All edges have 'from'" ).does_not_contain ( None )
		assert_that ( [ edge.get ( "to" ) for edge in self.jsonl_edges ], "All edges have 'to'" ).does_not_contain ( None )
		assert_that (
			[ edge[ "properties" ] for edge in self.jsonl_edges if edge[ "id" ] != "E004" ],
			"All edges that should have properties actually have them"
		).does_not_contain ( {} )
		e004 = next ( edge for edge in self.jsonl_edges if edge[ "id" ] == "E004" )
		assert_that ( e004[ "properties" ], "Edge E004 has empty properties" ).is_equal_to ( {} )
	
	def test_edge_weight ( self ):
		e002 = next ( edge for edge in self.jsonl_edges if edge[ "id" ] == "E002" )
		e002_weights = set ( e002[ "properties" ][ "weight" ] )
		assert_that ( e002_weights, "Edge E002 has correct 'weight' property" )\
			.is_equal_to ( { 0.85, 0.95 } )

	def test_edge_description ( self ):
		e003 = next ( edge for edge in self.jsonl_edges if edge[ "id" ] == "E003" )
		e003_descriptions = e003[ "properties" ][ "description" ]
		assert_that ( e003_descriptions, "Edge E003 has correct 'description' property" )\
			.contains ( "Inferred relationship" )
# /TestPgDf2PgJSONL
