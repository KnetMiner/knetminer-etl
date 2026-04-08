import os
from pathlib import Path

import pytest
from ketl.config import load_config
from assertpy import assert_that


def test_config_load ( config ):
	assert_that ( config, "Configuration was loaded" ).is_not_none ()

def test_neo4j_config ( config ):
	assert_that ( config["neo4j"], "Neo4j configuration was loaded" ).is_not_none ()
	neo4j = config["neo4j"]

	assert_that ( neo4j[ "uri" ], "Neo4j URI was loaded" ).is_equal_to ( "bolt://neo.somewhere.net:7687" )
	assert_that ( neo4j["auth"]["user"], "Neo4j user was loaded" ).is_equal_to ( "neo4j" )
	assert_that ( neo4j["connection_timeout"], "Neo4j connection timeout was loaded" )\
		.is_equal_to ( 15 )
	
def test_neoloader_config ( config ):
	assert_that ( config["neoloader"], "Neoloader configuration was loaded" ).is_not_none ()
	neoloader = config["neoloader"]

	default_property_config = neoloader["default_property_config"]
	assert_that ( default_property_config, "Neoloader default property configuration was loaded" )\
		.is_instance_of ( dict )\
		.contains_entry ( { "multi_value_mode": "multiple" } )

	property_configs = neoloader["property_configs"]
	assert_that ( property_configs, "Neoloader property configurations were loaded" )\
		.is_instance_of ( dict )
	assert_that ( property_configs["has_pvalue"], "has_pvalue property configuration was loaded" )\
		.is_instance_of ( dict )\
		.contains_entry ( { "multi_value_mode": "single" } )

	assert_that ( neoloader["loader_max_concurrency"], "Neo4j loader max concurrency was loaded" )\
		.is_equal_to ( 8 )

def test_env_var_resolution ( config ):
	neo4j = config["neo4j"]
	assert_that ( neo4j["auth"]["password"], "Neo4j password was loaded from environment" )\
		.is_equal_to ( os.getenv ( "NEO4J_PASSWORD" ) )


@pytest.fixture ( scope = "module" )
def config ():
	"""
	The test configuration is in `/tests/ketl/resources/`
	"""
	
	# Set NEO4J_PASSWORD to later test env resolution
	os.environ["NEO4J_PASSWORD"] = "testTest"	
	yaml_path =  Path ( os.path.dirname ( __file__ ) + "/../resources/test-config.yml" ).absolute ()
	config = load_config ( yaml_path )
	return config
