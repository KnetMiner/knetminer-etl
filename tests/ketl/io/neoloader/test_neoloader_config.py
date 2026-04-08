"""
Tests the configuration functionality of the NeoLoader components.
"""

from datetime import timedelta
import os
import neo4j
import pytest
from ketl.config import load_config
from assertpy import assert_that
from pathlib import Path

from testcontainers.neo4j import Neo4jContainer
from typing import Generator


from ketl.io.neoloader import NeoLoaderConfig, NeoLoaderPropertyConfig, create_neo_driver_from_config

def test_neoloader_config ( config ):
	cfg_dict = config["neoloader"]
	loader_cfg: NeoLoaderConfig = NeoLoaderConfig.from_config ( cfg_dict )

	assert_that ( loader_cfg, "NeoLoaderConfig was created from configuration" )\
	.is_instance_of ( NeoLoaderConfig )
	
	assert_that ( 
		loader_cfg.default_property_config,
		"NeoLoaderConfig default property configuration was loaded"
	).is_instance_of ( NeoLoaderPropertyConfig )

	assert_that (
		loader_cfg.default_property_config.multi_value_mode,
		"NeoLoaderConfig default property configuration multi_value_mode was loaded"
	).is_equal_to ( cfg_dict [ "default_property_config" ]["multi_value_mode"] )

	assert_that ( loader_cfg.property_configs, "NeoLoaderConfig property configurations were loaded" )\
	.is_instance_of ( dict )\
	.contains_entry ({ 
		"has_pvalue": NeoLoaderPropertyConfig.from_config ( cfg_dict [ "property_configs" ]["has_pvalue"] ) 
	})

	assert_that ( loader_cfg.loader_max_concurrency, "NeoLoaderConfig max concurrency was loaded" )\
	.is_equal_to ( cfg_dict [ "loader_max_concurrency" ] )

	assert_that ( loader_cfg.max_retry_pause, "NeoLoaderConfig max retry pause was loaded" )\
	.is_equal_to ( timedelta ( **cfg_dict [ "max_retry_pause" ] ) )


def test_create_neo_driver_from_config ( config, neo4j_container ):
	neo4j_cfg = config["neo4j"]

	# If we want to test an actual connection, we need to adapt this config to the 
	# test container
	neo4j_cfg["uri"] = neo4j_container.get_connection_url()
	neo4j_cfg["auth"] = {
		"user": neo4j_container.username,
		"password": neo4j_container.password
	}
	driver: neo4j.Driver = create_neo_driver_from_config ( neo4j_cfg, is_async = False )

	assert_that ( driver, "Neo4j driver was created from configuration" )\
	.is_instance_of ( neo4j.Driver )

	assert_that ( driver.verify_connectivity(), "Neo4j driver connectivity was verified" )\
		.is_none ()


@pytest.fixture ( scope = "module" )
def config ():
	"""
	The test configuration in `/tests/resources`
	"""
	# Injected in the config. Not used here, but for sake of completeness.
	os.environ["NEO4J_PASSWORD"] = "testTest"	
	yaml_path =  Path ( os.path.dirname ( __file__ ) + "/../../../resources/test-config.yml" )\
		.absolute ()
	config = load_config ( yaml_path )
	return config


@pytest.fixture ( scope = "module" )
def neo4j_container() -> Generator[ Neo4jContainer, None, None ]:
	"""
	The test container common to all driver fixtures and all the tests.

	Here, this is used only to ensure a driver is instantiated from configuration
	"""
	with Neo4jContainer() as container:
		yield container


