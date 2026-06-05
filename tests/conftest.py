import logging
import os
import subprocess
from typing import Generator

import neo4j
import pytest
from brandizpyes.logging import logger_config
from pyspark.sql import SparkSession
from testcontainers.neo4j import Neo4jContainer

"""
Pytest configuration file, which the framework picks up at startup.

[Details here](https://docs.pytest.org/en/stable/reference/fixtures.html)

"""
def pytest_configure ( config ):
	"""
	As per their docs, this is picked up by pytest at startup.

	This configures the Python logging module from a YAML file included in the test resources.
	"""

	cfg_path = os.path.dirname ( __file__ ) + "/resources/logging-test.yml"
	logger_config ( __name__, cfg_path = cfg_path )


@pytest.fixture ( name = "spark_session", scope = "module" )
def create_spark_session_fixture ():
	"""
	Creates a Spark session as a pytest fixture, which can be injected into tests
	and test methods.

	Note that we tried the scope "session", but Spark crashes miserably, maybe due
	to some concurrency issue when running tests in parallel.
	"""
	
	log = logging.getLogger ( __name__ )
	log.info ( "Creating Spark session for tests" )

	spark_session = SparkSession.builder\
		.master ( "local[*]" )\
		.appName ( "test_ketl" )\
		.getOrCreate()
			
	yield	spark_session	

	log.info ( "Stopping Spark session for tests" )
	spark_session.stop ()


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

	TODO: Move to a utils module
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


def run_snakefile ( 
	snakefile_path: str, snake_target: str = "all", 
	ketl_data_dir_path: str = "/tmp/ketl",
	custom_env: dict[str, str] = None
) -> None:
	"""
	Helper to run our SnakeMake test files

	- `snakefile_path` is the path to the Snakefile to run. If it's not absolute, it's rooted into
	<project_root>/tests/resources.
	- `snake_target` is the target to run in the Snakefile, defaulting to "all"
	- `ketl_data_dir_path` root dir for the workflow output.
	- `custom_env` additional OS envs to be added to the variables set here
	"""

	my_dir = os.path.dirname ( os.path.abspath ( __file__ ) ) # tests/
	prj_dir = os.path.abspath ( my_dir + "/.." )
	test_dir = prj_dir + "/tests"
	
	if not snakefile_path.startswith ( "/" ):
		snakefile_path = f"{test_dir}/resources/{snakefile_path}"

	pypath = []
	if "PYTHONPATH" in os.environ: pypath.append ( os.environ [ "PYTHONPATH" ] )
	pypath.append ( "tests" )
	# Snake often includes modules in the same dir where the Snakefile is,
	pypath.append ( os.path.realpath ( os.path.dirname ( snakefile_path ) ) )

	os.chdir ( prj_dir )
	
	# Clean the output dir, if it exists
	if os.path.exists ( ketl_data_dir_path ):
		subprocess.run ( [ "rm", "-rf", ketl_data_dir_path ], check = True )

	env = { **os.environ, "KETL_DATA": ketl_data_dir_path, "PYTHONPATH": ":".join ( pypath ) }
	if custom_env:
		env = { **env, **custom_env }

	subprocess.run ( 
		[ "snakemake", "-s", snakefile_path, "--cores", "all", snake_target ], 
		check = True,
		env = env
	)
