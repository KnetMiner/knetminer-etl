import logging
import os

import pytest
from brandizpyes.logging import logger_config
from pyspark.sql import SparkSession

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
		.appName ( "test_tabmap" )\
		.getOrCreate()		
	yield	spark_session	

	log.info ( "Stopping Spark session for tests" )
	spark_session.stop ()

