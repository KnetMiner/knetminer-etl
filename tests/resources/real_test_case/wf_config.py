"""
You can configure a workflow either with a module like this, or via YAML (see `config.yml`).

The latter is simpler, the Python way is more expressive and more powerful. HOWEVER,  it is well-known
that Python-based configuration is less secure too; if you use it, be sure you're in control of 
who writes the config and who reads it.

Note that the NeoLoader configuration is in `mapping.py`, since this is more related to 
mapping than config and doesn't depend much on the target DB.

"""

import os

import neo4j
from pyspark.sql import SparkSession


def get_spark_session ():
	return SparkSession.builder\
			.master ( "local[*]" )\
			.appName ( "test_ketl_snake" )\
			.getOrCreate()

def create_neo4j_driver () -> neo4j.AsyncDriver:
	driver = neo4j.AsyncGraphDatabase.driver ( 
		"bolt://localhost:" + os.getenv ( "NEO4J_PORT", "7687" ),
		auth = ( 
			os.getenv ( "NEO4J_USER", "neo4j" ), 
			os.getenv ( "NEO4J_PASSWORD", "" )
		)
	)
	return driver
