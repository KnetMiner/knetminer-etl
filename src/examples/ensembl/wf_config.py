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


def get_spark_session ( app_name_postfix: str = "" ) -> SparkSession:
	"""
	It's recommended that you give one app name per session, eg, per rule
	"""
	if app_name_postfix and not app_name_postfix.startswith ( ("_", "-", ".", ":", "/") ):
		app_name_postfix = ":" + app_name_postfix
	return SparkSession.builder\
		.master ( os.getenv ( "SPARK_MASTER", "local[*]" ) )\
		.appName ( "ketl-ensembl" + app_name_postfix )\
		.getOrCreate()

def create_neo4j_driver () -> neo4j.AsyncDriver:
	driver = neo4j.AsyncGraphDatabase.driver ( 
		os.getenv ( "NEO4J_URL", "bolt://localhost" ),
		auth = ( 
			os.getenv ( "NEO4J_USER", "neo4j" ), 
			os.getenv ( "NEO4J_PASSWORD", "" )
		)
	)
	return driver
