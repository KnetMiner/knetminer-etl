import types

from pyspark.sql import SparkSession


def forward_spark_session_fixture ( spark_session: SparkSession, module: types.ModuleType ):
	"""
	Forwards `spark_session` to the current module (the one invoking this function) as a module-level
	global variable, named `_spark_session`. 

	This is ugly, but needed for classes that uses Spark at `@classmethod` level, eg, to initialise 
	data for multiple instance methods.

	This should be used in a test module this way:

	```python
	@pytest.fixture ( scope = "module", autouse = True )
	# @pytest.mark.usefixtures ( "spark_session" ) # shouldn't be needed
	def _forward_spark_session_fixture ( spark_session: SparkSession ):
		forward_spark_session_fixture ( spark_session, module = sys.modules[ __name__ ] )
	```
	"""
	setattr ( module, "_spark_session", spark_session )	
