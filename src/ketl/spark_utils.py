import logging
from typing import TYPE_CHECKING, List, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import Row
from pyspark.testing.utils import assertDataFrameEqual

if TYPE_CHECKING:
	import pandas
	import pyspark.pandas

import sys

log = logging.getLogger ( __name__ )

class DataFrameCheckpointManager:
	"""
	Manages saving and loading intermediate DataFrames, ie, DFs that are saved as intermediate 
	data, so that a workflow can be restarted from them and provide incremental runs.

	These procedures are purposefully abstract, all the client needs to know is that the path associated
	to a DF identifies the data and can be used to reload it later.

	Internally, we perform Spark-specific optimisations, which are kept transparent to the client.
	"""
	@staticmethod
	def df_save ( 
		df: DataFrame, path: str, target_partition_size: int = 256 * 1024 ** 2 
	) -> DataFrame:
		"""
		Saves a dataframe which is intended as an intermediate in a workflow like a SnakeMake workflow,
		ie, saves some partial data that allows to restart the workflow from them and have incremental runs.

		As you can see below, internally, we estimate how big the DF is and we choose to repartition it
		using :meth:`DataFrame.repartition`, `:fun:DataFrame.coalesce` or nothing, depending on the size.
		These are optimisations required by Spark and as said, we aim at not bothering the client with 
		them.

		
		## Parameters:

		:param df: the DataFrame to save
		
		:param path: the path where to save it. If this is a check path, its actual base path is automatically
		retrieved using :meth:`get_intermediate_path`.		

		:param target_partition_size: target size of each partition file, in bytes

		:return: the saved DataFrame, which might be a repartitioned/coalesced version of the original one
		You won't need this often, we return it mainly to support tests.
		"""
		log.info ( f"Saving intermediate DataFrame to {path}" )


		size = DataFrameCheckpointManager.df_rough_size ( df )
		new_partitions = DataFrameCheckpointManager.df_new_partition_size ( size, target_partition_size )
		current_partitions = df.rdd.getNumPartitions ()

		if new_partitions < current_partitions:
			log.debug ( f"Coalescing from {current_partitions} to {new_partitions} partitions" )
			df = df.coalesce ( new_partitions )
		elif new_partitions > current_partitions:
			log.debug ( f"Repartitioning from {current_partitions} to {new_partitions} partitions" )
			df = df.repartition ( new_partitions )
		else:
			log.debug ( f"Keeping current partition count: {current_partitions}" )

		path = DataFrameCheckpointManager.df_path ( path )
		df.write.mode ( "overwrite" ).parquet ( path )
		log.info ( f"DataFrame saved" )
		return df


	@staticmethod
	def df_load ( path_or_df: str | DataFrame, spark: SparkSession ) -> DataFrame:
		"""
		Loads a dataframe which was saved as an intermediate by :meth:`df_save`.

		## Parameters:

		:param path_or_df: the path where the DataFrame was saved, or just a DF. In the latter 
		case, we just return the DF, and this option is here for components that take their input
		from a data frame. That is, they can easily use this helper to start from a path instead.
		When this is a path, it is passed to :meth:`get_intermediate_path` to automatically retrieve 
		the actual base path.

		:param spark: the Spark session to use to load the DF from. This is mandatory when 
		`path_or_df` is a path.

		"""
		if isinstance ( path_or_df, DataFrame ):
			log.debug ( f"load_intermediate() got a DataFrame as input, returning it" )
			return path_or_df
		
		if not spark:
			raise ValueError ( 
				"DataFrameCheckpointManager.load_intermediate(): a SparkSession is required "
				"when loading from a path." 
			)

		path = DataFrameCheckpointManager.df_path ( path_or_df )
		log.info ( f"Loading intermediate DataFrame from {path}" )
		df = spark.read.parquet ( path )		
		log.info ( f"DataFrame loaded" )

		return df


	@staticmethod
	def df_check_path ( base_path: str ) -> str:
		"""
		Returns a path that can be used to check if the parquet file identified by the
		parameter exists.

		This is useful in frameworks like SnakeMake, especially when used in combination
		with loading and saving methods, which automatically invokes :meth:`get_intermediate_path`.
		In the KETL framework, we do so for all relevant load/save operations (eg, in mappers, PG dumpers).

		In practice, it just returns `${base_path}.parquet/_SUCCESS`, but it allows you
		to abstract this .parquet-specific detail away.
		"""
		return f"{base_path}/_SUCCESS"
	

	@staticmethod
	def df_path ( df_path: str ) -> str:
		"""
		Returns the base path that was used to save the intermediate file 
		(a parquet directory in the current implementation).

		This is used in :meth:`df_load` and :meth:`df_save`, to automatically
		retrieve the base path from a check path.

		This is useful in frameworks like SnakeMake, ie, if you use :meth:`df_check_path` 
		in rules and then pass this to load/save methods, these will automatically get the base path.

		In practice, this just strips the `/.parquet/_SUCCESS` suffix, if present. If the path is empty
		or None, just returns it as is.
		"""
		if not df_path: return df_path
		if not df_path.endswith ( "/_SUCCESS" ): return df_path
		return df_path [ :-len ( "/_SUCCESS" ) ]


	@staticmethod
	def df_rough_size ( df: DataFrame, sample_ratio: float = 0.1 ) -> int:
		"""
		Estimates a DataFrame size by sampling partitions and using Python memory size.
		Fast and approximate, sufficient for choosing partition counts.

		## Parameters:

		:param df: the DataFrame whose size to estimate
		:param sample_ratio: ratio of partitions to sample, passed to :meth:`RDD.sample`
		
		:return: estimated size in bytes
		"""
		rdd = df.rdd

		def partition_size ( iterator ):
			"""
			Called by :meth:`RDD.mapPartitions` over a partition sample.
			Sums the sizes of each partition.
			"""
			iterator = map ( sys.getsizeof, iterator )
			total = sum ( iterator )
			yield total


		# sample ratio at the partition level
		sampled_sizes = rdd.sample ( False, sample_ratio )\
			.mapPartitions ( partition_size )\
			.collect()

		if not sampled_sizes: return 0

		avg_partition_size = sum ( sampled_sizes ) / len ( sampled_sizes )
		estimated_partitions = rdd.getNumPartitions ()
		estimated_total = int ( avg_partition_size * estimated_partitions / sample_ratio )

		return estimated_total


	def df_new_partition_size ( estimated_df_size: int, target_partition_size: int = 256 * 1024 ** 2 ) -> int:
		"""
		Choose a reasonable no of partitions to be used by :meth:`df_save`, so each output file is 
		~target_partition_size bytes.

		## Parameters:

		:param estimated_df_size: estimated size of the DF, in bytes
		:param target_partition_size: target size of each partition file, in bytes
		:return: no of partitions to use
		"""
		if estimated_df_size == 0: return 1

		partitions = max ( 1, estimated_df_size // target_partition_size )
		return partitions




def assertDataFrameEqualX ( 
	df1: Union[DataFrame, "pandas.DataFrame", "pyspark.pandas.DataFrame", List[Row]],
	df2: Union[DataFrame, "pandas.DataFrame", "pyspark.pandas.DataFrame", List[Row]], 
	msg = "", 
	**kwargs 
):
	"""
	An extension of :func:`assertDataFrameEqual` to add a custom failure message.

	Possible @kwargs are passed to :func:`assertDataFrameEqual`, in addition to the two DF to compare.
	"""
	try:
		assertDataFrameEqual ( df1, df2, **kwargs )
	except AssertionError as ex:
		raise AssertionError ( f"{msg}" ) from ex


def areDataFramesEqual (
	df1: Union[DataFrame, "pandas.DataFrame", "pyspark.pandas.DataFrame", List[Row]],
	df2: Union[DataFrame, "pandas.DataFrame", "pyspark.pandas.DataFrame", List[Row]], 
	**kwargs 
) -> bool:
	"""
	Tests equality of two DataFrames, returning a boolean.

	Possible @kwargs are passed to :func:`assertDataFrameEqual`, in addition to the two DF to compare.

	This uses :func:`assertDataFrameEqual` internally, catching its AssertionError, ie, might not be
	very efficient.
	"""
	try:
		assertDataFrameEqual ( df1, df2, **kwargs )
		return True
	except AssertionError:
		return False
