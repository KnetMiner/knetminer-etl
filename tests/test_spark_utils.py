import logging
import uuid

import pytest
from assertpy import assert_that
from pyspark.sql import SparkSession

from ketl.spark_utils import DataFrameCheckpointManager

log = logging.getLogger ( __name__ )

def test_checkpoint_manager ():

	spark = SparkSession.builder.master ( "local[*]" )\
		.appName ( "test_checkpoint_manager" )\
		.getOrCreate()

	test_data = [ (1, "Alice"), (2, "Bob"), (3, "Cathy") ]
	test_headers = [ "id", "name" ]
	test_df = spark.createDataFrame ( test_data, test_headers )

	# Save
	ckpt_path = f"/tmp/ketl_spark_utils_checkpoint_{uuid.uuid4().hex[:8]}.parquet"

	DataFrameCheckpointManager.save_intermediate ( test_df, ckpt_path )

	# Restart Spark and then reload
	spark.stop()
	spark = SparkSession.builder.master ( "local" )\
		.appName ( "test" )\
		.getOrCreate()
	
	# Let's test this function here, it's so small, not worth a dedicated test
	assert_that ( 
		DataFrameCheckpointManager.get_intermediate_check_path ( ckpt_path ),
		"The checkpoint file exists" 
	).exists ()
	
	loaded_df = DataFrameCheckpointManager.load_intermediate ( ckpt_path, spark )

	# And now check

	assert_that ( loaded_df.columns, "Reloaded DF has the same schema" )\
		.is_equal_to ( test_headers )
	
	loaded_tuples = set( tuple(row) for row in loaded_df.collect() )
	test_tuples = set( tuple(row) for row in test_data )

	assert_that ( loaded_tuples, "Reloaded DF has the same data" )\
		.is_equal_to ( test_tuples )
	
	# Saved .parquet has 1 partition only. This is internal, it's transparent to the client.
	assert_that ( loaded_df.rdd.getNumPartitions (), "Reloaded DF has 1 partition" )\
		.is_equal_to ( 1 )

	spark.stop()


@pytest.mark.parametrize (
	ids = [ "reduce", "increase" ],
	argnames = "n_original_partitions, n_induced_partitions",
	argvalues = [ (10, 2), (1, 5) ]
)
def test_checkpoint_manager_repartition ( n_original_partitions: int, n_induced_partitions: int ):
	"""
	Tests that internally, :meth:`DataFrameCheckpointManager.save_intermediate` repartitions 
	a DF, either by reducing the partitions or increasing them, depending on the original
	DF size.

	## Parameters:
	:param n_original_partitions: number of partitions to create the original DF with

	:param n_induced_partitions: number of partitions to induce when 
	  :meth:`DataFrameCheckpointManager.save_intermediate` computes the no of partitions for
		the saved DF (see the test implementation for details)
	"""
	spark = SparkSession.builder.master ( "local[*]" )\
		.appName ( "test_checkpoint_manager_repartition" )\
		.getOrCreate()
	
	# Create a DF with many partitions
	test_data = [ (i, f"Name_{i}") for i in range (1000) ]
	test_headers = [ "id", "name" ]
	test_df = spark.createDataFrame ( test_data, test_headers )\
		.repartition ( n_original_partitions )

	# Save
	ckpt_path = f"/tmp/ketl_spark_utils_checkpoint_{uuid.uuid4().hex[:8]}.parquet"

	# Induce n_induced_partitions partitions, by using the estimated size and setting an adequate target_partition_size
	size = DataFrameCheckpointManager.get_df_rough_size ( test_df )

	# It's worth this sanity test
	assert_that ( size, "Estimated DF size is consistent" )\
		.is_positive ()

	target_partition_size = size // n_induced_partitions
	saved_df = DataFrameCheckpointManager.save_intermediate ( test_df, ckpt_path, target_partition_size = target_partition_size )

	n_partitions = saved_df.rdd.getNumPartitions ()
	log.info ( 
		"test_checkpoint_manager_repartition(), saved partitions: " +
		f"{n_partitions} (planned {n_induced_partitions}, original {n_original_partitions})"
	)

	# Should have repartitioned to n_induced_partitions
	# This is the only thing we can test here, since the computed partitions depend on the estimated
	# DF size, which in turn, depends on a random sample, which, of course, varies randomly.
	#
	is_consistent = \
		n_original_partitions > n_induced_partitions and n_partitions < n_original_partitions \
		or n_partitions > n_original_partitions
	assert_that ( is_consistent, f"Reloaded DF has the no of expected partitions" ) \
		.is_true ()
		

	# Restart Spark, reload and test the reloaded data, just in case
	spark.stop()
	spark = SparkSession.builder.master ( "local" )\
		.appName ( "test" )\
		.getOrCreate()
	
	loaded_df = DataFrameCheckpointManager.load_intermediate ( ckpt_path, spark )

	loaded_tuples = set( tuple ( row ) for row in loaded_df.collect () )
	test_tuples = set( tuple ( row ) for row in test_data )

	assert_that ( loaded_df.columns, "Reloaded DF has the same schema" )\
		.is_equal_to ( test_headers )
	assert_that ( loaded_tuples, "Reloaded DF has the same data" )\
		.is_equal_to ( test_tuples )
	
	spark.stop()
