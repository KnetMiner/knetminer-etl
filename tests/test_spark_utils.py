import logging
import uuid

import pytest
from assertpy import assert_that
from pyspark.sql import SparkSession

from ketl.spark_utils import DataFrameCheckpointManager

log = logging.getLogger ( __name__ )


@pytest.mark.usefixtures ( "spark_session" )
class TestDataFrameCheckpointManager:
	def test_saving_n_loading ( self, spark_session: SparkSession ):
		test_data = [ (1, "Alice"), (2, "Bob"), (3, "Cathy") ]
		test_headers = [ "id", "name" ]
		test_df = spark_session.createDataFrame ( test_data, test_headers )

		# Save
		ckpt_path = f"/tmp/ketl_spark_utils_checkpoint_{uuid.uuid4().hex[:8]}.parquet"

		DataFrameCheckpointManager.df_save ( test_df, ckpt_path )


		# Let's test this function here, it's so small, not worth a dedicated test
		assert_that ( 
			DataFrameCheckpointManager.df_check_path ( ckpt_path ),
			"The checkpoint file exists" 
		).exists ()
		
		loaded_df = DataFrameCheckpointManager.df_load ( ckpt_path, spark_session )

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

	# /test_saving_n_loading


	@pytest.mark.parametrize (
		ids = [ "basic", "not_check_path", "empty", "none" ],
		argnames = "test_path, expected_path",
		argvalues = [
			( "/tmp/some_intermediate.parquet/_SUCCESS", "/tmp/some_intermediate.parquet" ),
			( "/tmp/some_intermediate.parquet", "/tmp/some_intermediate.parquet" ),
			( "", "" ),
			( None, None )
		]
	)
	def test_get_intermediate_path ( self, test_path, expected_path ):
		"""
		Tests :meth:`DataFrameCheckpointManager.get_intermediate_path`
		"""
		
		resolved_path = DataFrameCheckpointManager.df_path ( test_path )

		assert_that ( resolved_path, "Resolved path is correct" )\
			.is_equal_to ( expected_path )


	def test_check_path_auto_conversion ( self, spark_session: SparkSession ):
		"""
		Tests that :meth:`DataFrameCheckpointManager.save_intermediate` and
		:meth:`DataFrameCheckpointManager.load_intermediate` work correctly when they're given a check path
		as input/output path.
		"""
		
		test_data = [ (1, "Alice"), (2, "Bob"), (3, "Cathy") ]
		test_headers = [ "id", "name" ]
		test_df = spark_session.createDataFrame ( test_data, test_headers )
		
		# Save
		out_path = f"/tmp/ketl_spark_utils_checkpoint_{uuid.uuid4().hex[:8]}.parquet"
		check_path = DataFrameCheckpointManager.df_check_path ( out_path )
		DataFrameCheckpointManager.df_save ( test_df, check_path )

		# The out_path must exist
		assert_that ( out_path, "Output path exists" ).exists ()

		# So, let's try the loading
		loaded_df = DataFrameCheckpointManager.df_load ( check_path, spark_session )

		# If there is no error, it went fine. Let's check the DF just in case
		assert_that ( loaded_df.count (), "Reloaded DF has the right cardinality" )\
			.is_equal_to ( test_df.count () )
		

	@pytest.mark.parametrize (
		ids = [ "reduce", "increase" ],
		argnames = "n_original_partitions, n_induced_partitions",
		argvalues = [ (10, 2), (1, 5) ]
	)
	def test_checkpoint_manager_repartition ( 
		self, n_original_partitions: int, n_induced_partitions: int, spark_session: SparkSession
	):
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
		# Create a DF with many partitions
		test_data = [ (i, f"Name_{i}") for i in range (1000) ]
		test_headers = [ "id", "name" ]
		test_df = spark_session.createDataFrame ( test_data, test_headers )\
			.repartition ( n_original_partitions )

		# Save
		ckpt_path = f"/tmp/ketl_spark_utils_checkpoint_{uuid.uuid4().hex[:8]}.parquet"

		# Induce n_induced_partitions partitions, by using the estimated size and setting an adequate target_partition_size
		size = DataFrameCheckpointManager.df_rough_size ( test_df )

		# It's worth this sanity test
		assert_that ( size, "Estimated DF size is consistent" )\
			.is_positive ()

		target_partition_size = size // n_induced_partitions
		saved_df = DataFrameCheckpointManager.df_save ( test_df, ckpt_path, target_partition_size = target_partition_size )

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
			
		
		loaded_df = DataFrameCheckpointManager.df_load ( ckpt_path, spark_session )

		loaded_tuples = set( tuple ( row ) for row in loaded_df.collect () )
		test_tuples = set( tuple ( row ) for row in test_data )

		assert_that ( loaded_df.columns, "Reloaded DF has the same schema" )\
			.is_equal_to ( test_headers )
		assert_that ( loaded_tuples, "Reloaded DF has the same data" )\
			.is_equal_to ( test_tuples )
		
	# /test_checkpoint_manager_repartition
