import pytest
from assertpy import assert_that
from pyspark.sql import SparkSession
from pyspark.errors import AnalysisException

from ketl.filtering import df_join_filter

@pytest.mark.integration
def test_df_join_filter_base ( spark_session: SparkSession ):
	expected_gene_rows = [
		( "G001", "Gene 1" ),
		( "G002", "Gene 2" )		
	]

	all_gene_rows = expected_gene_rows + [
		( "G003", "Gene 3" ),
		( "G004", "Gene 4" ),
	]


  # Target to filter 
	gene_df = spark_session.createDataFrame ( 
		schema = [ "gene_id", "gene_name", ],
		data = all_gene_rows
	)

	protein_df = spark_session.createDataFrame ( 
		schema = [ "protein_id", "protein_name", "gene_id" ],
		data = [
			( "P01", "Protein 1", "G001" ), 
			( "P02", "Protein 2", "G002" ), 
		]
	)

	# Filter genes without proteins
	filtered_gene_df = df_join_filter ( 
		target_df = gene_df, reference_df = protein_df, join_cols_or_expr = ( "gene_id", )
	)

	# The result has the same schema as the target
	assert_that ( filtered_gene_df.columns, "Filtered gene dataframe has the target columns" )\
		.is_equal_to ( gene_df.columns )

	# The row set is the same as expected_gene_rows as set
	assert_that ( set ( filtered_gene_df.collect () ), "Filtered gene dataframe is correct" )\
		.is_equal_to ( set ( expected_gene_rows ) )
	

@pytest.mark.integration
def test_df_join_filter_expr ( spark_session: SparkSession ):
	expected_gene_rows = [
		( "G001", "Gene 1" ),
		( "G002", "Gene 2" )		
	]

	all_gene_rows = expected_gene_rows + [
		( "G003", "Gene 3" ),
		( "G004", "Gene 4" ),
	]

	gene_df = spark_session.createDataFrame ( 
		schema = [ "gene_id", "gene_name", ],
		data = all_gene_rows
	)

	protein_df = spark_session.createDataFrame ( 
		schema = [ "protein_id", "protein_name", "gene_id", "gene_name" ],
		data = [
			( "P01", "Protein 1", "G001", "Foo Gene" ), # matches on ID only
			( "P02", "Protein 2", "G999", "Gene 2" ), # matches on name only
		]
	)

	filtered_gene_df = df_join_filter ( 
		target_df = gene_df, reference_df = protein_df, 
		join_cols_or_expr = "target.gene_id = ref.gene_id OR target.gene_name = ref.gene_name"
	)

	assert_that ( filtered_gene_df.columns, "Filtered gene dataframe has the target columns" )\
		.is_equal_to ( gene_df.columns )
	assert_that ( set ( filtered_gene_df.collect () ), "Filtered gene dataframe is correct" )\
		.is_equal_to ( set ( expected_gene_rows ) )


@pytest.mark.integration
def test_df_join_filter_bad_expr ( spark_session: SparkSession ):
	gene_df = spark_session.createDataFrame ( schema = [ "gene_id" ], data = [ ( "G001", ) ] )
	protein_df = spark_session.createDataFrame ( schema = [ "gene_id" ], data = [ ( "G001", ) ] )

	with pytest.raises ( AnalysisException ):
		df_join_filter ( 
			target_df = gene_df, reference_df = protein_df, 
			join_cols_or_expr = "target.gene_id = ref.foo_id"
		).collect ()
