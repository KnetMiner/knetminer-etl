"""
TODO: Comment me!

TODO: try SnakeMake unit test tool: https://snakemake.readthedocs.io/en/stable/snakefiles/testing.html

"""

KETL_DATA = os.environ [ "KETL_DATA" ] # TODO
KETL_IN = f"{KETL_DATA}/input"
KETL_OUT = f"{KETL_DATA}/output"

spark_session = None # TODO

rule map_gene_tsv:
	params:
		out_path = f"{KETL_OUT}/genes-triples.parquet"
	input:
		tsv = f"{KETL_IN}/genes.tsv"
	output:
		parquet = DataFrameCheckpointManager.get_intermediate_check_path ( params.out_path )
	run:
		"""
		This is the ugly version. Typically, you'll want to isolate this into 
		a separate .py, which would receive tsv/parquet as params
		
		"""
		tb_mapper = TabFileMapper (
			id_mapper = IdColumnMapper ( column_id = "accession" ),	
			column_mappers = [
				ColumnMapper ( column_id = "name", property = "hasGeneName" ),
				ColumnMapper ( "accession", "hasAccession" ),
				ColumnMapper ( "chromosome", "hasChromosomeId" ),
				ColumnMapper ( "begin", "hasChromosomeBegin", spark_data_type = IntegerType () ),
				ColumnMapper ( "end", "hasChromosomeEnd", spark_data_type = IntegerType () )
			],
			const_prop_mappers = [
				ConstantPropertyMapper.for_type ( "Gene" ),
				ConstantPropertyMapper ( property = "source", constant_value = "TestTSV" )
			],
			spark_options = { "inferSchema": False }
		)

		# TODO: merge the two steps in a TabFileMapper helper method
		triples_df = tb_mapper.map ( spark_session, input.tsv )
		DataFrameCheckpointManager.save_intermediate ( triples_df, output.parquet )

