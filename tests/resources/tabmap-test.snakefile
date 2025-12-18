"""
TODO: Comment me!

TODO: try SnakeMake unit test tool: https://snakemake.readthedocs.io/en/stable/snakefiles/testing.html

TODO: Map TSV to edges and put all together
TODO: Neo4j loader
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

from ketl import (ConstantPropertyMapper, PGElementType, pg_df_2_pg_jsonl,
                  triples_2_pg_df)
from ketl.spark_utils import DataFrameCheckpointManager
from ketl.tabmap import (ColumnMapper, IdColumnMapper, SparkDataFrameMapper,
                         TabFileMapper)

KETL_DATA = os.environ [ "KETL_DATA" ] # TODO
KETL_IN = os.path.abspath ( workflow.basedir )
KETL_OUT = f"{KETL_DATA}/output"

# TODO: factorise it in a config file, fixture or alike
spark_session = SparkSession.builder\
	.master ( "local[*]" )\
	.appName ( "test_tabmap" )\
	.getOrCreate()		


# Needs to stay here, not in params, since the dynamic output isn't computed during the 
# Snakefile parsing.
mapped_genes_path = f"{KETL_OUT}/genes-triples.parquet"
mapped_genes_check_path = DataFrameCheckpointManager.get_intermediate_check_path ( 
	mapped_genes_path
)

pg_genes_path = f"{KETL_OUT}/genes-pg.parquet"
pg_genes_check_path = DataFrameCheckpointManager.get_intermediate_check_path ( 
	pg_genes_path
)

rule all:
	input:
		f"{KETL_OUT}/knowledge-graph.json"


rule triples_2_json_pg:
	# TODO: Make at least another parquet and have the rule running once per each input
	input:
		triples_df = pg_genes_check_path
	output:
		json_pg = f"{KETL_OUT}/knowledge-graph.json"
	run:
		pg_df_2_pg_jsonl ( pg_genes_path, spark_session, output.json_pg )


rule genes_triples_2_pg_df:
	"""
	TODO: Show how to combine mapping and PG conversion.
	"""
	input:
		triples_df = mapped_genes_check_path
	output:
		pg_df = pg_genes_check_path
	run:
		pg_df = triples_2_pg_df (
			mapped_genes_path,
			PGElementType.NODE,
			spark = spark_session,
			out_path = pg_genes_path
		 )


rule map_gene_tsv:
	input:
		tsv = f"{KETL_IN}/test_genes.tsv"
	output:
		parquet = mapped_genes_check_path
	run:
		"""
		This is the ugly version. Typically, you'll want to isolate this into 
		a separate .py, which would receive tsv/parquet as params.

		TODO: add a rule with that style.
		
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

		tb_mapper.map ( spark_session, input.tsv, out_path = mapped_genes_path )

