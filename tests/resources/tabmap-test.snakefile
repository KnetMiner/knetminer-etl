"""
A SnakeMake workflow that tests tab mapping.

TODO: try SnakeMake unit test tool: https://snakemake.readthedocs.io/en/stable/snakefiles/testing.html

TODO: Map TSV to edges and put all together. To finish this:
  - Test RowValueMapper.from_extractor() in its own test OK
	- Review for_from(), for_to() and alike (too many defaults) OK
	- Finalise snake.py OK
	- Bring snake.py here OK
	- Comments
	- test_infer_schema, test_inconsistent_mappers_on_same_row
	
TODO: Neo4j loader
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

from ketl import (ConstantPropertyMapper, PGElementType, pg_df_2_pg_jsonl,
                  triples_2_pg_df)
from ketl.spark_utils import DataFrameCheckpointManager
from ketl.tabmap import (ColumnMapper, IdColumnMapper, SparkDataFrameMapper,
						 TabFileMapper)

from ketltest.snake import PROTEINS_MAPPER, ENCODING_MAPPER


KETL_DATA = os.environ [ "KETL_DATA" ] # TODO
KETL_IN = os.path.abspath ( workflow.basedir )
KETL_OUT = f"{KETL_DATA}/output"
KETL_TMP = f"{KETL_DATA}/tmp"

# TODO: factorise it in a config file, fixture or alike
spark_session = SparkSession.builder\
	.master ( "local[*]" )\
	.appName ( "test_tabmap" )\
	.getOrCreate()		


# Needs to stay here, not in params, since the dynamic output isn't computed during the 
# Snakefile parsing.
mapped_genes_path = f"{KETL_TMP}/genes-triples.parquet"
mapped_genes_check_path = DataFrameCheckpointManager.get_intermediate_check_path ( 
	mapped_genes_path
)

mapped_proteins_path = f"{KETL_TMP}/proteins-triples.parquet"
mapped_proteins_check_path = DataFrameCheckpointManager.get_intermediate_check_path ( 
	mapped_proteins_path
)

mapped_encodings_path = f"{KETL_TMP}/encodings-triples.parquet"
mapped_encodings_check_path = DataFrameCheckpointManager.get_intermediate_check_path ( 
	mapped_encodings_path
)

node_pg_path = f"{KETL_TMP}/knowledge-graph.parquet"
node_pg_check_path = DataFrameCheckpointManager.get_intermediate_check_path ( 
	node_pg_path
)

rule all:
	input:
		f"{KETL_OUT}/nodes-pg.json",
		f"{KETL_OUT}/edges-pg.json"


rule node_triples_2_json_pg:
	input:
		triples_df_path = node_pg_check_path
	output:
		json_pg = f"{KETL_OUT}/nodes-pg.json"
	run:
		pg_df_2_pg_jsonl ( node_pg_path, spark_session, output.json_pg )


rule node_triples_2_pg_df:
	"""
	Builds a single PG DataFrame from multiple triples DataFrames.

	This shows how to do that by unioning DFs.
	"""
	input:
		triples_df = [ mapped_genes_check_path, mapped_proteins_check_path ]
	output:
		pg_df = node_pg_check_path
	run:
		triples_df = None
		for path in ( mapped_genes_path, mapped_proteins_path ):
			triples_df = \
				spark_session.read.parquet ( path ) if not triples_df \
				else triples_df.unionByName ( spark_session.read.parquet ( path ) )

		pg_df = triples_2_pg_df (
			triples_df,
			PGElementType.NODE,
			spark = spark_session,
			out_path = node_pg_path
		)


rule encodings_triples_2_json_pg:
	"""
	Builds the PG edges and saves them straight to JSON.

	Here, we have only one triples DF for edges, so we can show the alternative step that merges 
	the PG dataframe definition and the JSON dump in one rule. The former is lazy, so the performance 
	doesn't differ much.
	
	Presumably, we'll end up having at least one PG builder/exporter per element type (NODE, EDGE), 
	no matter the option you choose about the PG DF build and JSON dump steps.
	"""

	input:
		triples_df_path = mapped_encodings_check_path
	output:
		json_pg = f"{KETL_OUT}/edges-pg.json"
	run:
		triples_df = spark_session.read.parquet ( mapped_encodings_path )

		pg_df = triples_2_pg_df (
			triples_df,
			PGElementType.EDGE,
			spark = spark_session
		)

		pg_df_2_pg_jsonl ( pg_df, spark_session, output.json_pg )


rule map_gene_tsv:
	input:
		tsv = f"{KETL_IN}/test-genes.tsv"
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
			row_mappers = [
				ColumnMapper ( column_id = "name", property = "hasGeneName" ),
				ColumnMapper ( "accession", "hasAccession" ),
				ColumnMapper ( "chromosome", "hasChromosomeId" ),
				ColumnMapper ( "begin", "hasChromosomeBegin", spark_data_type = IntegerType () ),
				ColumnMapper ( "end", "hasChromosomeEnd", spark_data_type = IntegerType () )
			],
			const_prop_mappers = [
				ConstantPropertyMapper.for_type ( "Gene" ),
				ConstantPropertyMapper ( property = "source", constant_value = "SnakeTest" )
			],
			spark_options = { "inferSchema": False }
		)

		tb_mapper.map ( spark_session, input.tsv, out_path = mapped_genes_path )


rule map_protein_tsv:
	input:
		tsv = f"{KETL_IN}/test-proteins.tsv"
	output:
		parquet = mapped_proteins_check_path
	run:
		"""
		This imports from a file of mappers/config.
		"""
		PROTEINS_MAPPER.map ( spark_session, input.tsv, out_path = mapped_proteins_path )


rule map_encoding_tsv:
	input:
		# The 1-1 links to the genes are included in the proteins file, and our framework
		# allows for mapping the same files to multiple mappers.
		tsv = f"{KETL_IN}/test-proteins.tsv"
	output:
		parquet = mapped_encodings_check_path
	run:
		ENCODING_MAPPER.map ( spark_session, input.tsv, out_path = mapped_encodings_path )
