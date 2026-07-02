"""
Some of the most basic helpers are tested in test_tabmap_core, additional ones are tested here.
"""

import io

from assertpy import assert_that

from ketl.core import GraphProperty
import ketl.helpers as khelpers
import ketl.tabmap.helpers as tbhelpers

from pyspark.sql import DataFrame, SparkSession
import pytest

from ketl.tabmap.core import ColumnTripleMapper, ColumnValueMapper, GenericTabFileMapper, SparkDataFrameMapper, SparkDataFrameMapperBase

from brandizpyes.io import dump_output

import tempfile


@pytest.mark.integration
def test_df_mappers_chain ( 
	tsv_data: list[list[str]], 
	tsv_df: DataFrame,
	mapper_chain: SparkDataFrameMapperBase,
	gene2go_mapper: SparkDataFrameMapper
):
	triples_df = mapper_chain.map ( tsv_df )

	triples_df.show ( n = 1000, truncate = False )
  
	expected_size = 2*3 + 2*3 + 3*3 # genes(name, type) + gos(label, type) + edges(type, from, to)
	assert_that ( triples_df.count (), "Chained mapper yields the right no. of triples" )\
		.is_equal_to ( expected_size )

	for row in tsv_data [ 1: ]:
		(name, acc, go_acc, go_label) = row
		df_gene_name = triples_df.filter ( f"id = '{acc}' AND key = 'name' AND value = '\"{name}\"'" )
		assert_that ( df_gene_name.count (), f"Gene {acc} found" ).is_equal_to ( 1 )
		
		df_gene_type = triples_df.filter ( f"id = '{acc}' AND key = '{GraphProperty.TYPE_KEY}' AND value = 'Gene'" )
		assert_that ( df_gene_type.count (), f"Gene {acc} type found" ).is_equal_to ( 1 )

		df_go = triples_df.filter ( f"id = '{go_acc}' AND key = 'label' AND value = '\"{go_label}\"'" )
		assert_that ( df_go.count (), f"GO {go_acc} found" ).is_equal_to ( 1 )

		df_go_type = triples_df.filter ( f"id = '{go_acc}' AND key = '{GraphProperty.TYPE_KEY}' AND value = 'GOTerm'" )
		assert_that ( df_go_type.count (), f"GO {go_acc} type found" ).is_equal_to ( 1 )

		row_dict = dict ( zip ( tsv_data [ 0 ], row ) )
		link_id = gene2go_mapper.id_mapper.value( row_dict )
		df_link = triples_df.filter ( f"id = '{link_id}'" )

		# 1 from, 1 to, 1 type
		assert_that ( df_link.count (), f"Link {link_id} found" ).is_equal_to ( 3 )

		# Check the link type, from, to
		df_link_type = df_link.filter ( f"id = '{link_id}' AND key = '{GraphProperty.TYPE_KEY}' AND value = 'hasFunction'" )
		assert_that ( df_link_type.count (), f"Link {link_id} type found" ).is_equal_to ( 1 )
		df_link_from = df_link.filter ( f"id = '{link_id}' AND key = '{GraphProperty.FROM_KEY}' AND value = '{acc}'" )
		assert_that ( df_link_from.count (), f"Link {link_id} from found" ).is_equal_to ( 1 )
		df_link_to = df_link.filter ( f"id = '{link_id}' AND key = '{GraphProperty.TO_KEY}' AND value = '{go_acc}'" )
		assert_that ( df_link_to.count (), f"Link {link_id} to found" ).is_equal_to ( 1 )


@pytest.mark.integration
def test_df_mappers_chain_to_tab_file_mapper (
	spark_session: SparkSession,
	tsv_data: list[list[str]],
	mapper_chain: SparkDataFrameMapperBase
):
	"""
	Tests the :meth:`ketl.tabmap.SparkDataFrameMapperBase.to_tab_file_mapper` helper, when 
	applied to a mapper chain.
	"""
	tab_mapper = mapper_chain.to_tab_file_mapper ()
	assert_that ( tab_mapper, "the tab file mapper correctly created" )\
		.is_instance_of ( GenericTabFileMapper )
	assert_that ( tab_mapper.data_frame_mapper, "tab file mapper has the correct DF mapper" )\
		.is_equal_to ( mapper_chain )
	
	# Just in case, let's ensure it does the mapping job
	
	# First, we need to dump the data into a temp CSV
	tmp_tsv_path = tempfile.gettempdir () + "/test_df_mappers_chain_to_tab_file_mapper.tsv"
	dump_output ( 
		writer = lambda f: f.write ( "\n".join ( [ "\t".join ( row ) for row in tsv_data ] ) ),
		out_sink = tmp_tsv_path
	)

	triples_df = tab_mapper.map ( spark_session, tmp_tsv_path )

	expected_size = 2*3 + 2*3 + 3*3 # genes(name, type) + gos(label, type) + edges(type, from, to)
	assert_that ( triples_df.count (), "tab file mapper yields the right no. of triples" )\
		.is_equal_to ( expected_size )



@pytest.fixture ( scope = "session" )
def tsv_data () -> list[list[str]]:
	return [ [ "gene name", "accession", "go accession", "go label" ],
		[ "P53", "ACC001", "GO:0000001", "mitochondrion inheritance" ],
		[ "TPL", "ACC002", "GO:0000002", "mitochondrial genome maintenance" ],
		[ "BLAH", "ACC003", "GO:0000003", "reproduction" ]
	]


@pytest.fixture ( scope = "session" )
def tsv_df ( spark_session: SparkSession, tsv_data: list[list[str]] ) -> DataFrame:
	"""
	Helper to build a Spark DataFrame from the TSV data in the `tsv_data` fixture.
	"""
	return spark_session.createDataFrame ( tsv_data [ 1: ], schema = tsv_data [ 0 ] )


@pytest.fixture (scope = "session" )
def mapper_chain ( 
	gene_mapper: SparkDataFrameMapper, 
	go_mapper: SparkDataFrameMapper, 
	gene2go_mapper: SparkDataFrameMapper
) -> SparkDataFrameMapperBase:
	all_mapper = tbhelpers.df_mappers_chain ( gene_mapper, go_mapper, gene2go_mapper )
	return all_mapper


@pytest.fixture (scope = "session" )
def gene_mapper () -> SparkDataFrameMapper:
	return SparkDataFrameMapper ( 
		id_mapper = ColumnValueMapper ( "accession" ),
		mapper_components = [
			ColumnTripleMapper ( "gene name", "name" ),
			khelpers.type_triple_mapper ( "Gene" )
		]
	)


@pytest.fixture (scope = "session" )
def go_mapper () -> SparkDataFrameMapper:
	return SparkDataFrameMapper (
		id_mapper = ColumnValueMapper ( "go accession" ),
		mapper_components = [
			ColumnTripleMapper ( "go label", "label" ),
			khelpers.type_triple_mapper ( "GOTerm" )
		]
	)


@pytest.fixture (scope = "session" )
def gene2go_mapper ( gene_mapper: SparkDataFrameMapper, go_mapper: SparkDataFrameMapper ) -> SparkDataFrameMapper:
	return SparkDataFrameMapper (
		id_mapper = SparkDataFrameMapper.AutoEdgeId (),
		mapper_components = [
			# TODO: check AgriSchemas
			khelpers.type_triple_mapper ( "hasFunction" ),
			tbhelpers.edge_source_row_triple_mapper ( gene_mapper.id_mapper ),
			tbhelpers.edge_target_row_triple_mapper ( go_mapper.id_mapper )
		]
	)
