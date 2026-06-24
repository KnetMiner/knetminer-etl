"""
Some of the most basic helpers are tested in test_tabmap_core, additional ones are tested here.
"""

from assertpy import assert_that

from ketl.core import GraphProperty
import ketl.helpers as khelpers
import ketl.tabmap.helpers as tbhelpers

from pyspark.sql import SparkSession
import pytest

from ketl.tabmap.core import ColumnTripleMapper, ColumnValueMapper, SparkDataFrameMapper

@pytest.mark.integration
@pytest.mark.usefixtures ( "spark_session" )
def test_df_mappers_chain ( spark_session: SparkSession ):
	csv = [ [ "gene name", "accession", "go accession", "go label" ],
		[ "P53", "ACC001", "GO:0000001", "mitochondrion inheritance" ],
		[ "TPL", "ACC002", "GO:0000002", "mitochondrial genome maintenance" ],
		[ "BLAH", "ACC003", "GO:0000003", "reproduction" ]
	]

	gene_mapper = SparkDataFrameMapper ( 
		id_mapper = ColumnValueMapper ( "accession" ),
		mapper_components = [
			ColumnTripleMapper ( "gene name", "name" ),
			khelpers.type_triple_mapper ( "Gene" )
		]
	)

	go_mapper = SparkDataFrameMapper (
		id_mapper = ColumnValueMapper ( "go accession" ),
		mapper_components = [
			ColumnTripleMapper ( "go label", "label" ),
			khelpers.type_triple_mapper ( "GOTerm" )
		]
	)

	gene2go_mapper = SparkDataFrameMapper (
		id_mapper = SparkDataFrameMapper.AutoEdgeId (),
		mapper_components = [
			# TODO: check AgriSchemas
			khelpers.type_triple_mapper ( "hasFunction" ),
			tbhelpers.edge_source_row_triple_mapper ( gene_mapper.id_mapper ),
			tbhelpers.edge_target_row_triple_mapper ( go_mapper.id_mapper )
		]
	)

	all_mapper = tbhelpers.df_mappers_chain ( gene_mapper, go_mapper, gene2go_mapper )

	df = spark_session.createDataFrame ( csv [ 1: ], schema = csv [ 0 ] )
	df_triples = all_mapper.map ( df )

	df_triples.show ( n = 1000, truncate = False )
  
	expected_size = 2*3 + 2*3 + 3*3 # genes(name, type) + gos(label, type) + edges(type, from, to)
	assert_that ( df_triples.count (), "Chained mapper yields the right no. of triples" )\
		.is_equal_to ( expected_size )

	for row in csv [ 1: ]:
		(name, acc, go_acc, go_label) = row
		df_gene_name = df_triples.filter ( f"id = '{acc}' AND key = 'name' AND value = '\"{name}\"'" )
		assert_that ( df_gene_name.count (), f"Gene {acc} found" ).is_equal_to ( 1 )
		
		df_gene_type = df_triples.filter ( f"id = '{acc}' AND key = '{GraphProperty.TYPE_KEY}' AND value = 'Gene'" )
		assert_that ( df_gene_type.count (), f"Gene {acc} type found" ).is_equal_to ( 1 )

		df_go = df_triples.filter ( f"id = '{go_acc}' AND key = 'label' AND value = '\"{go_label}\"'" )
		assert_that ( df_go.count (), f"GO {go_acc} found" ).is_equal_to ( 1 )

		df_go_type = df_triples.filter ( f"id = '{go_acc}' AND key = '{GraphProperty.TYPE_KEY}' AND value = 'GOTerm'" )
		assert_that ( df_go_type.count (), f"GO {go_acc} type found" ).is_equal_to ( 1 )

		row_dict = dict ( zip ( csv [ 0 ], row ) )
		link_id = gene2go_mapper.id_mapper.value( row_dict )
		df_link = df_triples.filter ( f"id = '{link_id}'" )

		# 1 from, 1 to, 1 type
		assert_that ( df_link.count (), f"Link {link_id} found" ).is_equal_to ( 3 )

		# Check the link type, from, to
		df_link_type = df_link.filter ( f"id = '{link_id}' AND key = '{GraphProperty.TYPE_KEY}' AND value = 'hasFunction'" )
		assert_that ( df_link_type.count (), f"Link {link_id} type found" ).is_equal_to ( 1 )
		df_link_from = df_link.filter ( f"id = '{link_id}' AND key = '{GraphProperty.FROM_KEY}' AND value = '{acc}'" )
		assert_that ( df_link_from.count (), f"Link {link_id} from found" ).is_equal_to ( 1 )
		df_link_to = df_link.filter ( f"id = '{link_id}' AND key = '{GraphProperty.TO_KEY}' AND value = '{go_acc}'" )
		assert_that ( df_link_to.count (), f"Link {link_id} to found" ).is_equal_to ( 1 )
