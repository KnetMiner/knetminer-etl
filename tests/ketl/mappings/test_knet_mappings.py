import json

from assertpy import assert_that
from pyspark.sql import SparkSession
import pytest

from ketl.core import GraphProperty, GraphTriple, PGElementType, ValueConverter
from ketl.io.core import pg_df_2_pg_jsonl, triples_2_pg_df
from ketl.tabmap.core import ColumnTripleMapper, ColumnValueMapper, SparkDataFrameMapper

import ketl.helpers as khelpers
import ketl.tabmap.helpers as tbhelpers

import ketl.mappings.knetminer as knetmaps

vconverter = ValueConverter.get_default ()

@pytest.mark.integration
def test_add_accession_tabmapper ( spark_session: SparkSession ):
	data = [ [ "gene id", "gene name", "accession" ],
		[ "GENE001", "Gene 1", "ACC001" ],
		[ "GENE002", "Gene 2", "ACC002" ],
		[ "GENE003", "Gene 3", "ACC003" ]
	]

	acc_source = "ENSEMBL"

	gene_mapper = SparkDataFrameMapper (
		id_mapper = ColumnValueMapper ( "gene id" ),
		mapper_components = [
			ColumnTripleMapper ( "gene name", "name" ),
			khelpers.type_triple_mapper ( "Gene" ),
			knetmaps.create_accession_tabmapper (
				acc_source_mapper = f"!{acc_source}",
				acc_mapper = "accession"
			)
		]
	)

	df = spark_session.createDataFrame ( data [ 1: ], schema = data [ 0 ] )

	df_gene_triples = gene_mapper.map ( df )

	assert_that ( df_gene_triples.count (), "No of generated triples is as expected" )\
		.is_equal_to ( ( len ( data ) - 1 ) * 3 ) # name + accession + type


	for row in data [ 1: ]:
		gene_id, gene_name, gene_accession = row
		
		# Type is still set
		assert_that (
			df_gene_triples.filter ( 
				f"{GraphTriple.ID_KEY} = '{gene_id}' AND key = '{GraphProperty.TYPE_KEY}' AND value = 'Gene'"
			).collect (),
			f"#{gene_id} has the expected type"
		).is_length ( 1 )


		# The name mapping still works
		cvt_name = vconverter.serialize ( gene_name )
		assert_that (
			df_gene_triples.filter ( 
				f"{GraphTriple.ID_KEY} = '{gene_id}' AND key = 'name' AND value = '{cvt_name}'"
			).collect (),
			f"#{gene_id} has the expected name"
		).is_length ( 1 )

		# The accession mapping works too
		cvt_acc = vconverter.serialize ( f"{acc_source}:{gene_accession}" )
		assert_that (
			df_gene_triples.filter ( 
				f"{GraphTriple.ID_KEY} = '{gene_id}' AND key = 'accessions' AND value = '{cvt_acc}'"
			).collect (),
			f"#{gene_id} has the expected accession"
		).is_length ( 1 )


@pytest.mark.integration
def test_add_accession_tabmapper_source_from_column ( spark_session: SparkSession ):
	data = [ [ "gene id", "gene name", "accession", "source" ],
		[ "GENE001", "Gene 1", "ACC001", "ENSEMBL" ],
		[ "GENE002", "Gene 2", "ACC002", "ENSEMBL" ],
		[ "GENE003", "Gene 3", "ACC003", "TAIR" ]
	]

	gene_mapper = SparkDataFrameMapper (
		id_mapper = ColumnValueMapper ( "gene id" ),
		mapper_components = [
			ColumnTripleMapper ( "gene name", "name" ),
			khelpers.type_triple_mapper ( "Gene" ),
			knetmaps.create_accession_tabmapper (
				acc_source_mapper = "source",
				acc_mapper = "accession"
			)
		]
	)

	df = spark_session.createDataFrame ( data [ 1: ], schema = data [ 0 ] )

	df_gene_triples = gene_mapper.map ( df )

	assert_that ( df_gene_triples.count (), "No of generated triples is as expected" )\
		.is_equal_to ( ( len ( data ) - 1 ) * 3 ) # name + accession + type
	
	for row in data [ 1: ]:
		gene_id, _, gene_accession, gene_acc_source = row

		# Let's check accession only, we're fine with the other test about the rest
		cvt_acc = vconverter.serialize ( f"{gene_acc_source}:{gene_accession}" )
		assert_that (
			df_gene_triples.filter ( 
				f"{GraphTriple.ID_KEY} = '{gene_id}' AND key = 'accessions' AND value = '{cvt_acc}'"
			).collect (),
			f"#{gene_id} has the expected accession"
		).is_length ( 1 )


@pytest.mark.integration
def test_add_accession_tabmapper_source_from_mapper ( spark_session: SparkSession ):
	def source_extractor ( row: dict[str, str] ) -> str | None:
		source = row.get ( "source" )
		if not source: return "_"
		return source.upper ()
	
	data = [ [ "gene id", "gene name", "accession", "source" ],
		[ "GENE001", "Gene 1", "ACC001", "ensembl" ],
		[ "GENE002", "Gene 2", "ACC002", "" ],
		[ "GENE003", "Gene 3", "ACC003", "TAIR" ]
	]
	acc_source_mapper = tbhelpers.row_value_mapper ( fun = source_extractor )

	gene_mapper = SparkDataFrameMapper (
		id_mapper = ColumnValueMapper ( "gene id" ),
		mapper_components = [
			ColumnTripleMapper ( "gene name", "name" ),
			khelpers.type_triple_mapper ( "Gene" ),
			knetmaps.create_accession_tabmapper (
				acc_source_mapper = acc_source_mapper,
				acc_mapper = "accession"
			)
		]
	)

	df = spark_session.createDataFrame ( data [ 1: ], schema = data [ 0 ] )

	df_gene_triples = gene_mapper.map ( df )

	assert_that ( df_gene_triples.count (), "No of generated triples is as expected" )\
		.is_equal_to ( ( len ( data ) - 1 ) * 3 ) # name + accession + type
	
	for row in data [ 1: ]:
		gene_id, _, gene_accession, __ = row
		gene_acc_source = source_extractor ( dict ( zip ( data[0], row ) ) )

		# Let's check accession only, we're fine with the other test about the rest
		cvt_acc = vconverter.serialize ( f"{gene_acc_source}:{gene_accession}" )
		assert_that (
			df_gene_triples.filter ( 
				f"{GraphTriple.ID_KEY} = '{gene_id}' AND key = 'accessions' AND value = '{cvt_acc}'"
			).collect (),
			f"#{gene_id} has the expected accession"
		).is_length ( 1 )


@pytest.mark.integration
def test_add_accession_tabmapper_multiple_accessions ( spark_session: SparkSession ):
	data = [ [ "gene id", "gene name", "accession" ],
		[ "GENE001", "Gene 1", "ACC001.1" ],
		[ "GENE002", "Gene 2", "ACC003" ],
		[ "GENE001", "Gene 1", "ACC001.2" ]
	]

	acc_source = "ENSEMBL"

	gene_mapper = SparkDataFrameMapper (
		id_mapper = ColumnValueMapper ( "gene id" ),
		mapper_components = [
			ColumnTripleMapper ( "gene name", "name" ),
			khelpers.type_triple_mapper ( "Gene" ),
			knetmaps.create_accession_tabmapper (
				acc_source_mapper = f"!{acc_source}",
				acc_mapper = "accession"
			)
		]
	)

	df = spark_session.createDataFrame ( data [ 1: ], schema = data [ 0 ] )

	df_gene_triples = gene_mapper.map ( df )

	assert_that ( df_gene_triples.count (), "No of generated triples is as expected" )\
		.is_equal_to ( ( len ( data ) - 1 ) * 3 ) 
		# 3 * (name, accession, type), name 'Gene 1' is repeated
	
	# Let's check the PG
	df_pg = triples_2_pg_df  ( df_gene_triples )
	pg_jsonl = pg_df_2_pg_jsonl ( df_pg )
	pg_jsonl = [ json.loads ( line ) for line in pg_jsonl.splitlines () ]
	# Converts all the accessions to a set, else it won't compare correctly
	pg_jsonl = [ 
		{ **node, "properties": { **node["properties"], "accessions": set ( node["properties"]["accessions"] ) } } 
			for node in pg_jsonl 
	]	

	expected_pg_node = { 
		"id": data[1][0],
		"labels": [ "Gene" ], 
		"properties": { 
			"name": [ data[1][1] ],
			"accessions": set ( [ f"{acc_source}:{data[1][2]}", f"{acc_source}:{data[3][2]}" ] )
		},
		"type": PGElementType.NODE.value
	}

	assert_that ( pg_jsonl, "PG node is as expected" )\
		.contains ( expected_pg_node )
	
	