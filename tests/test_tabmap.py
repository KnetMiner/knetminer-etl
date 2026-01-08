import logging
import os
import warnings

import pytest
from assertpy import assert_that
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType

from ketl import ConstantPropertyMapper, GraphTriple, IdentityValueConverter
from ketl.spark_utils import assertDataFrameEqualX
from ketl.tabmap import (ColumnMapper, ColumnValueMapper, IdColumnMapper, RowTripleMapperMixin, RowValueMapper,
                         SparkDataFrameMapper, TabFileMapper)

log = logging.getLogger ( __name__ )

class TestRowValueMapper:
	def test_from_extractor ( self ):
		extractor = lambda row: f"ENSEMBL:{row [ 'accession' ].upper ()} ({row ['name']})"

		rvmap = RowValueMapper.from_extractor (
			extractor = extractor,
			column_ids = [ "accession", "name" ]
		 )
		row = { "accession": "ENSG00000139618", "name": "BRCA2", "other": "foo" }
		mapped_value = rvmap.value ( row )
		assert_that ( mapped_value, "The test custom mapper returns correct edge ID" )\
			.is_equal_to ( '"' + extractor ( row ) + '"' )
		
	@pytest.mark.parametrize ( 
		"prefix",
		[ None, "test:" ],
		ids = [ "regular", "with-prefix" ]
	)
	def test_for_edge_id ( self, prefix ):
		relation_type = "encodes-protein"

		rvmap = RowValueMapper.for_edge_id (
			relation_type = relation_type,
			from_column_id = "gene accession",
			to_column_id = "protein accession",
			prefix = prefix
		)

		row = { "gene accession": "GENE001", "protein accession": "PROT001" }
		mapped_value = rvmap.value ( row )
		if not prefix: prefix = ""
		expected_value = f"{prefix}{relation_type}_{row['gene accession']}_{row['protein accession']}"

		assert_that ( mapped_value, "for_edge_id() returns the expected edge ID" )\
			.is_equal_to ( expected_value )
		
	@pytest.mark.parametrize ( 
		"prefix",
		[ None, "test:" ],
		ids = [ "regular", "with-prefix" ]
	)
	def test_for_edge_id_auto ( self, prefix ):
		relation_type = "encodes-protein"

		type_map = ConstantPropertyMapper.for_type ( relation_type )
		from_map = ColumnMapper.for_from ( "gene accession" )
		to_map = ColumnMapper.for_to ( "protein accession" )

		edge_id_map = RowValueMapper.for_edge_id_auto (
			property_mappers = [ type_map, from_map, to_map ],
			prefix = prefix
		)

		row = { "gene accession": "GENE002", "protein accession": "PROT002" }
		mapped_value = edge_id_map.value ( row )
		if not prefix: prefix = ""
		expected_value = f"{prefix}{relation_type}_{row['gene accession']}_{row['protein accession']}"

		assert_that ( mapped_value, "for_edge_id_auto() returns the expected edge ID" )\
			.is_equal_to ( expected_value )
# /TestRowValueMapper


class TestRowTripleMapperMixin:
	def test_extractor ( self ):
		ex_ns = "http://example.org/resource/"
		extractor = lambda row: f"{ex_ns}encodes_{row[ 'gene accession'] }_{row[ 'protein accession' ]}"

		prop = "uri"
		triple_id = "edge1"
		row = { "gene accession": "gene001", "protein accession": "prot001" }

		mapper = RowTripleMapperMixin.from_extractor (
			extractor = extractor,
			property = prop,
			column_ids = [ "gene accession", "protein accession" ],
			value_converter = IdentityValueConverter ()
		)
		mapped_triple = mapper.triple ( triple_id, row )
		expected_triple_id = GraphTriple ( triple_id, prop, extractor ( row ) )

		assert_that ( mapped_triple, "The test custom mapper returns the expected triple" )\
			.is_equal_to ( expected_triple_id )
		
	def test_for_from ( self ):
		extractor = lambda row: f"ENSEMBL:{row [ 'gene accession' ].upper ()}"
		
		mapper = RowTripleMapperMixin.for_from (
			extractor = extractor,
			column_ids = [ "gene accession" ]
		)

		triple_id = "edge001"
		row = { "gene accession": "gene002", "protein accession": "prot002" }

		mapped_triple = mapper.triple ( triple_id, row )
		expected_triple = GraphTriple ( triple_id, GraphTriple.FROM_KEY, extractor ( row ) )

		assert_that ( mapped_triple, "for_from() returns the expected triple" )\
			.is_equal_to ( expected_triple )

	def test_for_to ( self ):
		extractor = lambda row: f"UNIPROT:{row [ 'protein accession' ].upper ()}"
		
		mapper = RowTripleMapperMixin.for_to (
			extractor = extractor,
			column_ids = [ "protein accession" ]
		)

		triple_id = "edge002"
		row = { "gene accession": "gene003", "protein accession": "prot003" }

		mapped_triple = mapper.triple ( triple_id, row )
		expected_triple = GraphTriple ( triple_id, GraphTriple.TO_KEY, extractor ( row ) )

		assert_that ( mapped_triple, "for_to() returns the expected triple" )\
			.is_equal_to ( expected_triple )
# /TestRowTripleMapperMixin


class TestColumnValueMapper:
	def test_basics ( self ):
		cvmap = ColumnValueMapper ( "name" )
		test_value = "Alice"
		row = { "name": test_value, "age": 20 }
		v = cvmap.value ( row )
		# Values are always serialised when mapped to DF/GraphTriple(s)
		assert_that ( v, "value() works correctly for string" ).is_equal_to ( f'"{test_value}"' )

	def test_numeric_value ( self ):
		cvmap = ColumnValueMapper ( "age" )
		test_value = 30
		row = { "name": "Bob", "age": test_value }
		v = cvmap.value ( row )
		# Numeric values are serialised too
		assert_that ( v, "value() works correctly for number" ).is_equal_to ( f'{test_value}' )
		
	def test_value_serializer ( self ):
		cvmap = ColumnValueMapper ( "name", pre_serializers = lambda v: v.upper () )
		test_value = "Alice"
		row = { "name": test_value, "age": 20 }
		v = cvmap.value ( row )
		# As above, we should get the converted and serialised value
		assert_that ( v, "value() works correctly with serializer" ).is_equal_to ( f'"{test_value.upper ()}"' )
		
	def test_missing_column ( self ):
		cvmap = ColumnValueMapper ( "name" )
		row = { "age": 20 }
		v = cvmap.value ( row )
		assert_that ( v, "value() returns None for missing column" ).is_none ()

	def test_empty_row ( self ):
		cvmap = ColumnValueMapper ( "name" )
		row = {}
		v = cvmap.value ( row )
		assert_that ( v, "value() returns None for empty row" ).is_none ()
# /TestColumnValueMapper

class TestIdColumnMapper:
	def test_basics ( self ):
		idmap = IdColumnMapper ( "id" )
		test_id = "N001"
		row = { "id": test_id, "name": "Alice" }
		v = idmap.value ( row )
		assert_that ( v, "value() returns correct ID" ).is_equal_to ( test_id )

	def test_missing_column ( self ):
		idmap = IdColumnMapper ( "id" )
		row = { "name": "Alice" }
		assert_that ( lambda: idmap.value ( row ), "value() fails with missing column" )\
			.raises ( ValueError )
		
	def test_empty_value ( self ):
		idmap = IdColumnMapper ( "id" )
		row = { "id": "", "name": "Alice" }
		assert_that ( lambda: idmap.value ( row ), "value() fails with empty ID" )\
			.raises ( ValueError )
# /TestIdColumnMapper

class TestColumnMapper:
	def test_basics ( self ):
		cmap = ColumnMapper ( "name", "hasName" )
		test_value = "Alice"
		row = { "name": test_value, "age": 20, "foo": "bar" }
		rec = cmap.triple ( "N001", row )
		assert_that ( rec, "node_record() returns a triple" ).is_not_none ()
		assert_that ( rec.id, "Triple has correct id" ).is_equal_to ( "N001" )
		assert_that ( rec.key, "Triple has correct key" ).is_equal_to ( "hasName" )
		assert_that ( rec.value, "Triple has correct value" ).is_equal_to ( f'"{test_value}"' )

	def test_missing_column ( self ):
		cmap = ColumnMapper ( "name", "hasName" )
		row = { "age": 20, "foo": "bar" }
		rec = cmap.triple ( "N001", row )
		assert_that ( rec, "node_record() returns None for missing column" ).is_none ()
		
	def test_empty_row ( self ):
		cmap = ColumnMapper ( "name", "hasName" )
		row = {}
		rec = cmap.triple ( "N001", row )
		assert_that ( rec, "node_record() returns None for empty row" ).is_none ()
# /TestColumnMapper


@pytest.mark.usefixtures ( "spark_session" )
class TestSparkDataFrameMapper:

	def test_map ( self, spark_session ):
		data = [
			{ "id": "001", "name": "Alice", "age": 30, "city": "Wonderland" },
			{ "id": "002", "name": "Bob", "age": 25, "city": "Builderland" },
			{ "id": "003", "name": None, "age": 22, "city": "Nullville" }
		]
		df = spark_session.createDataFrame ( data )

		id_mapper = IdColumnMapper ( "id" )
		name_mapper = ColumnMapper ( "name", "hasName" )
		age_mapper = ColumnMapper ( "age" )
		
		row_mappers = [ name_mapper, age_mapper ]
		
		df_mapper = SparkDataFrameMapper ( id_mapper, row_mappers = row_mappers )
		triples_df = df_mapper.map ( df )
		log.debug ( f"test_map(), mapped triples: {triples_df.collect()}" )

		expected_data = [
			{ "id": "001", "key": "hasName", "value": '"Alice"' },
			{ "id": "001", "key": "age", "value": "30" },			
			{ "id": "002", "key": "hasName", "value": '"Bob"' },
			{ "id": "002", "key": "age", "value": "25" },
			{ "id": "003", "key": "age", "value": "22" }
		]

		expected_df = spark_session.createDataFrame ( expected_data, triples_df.schema )

		assert_that ( triples_df.count (), "Row count matches expected" ).is_equal_to ( expected_df.count () )
		assertDataFrameEqualX ( 
			triples_df, expected_df, ignoreColumnOrder = True, ignoreColumnType = False,
			msg = "map() didn't work"
		)
	# /test_map

	def test_map_constants ( self, spark_session ):
		data = [
			{ "id": "001", "name": "Alice", "age": 30, "city": "Wonderland" },
			{ "id": "002", "name": "Bob", "age": 25, "city": "Builderland" },
		]
		df = spark_session.createDataFrame ( data )

		id_mapper = IdColumnMapper ( "id" )
		name_mapper = ColumnMapper ( "name", "hasName" )
		age_mapper = ColumnMapper ( "age" )
		col_mappers = [ name_mapper, age_mapper ]

		type_mapper = ConstantPropertyMapper.for_type ( "Person" )
		source_mapper = ConstantPropertyMapper ( "source", "TestDataset" )
		const_mappers = [ type_mapper, source_mapper ]
		
		df_mapper = SparkDataFrameMapper ( id_mapper, col_mappers, const_mappers )
		
		triples_df = df_mapper.map ( df )
		log.info ( f"test_map_constants (), mapped triples: {triples_df.collect()}" )

		# Query it and check type/source are there

		type_rows = triples_df.filter ( triples_df.key == GraphTriple.TYPE_KEY ).collect ()
		assert_that ( len ( type_rows ), "Type triples are created" ).is_equal_to ( 2 )

		for row in type_rows:
			assert_that ( row.value, "Triple type value is correct" ).is_equal_to ( "Person" )

		source_rows = triples_df.filter ( triples_df.key == "source" ).collect ()
		assert_that ( len ( source_rows ), "Source triples are created" ).is_equal_to ( 2 )
		
		for row in source_rows:
			assert_that ( row.value, "Triple source value is correct" ).is_equal_to ( '"TestDataset"' )


		# Just in case, let's check we still have the other triples
		for col_mapper in [ name_mapper, age_mapper ]:
			key = col_mapper.property
			key_rows = triples_df.filter ( triples_df.key == key ).collect ()
			assert_that ( len ( key_rows ), f"{key} triples are created" ).is_equal_to ( 2 )

		# And all the row IDs
		ids = [ row [ "id" ] for row in data ]

		id_rows = triples_df.filter ( triples_df [ "id" ].isin ( ids ) ).collect ()
		assert_that (
			len ( id_rows ),
			"All triples are created for all IDs"
		).is_equal_to (
			( 2 + 2 ) * 2 # 2 rows, each with 2 const + 2 col_mappers
		)
	# /test_map_constants

	
	def test_from_extractor_row_mapper ( self, spark_session ):
		data = [
			{ "gene accession": "GENE001", "protein accession": "PROT001", "reference": "122030434" },
			{ "gene accession": "GENE002", "protein accession": "PROT002" }
		]
		df = spark_session.createDataFrame ( data )

		edge_id_mapper = RowValueMapper.for_edge_id (
			relation_type = "encodes-protein",
			from_column_id = "gene accession",
			to_column_id = "protein accession"
		)

		type_mapper = ConstantPropertyMapper.for_type ( "encodes-protein" )

		from_mapper = RowTripleMapperMixin.for_from (
			lambda row: f"ENSEMBL:{row['gene accession']}",
			[ "gene accession" ]
		)

		to_mapper = RowTripleMapperMixin.for_to (
			lambda row: f"UNIPROT:{row['protein accession']}",
			[ "protein accession" ]
		)

		def pmid_extractor ( row ):
			# If it's optional, you've to handle missing values, return None to tell the upstream
			# layers to skip this triple.
			ref = row.get ( "reference", None )
			return f"PMID:{ref}" if ref else None

		pmid_mapper = RowTripleMapperMixin.from_extractor (
			extractor = pmid_extractor,
			property = "hasPMID",
			column_ids = [ "reference" ]
		)

		df_mapper = SparkDataFrameMapper (
			id_mapper = edge_id_mapper,
			row_mappers = [ from_mapper, to_mapper, pmid_mapper ],
			const_prop_mappers = [ type_mapper ]
		)

		triples_df = df_mapper.map ( df )
		mapped_triples = { (row.id, row.key, row.value) for row in triples_df.collect () }
		log.debug ( f"test_from_extractor_row_mapper(), mapped triples: {mapped_triples}" )
		
		expected_triples = {
			( "encodes-protein_GENE001_PROT001", GraphTriple.TYPE_KEY, "encodes-protein" ),
			( "encodes-protein_GENE001_PROT001", GraphTriple.FROM_KEY, "ENSEMBL:GENE001" ),
			( "encodes-protein_GENE001_PROT001", GraphTriple.TO_KEY, "UNIPROT:PROT001" ),
			( "encodes-protein_GENE001_PROT001", "hasPMID", '"PMID:122030434"' ),
			
			( "encodes-protein_GENE002_PROT002", GraphTriple.TYPE_KEY, "encodes-protein" ),
			( "encodes-protein_GENE002_PROT002", GraphTriple.FROM_KEY, "ENSEMBL:GENE002" ),
			( "encodes-protein_GENE002_PROT002", GraphTriple.TO_KEY, "UNIPROT:PROT002" )
		}

		# Note: contains_only() with lists was giving some error about string formatting
		# (assertpy bug?)
		#
		assert_that ( mapped_triples, "Mapper triples are as expected" )\
			.is_equal_to ( expected_triples )
	# /test_from_extractor_row_mapper
		

	def test_auto_edge_id ( self, spark_session ):
		"""
		Tests RowValueMapper.for_edge_id_auto() together with SparkDataFrameMapper.
		"""
		data = [
			{ "gene accession": "GENE001", "protein accession": "PROT001" },
			{ "gene accession": "GENE002", "protein accession": "PROT002" }
		]
		df = spark_session.createDataFrame ( data )

		df_mapper = SparkDataFrameMapper (
			id_mapper = SparkDataFrameMapper.AutoEdgeId ( prefix = "test:" ),
			row_mappers = [ 
				ColumnMapper.for_from ( column_id = "gene accession" ),
				ColumnMapper.for_to ( column_id = "protein accession" )
			],
			const_prop_mappers = [ ConstantPropertyMapper.for_type ( "encodes-protein" ) ]
		)

		triples_df = df_mapper.map ( df )
		mapped_triples = { (row.id, row.key, row.value) for row in triples_df.collect () }
		log.debug ( f"test_auto_edge_id(), mapped triples: {mapped_triples}" )
		
		expected_triples = {
			( "test:encodes-protein_GENE001_PROT001", GraphTriple.TYPE_KEY, "encodes-protein" ),
			( "test:encodes-protein_GENE001_PROT001", GraphTriple.FROM_KEY, "GENE001" ),
			( "test:encodes-protein_GENE001_PROT001", GraphTriple.TO_KEY, "PROT001" ),
			
			( "test:encodes-protein_GENE002_PROT002", GraphTriple.TYPE_KEY, "encodes-protein" ),
			( "test:encodes-protein_GENE002_PROT002", GraphTriple.FROM_KEY, "GENE002" ),
			( "test:encodes-protein_GENE002_PROT002", GraphTriple.TO_KEY, "PROT002" )
		}

		assert_that ( set ( mapped_triples ), "Mapped triples are as expected" )\
			.is_equal_to ( set ( expected_triples ) )
	# /test_auto_edge_id
# /TestSparkDataFrameMapper

@pytest.mark.usefixtures ( "spark_session" )
class TestTabFileMapper:
	def	test_mapping_tsv ( self, spark_session ):

		# This also shows how a configuration in a real case would look like.
		# A config file might define a constant like tb_mapper here and then this
		# might be imported by a Snakemake file and used together with the call
		# to map() below.
		#
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
				ConstantPropertyMapper ( property = "source", constant_value = "TestTSV" )
			],
			spark_options = { "inferSchema": False }
		)

		test_file_path = os.path.dirname ( os.path.abspath ( __file__ ) ) + "/resources/test-genes.tsv"		
		triples_df = tb_mapper.map ( spark_session, test_file_path )


    # Post-execution checks
		# 

		triples = triples_df.collect ()
		log.debug ( f"test_mapping_tsv(), mapped triples: {triples}" )

		assert_that ( triples_df, "We have a data frame back" ).is_instance_of ( DataFrame )
		assert_that ( triples_df.count (), "Number of mapped triples match" )\
			.is_equal_to ( 
				8 # total rows in the TSV
				* ( len ( tb_mapper.data_frame_mapper.row_mappers ) 
			 			+ len ( tb_mapper.data_frame_mapper.const_prop_mappers ) )
			)

		assert_that ( triples_df.columns, "Data frame has the correct columns" )\
			.is_equal_to ( GraphTriple.DATAFRAME_SCHEMA_LIST )
				

		# ENSMBL0003	EGFR	7C	55019017	55211628
		test_id = "ENSMBL0003"
		assert_that ( triples, f"Expected ID {test_id} in the result" )\
			.extracting ( 'id' )\
			.contains ( test_id )


		for key, val in {
			"hasAccession": f'"{test_id}"',
			"hasGeneName": '"EGFR"',
			"hasChromosomeId": '"7C"',
			"hasChromosomeBegin": "55019017",
			"hasChromosomeEnd": "55211628",
			GraphTriple.TYPE_KEY: "Gene",
			"source": '"TestTSV"'
		}.items ():			
			assert_that ( triples, f"Expected {test_id}.{key} in the result" )\
			.extracting ( 'id', 'key', 'value' )\
			.contains ( ( test_id, key, val ) )
	# /test_mapping_tsv


	def test_infer_schema ( self, spark_session ):
		warnings.warn ( "TODO: implement me!" )


	def test_inconsistent_mappers_on_same_row ( self, spark_session ):
		warnings.warn ( "TODO: implement me!" )
# /TestTabFileMapper
