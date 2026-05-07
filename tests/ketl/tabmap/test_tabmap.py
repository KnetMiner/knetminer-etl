import logging
import os

import pytest
from assertpy import assert_that
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, StringType

from ketl.core import (ConstantTripleMapper, GraphTriple,
                       IdentityValueConverter)
from ketl.spark.utils import assertDataFrameEqualX
from ketl.tabmap.core import (ColumnTripleMapper, ColumnValueMapper,
                              RowTripleMapper, RowValueMapper,
                              SparkDataFrameMapper, TabFileMapper)

import ketl.helpers as khelpers
import ketl.tabmap.helpers as tbhelpers

log = logging.getLogger ( __name__ )

class TestRowValueMapper:
	def test_from_extractor ( self ):
		extractor = lambda row: f"ENSEMBL:{row [ 'accession' ].upper ()} ({row ['name']})"

		rvmap = tbhelpers.row_value_mapper ( fun = extractor )
		row = { "accession": "ENSG00000139618", "name": "BRCA2", "other": "foo" }
		mapped_value = rvmap.value ( row )
		assert_that ( mapped_value, "The test custom mapper returns correct edge ID" )\
			.is_equal_to ( extractor ( row ) )
		
	@pytest.mark.parametrize ( 
		argnames = "prefix",
		argvalues = [ None, "test:" ],
		ids = [ "regular", "with prefix" ]
	)
	def test_edge_id_row_value_mapper ( self, prefix ):
		type_id = "encodesProtein"
		rvmap = tbhelpers.edge_id_row_value_mapper (
			type_id = type_id,
			from_column_id = "gene accession",
			to_column_id = "protein accession"
		)
		if prefix:
			rvmap = rvmap.with_value_wrapper ( khelpers.string_value_wrapper ( prefix = prefix ) )

		row = { "gene accession": "GENE001", "protein accession": "PROT001" }
		mapped_value = rvmap.value ( row )
		if not prefix: prefix = ""
		expected_value = f"{prefix}{type_id}:{row['gene accession']}-{row['protein accession']}"

		assert_that ( mapped_value, "for_edge_id() returns the expected edge ID" )\
			.is_equal_to ( expected_value )
		
	@pytest.mark.parametrize ( 
		argnames = "prefix",
		argvalues = [ None, "test:" ],
		ids = [ "regular", "with prefix" ]
	)
	def test_edge_auto_id_row_value_mapper ( self, prefix ):
		if not prefix: prefix = ""
		type_map = khelpers.type_triple_mapper ( "encodesProtein" )
		from_map = tbhelpers.edge_source_row_triple_mapper ( "gene accession" )
		to_map = tbhelpers.edge_target_row_triple_mapper ( "protein accession" )

		edge_id_map = tbhelpers.edge_auto_id_row_value_mapper (
			property_mappers = [ type_map, from_map, to_map ]
		)
		if prefix:
			edge_id_map = edge_id_map.with_value_wrapper ( khelpers.string_value_wrapper ( prefix = prefix ) )

		row = { "gene accession": "GENE002", "protein accession": "PROT002" }
		mapped_value = edge_id_map.value ( row )
		expected_value = f"{prefix}{type_map.constant_value}:{row['gene accession']}-{row['protein accession']}"

		assert_that ( mapped_value, "edge_auto_id_row_value_mapper() returns the expected edge ID" )\
			.is_equal_to ( expected_value )
# /TestRowValueMapper


class TestRowTripleMapper:
	def test_extractor ( self ):
		ex_ns = "http://example.org/resource/"
		extractor = lambda row: f"{ex_ns}encodes_{row[ 'gene accession'] }-{row[ 'protein accession' ]}"

		prop = "uri"
		triple_id = "edge1"
		row = { "gene accession": "gene001", "protein accession": "prot001" }

		mapper = tbhelpers.row_triple_mapper ( extractor, prop )
		
		mapped_triple = mapper.triple ( triple_id, row )
		expected_triple_id = GraphTriple ( triple_id, prop, extractor ( row ) )

		assert_that ( mapped_triple, "The test custom mapper returns the expected triple" )\
			.is_equal_to ( expected_triple_id )
		
	def test_edge_source_row_triple_mapper ( self ):
		extractor = lambda row: f"ENSEMBL:{row [ 'gene accession' ].upper ()}"		
		mapper = tbhelpers.edge_source_row_triple_mapper ( extractor )
		
		triple_id = "edge001"
		row = { "gene accession": "gene002", "protein accession": "prot002" }

		mapped_triple = mapper.triple ( triple_id, row )
		expected_triple = GraphTriple ( triple_id, GraphTriple.FROM_KEY, extractor ( row ) )

		assert_that ( mapped_triple, "edge_source_row_triple_mapper() returns the expected triple" )\
			.is_equal_to ( expected_triple )

	def test_edge_target_row_triple_mapper ( self ):
		extractor = lambda row: f"UNIPROT:{row [ 'protein accession' ].upper ()}"
		mapper = tbhelpers.edge_target_row_triple_mapper ( extractor )
		
		triple_id = "edge002"
		row = { "gene accession": "gene003", "protein accession": "prot003" }

		mapped_triple = mapper.triple ( triple_id, row )
		expected_triple = GraphTriple ( triple_id, GraphTriple.TO_KEY, extractor ( row ) )

		assert_that ( mapped_triple, "edge_target_row_triple_mapper() returns the expected triple" )\
			.is_equal_to ( expected_triple )
# /TestRowTripleMapperMixin


class TestColumnValueMapper:
	def test_basics ( self ):
		cvmap = ColumnValueMapper ( "name" )
		test_value = "Alice"
		row = { "name": test_value, "age": 20 }
		v = cvmap.value ( row )
		assert_that ( v, "value() works correctly for string" ).is_equal_to ( test_value )

	def test_numeric_value ( self ):
		cvmap = ColumnValueMapper ( "age" )
		test_value = 30
		row = { "name": "Bob", "age": test_value }
		v = cvmap.value ( row )
		# Numeric values are serialised too
		assert_that ( v, "value() works correctly for number" ).is_equal_to ( test_value )
		
	def test_with_value_wrapper ( self ):
		default_value = "[NA]"
		cvmap = ColumnValueMapper ( "name" )\
			.with_value_wrapper ( lambda v: f'{v.upper ()}' if v else default_value )
		test_value = "Alice"
		row = { "name": test_value, "age": 20 }
		v = cvmap.value ( row )
		assert_that ( v, "value() works correctly with wrapper" ).is_equal_to ( test_value.upper () )
		assert_that ( cvmap.value ( { "name": None } ), "value() with wrapper returns default for None value" )\
			.is_equal_to ( default_value )
		assert_that ( cvmap.value ( { "name": "" } ), "value() with wrapper returns default for empty value" )\
			.is_equal_to ( default_value )
		assert_that ( cvmap.value ( {} ), "value() with wrapper returns default for missing value" )\
			.is_equal_to ( default_value )
		
	
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


class TestColumnTripleMapper:
	def test_basics ( self ):
		cmap = ColumnTripleMapper ( "name", "hasName" )
		test_value = "Alice"
		row = { "name": test_value, "age": 20, "foo": "bar" }
		triple = cmap.triple ( "N001", row )
		assert_that ( triple, "triple() returns a triple" ).is_not_none ()
		assert_that ( triple.id, "triple() has correct id" ).is_equal_to ( "N001" )
		assert_that ( triple.key, "triple() has correct key" ).is_equal_to ( "hasName" )
		assert_that ( triple.value, "triple() has correct value" ).is_equal_to ( test_value )

	def test_default_property_id ( self ):
		# Either form is valid
		cmap_name = ColumnTripleMapper ( "name" )
		cmap_age = ColumnValueMapper ( "age" ).to_triple_mapper ()

		test_id, test_name, test_age = "N001", "Alice", 20
		row = { "name": test_name, "age": test_age }

		triple_name = cmap_name.triple ( test_id, row )
		triple_age = cmap_age.triple ( test_id, row )

		for triple, expected_triple in [ ( triple_name, GraphTriple ( test_id, "name", test_name ) ), ( triple_age, GraphTriple ( test_id, "age", test_age ) ) ]:
			assert_that ( triple, "triple() returns a triple" ).is_equal_to ( expected_triple )

	def test_missing_column ( self ):
		# That's an alternative way to define them
		cmap = ColumnValueMapper ( "name" ).to_triple_mapper ( "hasName" )
		row = { "age": 20, "foo": "bar" }
		rec = cmap.triple ( "N001", row )
		assert_that ( rec, "triple() returns None for missing column" ).is_none ()
		
	def test_empty_row ( self ):
		cmap = ColumnTripleMapper ( "name", "hasName" )
		row = {}
		rec = cmap.triple ( "N001", row )
		assert_that ( rec, "triple() returns None for empty row" ).is_none ()
# /TestColumnTripleMapper


@pytest.mark.skip ( "TODO: re-enable after TestSparkDataFrameMapper refactoring" )
@pytest.mark.integration
@pytest.mark.usefixtures ( "spark_session" )
class TestSparkDataFrameMapper:

	def test_map ( self, spark_session ):
		data = [
			{ "id": "001", "name": "Alice", "age": 30, "city": "Wonderland" },
			{ "id": "002", "name": "Bob", "age": 25, "city": "Builderland" },
			{ "id": "003", "name": None, "age": 22, "city": "Nullville" }
		]
		df = spark_session.createDataFrame ( data )

		id_mapper = IdColumnValueMapper ( "id" )
		name_mapper = ColumnTripleMapper ( "name", "hasName" )
		age_mapper = ColumnTripleMapper ( "age" )
		
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

		id_mapper = IdColumnValueMapper ( "id" )
		name_mapper = ColumnTripleMapper ( "name", "hasName" )
		age_mapper = ColumnTripleMapper ( "age" )
		col_mappers = [ name_mapper, age_mapper ]

		type_mapper = ConstantTripleMapper.for_type ( "Person" )
		source_mapper = ConstantTripleMapper ( "source", "TestDataset" )
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

		type_mapper = ConstantTripleMapper.for_type ( "encodes-protein" )

		from_mapper = RowTripleMapper.for_from (
			lambda row: f"ENSEMBL:{row['gene accession']}",
			[ "gene accession" ]
		)

		to_mapper = RowTripleMapper.for_to (
			lambda row: f"UNIPROT:{row['protein accession']}",
			[ "protein accession" ]
		)

		def pmid_extractor ( row ):
			# If it's optional, you've to handle missing values, return None to tell the upstream
			# layers to skip this triple.
			ref = row.get ( "reference", None )
			return f"PMID:{ref}" if ref else None

		pmid_mapper = RowTripleMapper.from_extractor (
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
		assert_that ( mapped_triples, "ValueMapper triples are as expected" )\
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
				ColumnTripleMapper.for_from ( column_id = "gene accession" ),
				ColumnTripleMapper.for_to ( column_id = "protein accession" )
			],
			const_prop_mappers = [ ConstantTripleMapper.for_type ( "encodes-protein" ) ]
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

@pytest.mark.skip ( "TODO: re-enable after TestSparkDataFrameMapper refactoring" )
@pytest.mark.integration
@pytest.mark.usefixtures ( "spark_session" )
class TestTabFileMapper:

	@pytest.mark.parametrize ( 
		ids = [ "without schema inference", "with schema inference" ],
		argnames = "is_infer_schema", 
		argvalues = [ False, True ], 
	)
	def	test_mapping_tsv ( self, spark_session, is_infer_schema: bool ):
		spark_data_type = IntegerType () if not is_infer_schema else None
		
		# This also shows how a configuration in a real case would look like.
		# A config file might define a constant like tb_mapper here and then this
		# might be imported by a Snakemake file and used together with the call
		# to map() below.
		#
		tb_mapper = TabFileMapper (
			id_mapper = IdColumnValueMapper ( column_id = "accession" ),    
			row_mappers = [
				ColumnTripleMapper ( column_id = "name", property = "hasGeneName" ),
				ColumnTripleMapper ( "accession", "hasAccession" ),
				ColumnTripleMapper ( "chromosome", "hasChromosomeId" ),
				ColumnTripleMapper ( "begin", "hasChromosomeBegin", spark_data_type = spark_data_type ),
				ColumnTripleMapper ( "end", "hasChromosomeEnd", spark_data_type = spark_data_type )
			],
			const_prop_mappers = [
				ConstantTripleMapper.for_type ( "Gene" ),
				ConstantTripleMapper ( property = "source", constant_value = "TestTSV" )
			],
			spark_options = { "inferSchema": is_infer_schema }
		)

		test_file_path = os.path.dirname ( os.path.abspath ( __file__ + "/../.." ) ) \
			+ "/resources/test-genes.tsv"		
		triples_df = tb_mapper.map ( spark_session, test_file_path )


    # Post-execution checks
		# 

		triples = triples_df.collect ()
		log.debug ( f"test_mapping_tsv(), mapped triples: {triples}" )

		assert_that ( triples_df, "We have a data frame back" ).is_instance_of ( DataFrame )
		assert_that ( triples_df.count (), "Number of mapped triples match" )\
			.is_equal_to ( 
				8 # total rows in the TSV
				* ( len ( tb_mapper.row_mappers ) 
			 			+ len ( tb_mapper.const_prop_mappers ) )
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
			# These must be integer even when inferSchema is True and we haven't set any type in the mappers
			"hasChromosomeBegin": "55019017",
			"hasChromosomeEnd": "55211628",
			GraphTriple.TYPE_KEY: "Gene",
			"source": '"TestTSV"'
		}.items ():			
			assert_that ( triples, f"Expected {test_id}.{key} in the result" )\
			.extracting ( 'id', 'key', 'value' )\
			.contains ( ( test_id, key, val ) )
	# /test_mapping_tsv


	def test_inconsistent_mappers_on_same_row ( self, spark_session ):
		tb_mapper = TabFileMapper (
			id_mapper = IdColumnValueMapper ( column_id = "accession" ),    
			row_mappers = [
				ColumnTripleMapper ( column_id = "name", property = "hasGeneName" ),
				ColumnTripleMapper ( "accession", "hasAccession" ),
				ColumnTripleMapper ( "chromosome", "hasChromosomeId" ),
				ColumnTripleMapper ( "begin", "hasChromosomeBegin", spark_data_type = IntegerType () ),
				ColumnTripleMapper ( "end", "hasChromosomeEnd", spark_data_type = IntegerType () ),
				# add the same column mapped to a different type, to cause an error
				ColumnTripleMapper ( "begin", "hasChromosomeBeginStr", spark_data_type = StringType () )
			],
			const_prop_mappers = [
				ConstantTripleMapper.for_type ( "Gene" ),
				ConstantTripleMapper ( property = "source", constant_value = "TestTSV" )
			],
			# spark_options = { "inferSchema": True } should be implicit
		)

		test_file_path = os.path.dirname ( os.path.abspath ( __file__ ) ) + "/resources/test-genes.tsv"
		# Should raise ValueError, let's check with assertpy
		assert_that ( 
			lambda: tb_mapper.map ( spark_session, test_file_path ), 
			"map() raises a ValueError over inconsistent mappers"
		).raises ( ValueError )
	# /test_inconsistent_mappers_on_same_row

# /TestTabFileMapper
