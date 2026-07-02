import logging

from assertpy import assert_that
from pyspark.sql.types import IntegerType, StringType

import ketl.helpers as khelpers
from ketl.core import (ConstantTripleMapper, GraphProperty, GraphTriple,
                       JSONBasedValueConverter, SparkDataFrameTypes)

log = logging.getLogger ( __name__ )

class TestGraphProperty:
	def test_basics ( self ):
		# As said in the docstring, values will be stored with their JSON representations.
		# While this is not a GraphProperty requirement, we write tests with this convention, 
		# for sake of clarity.
		attrs = { "key": "name", "value": '"Gene A001"' }
		n = GraphProperty ( *attrs.values () )
		for a, v in attrs.items ():
			# self.assertEqual ( eval ( f"n.{a}" ), v, f"Attribute {a} mismatch" )
			assert_that ( eval ( f"n.{a}" ), f"The GraphProperty '{a}' has the expected value" )\
				.is_equal_to ( v )
# /TestGraphProperty


class TestGraphTriple:
	def test_basics ( self ):
		attrs = { GraphTriple.ID_KEY: "A001", "key": "name", "value": "Gene A001" }
		n = GraphTriple ( *attrs.values () )
		for a, v in attrs.items ():
			assert_that ( eval ( f"n.{a}" ), f"The GraphTriple '{a}' has the expected value" ).is_equal_to ( v )
# /TestGraphTriple

class TestValueConverters:
	def test_string_conversion ( self ):
		converter = JSONBasedValueConverter()
		v = "test"
		exp_ser_v = '"' + v + '"'

		assert_that ( converter.serialize ( v ), "String is serialized as expected" ).is_equal_to ( exp_ser_v )
		assert_that ( converter.unserialize ( exp_ser_v ), "String is unserialized as expected" ).is_equal_to ( v )
		assert_that ( type ( converter.unserialize ( exp_ser_v ) ), "Unserialized value is a string" ).is_equal_to ( str )

	def test_int_conversion ( self ):
		converter = JSONBasedValueConverter()
		v = 123
		exp_ser_v = str ( v )
		ser_v = converter.serialize ( v )
		assert_that ( ser_v, "Int is serialized as expected" ).is_equal_to ( exp_ser_v )
		assert_that ( converter.unserialize ( ser_v ), "Int is unserialized as expected" ).is_equal_to ( v )
	
	def test_float_conversion ( self ):
		converter = JSONBasedValueConverter()
		v = 12.34
		exp_ser_v = str ( v )
		ser_v = converter.serialize ( v )
		assert_that ( ser_v, "Float is serialized as expected" ).is_equal_to ( exp_ser_v )
		assert_that ( converter.unserialize ( ser_v ), "Float is unserialized as expected" ).is_equal_to ( v )

	def test_empty_string_conversion_with_default_converter ( self ):
		converter = JSONBasedValueConverter()
		v = ""
		sv = converter.serialize ( v )
		assert_that ( sv, "Empty string serializes to None" ).is_none ()

	def test_none_conversion ( self ):
		converter = JSONBasedValueConverter()
		assert_that ( converter.serialize ( None ), "None serializes to None" ).is_none ()
		assert_that ( converter.unserialize ( None ), "None unserializes to None" ).is_none ()
# /TestValueConverters

class TestConstantPropertyMapper:
	def test_basics ( self ):
		const_prop = "hasHello"
		const_value = "Hello, World!"
		const_mapper = ConstantTripleMapper ( const_prop, const_value )

		triple_id = "N001"
		triple = const_mapper.triple ( triple_id )

		assert_that ( triple, "ConstPropertyMapper.triple() returns a GraphTriple" ).is_instance_of ( GraphTriple )
		assert_that ( triple.id, "Triple ID is as expected" ).is_equal_to ( triple_id )
		assert_that ( triple.key, "Triple key is as expected" ).is_equal_to ( const_prop )
		assert_that ( triple.value, "Triple value is as expected" ).is_equal_to ( const_value )
	
	def test_none_value ( self ):
		"""TODO: Does this edge case make sense?"""
		const_prop = "hasNothing"
		const_value = None
		const_mapper = ConstantTripleMapper ( const_prop, const_value )

		triple_id = "N001"
		triple = const_mapper.triple ( triple_id )

		assert_that ( triple, "ConstPropertyMapper.triple() returns None for None value" ).is_none ()

	def test_empty_string_value ( self ):
		const_prop = "hasEmptyString"
		const_value = ""
		const_mapper = ConstantTripleMapper ( const_prop, const_value )

		triple_id = "N001"
		triple = const_mapper.triple ( triple_id, converter = JSONBasedValueConverter() )

		assert_that ( 
			triple,
			"ConstPropertyMapper.triple() returns None for empty strings that are serialised to None"
		).is_none ()

	def test_type_triple_mapper ( self ):
		type_label = "TestType"
		const_mapper = khelpers.type_triple_mapper ( type_label )

		triple_id = "N001"
		triple = const_mapper.triple ( triple_id )

		assert_that ( triple, "ConstPropertyMapper.for_type().triple() returns a GraphTriple" ).is_instance_of ( GraphTriple )
		assert_that ( triple.id, "Triple ID is as expected" ).is_equal_to ( triple_id )
		assert_that ( triple.key, "Triple key is as expected" ).is_equal_to ( GraphTriple.TYPE_KEY )
		assert_that ( triple.value, "Triple value is as expected" ).is_equal_to ( type_label )

	def test_type_triple_mapper_cache ( self ):
		type_label = "TestType"
		const_mapper1 = khelpers.type_triple_mapper ( type_label )
		const_mapper2 = khelpers.type_triple_mapper ( type_label )

		assert_that ( const_mapper1, "type_triple_mapper(), the caching works" ).is_equal_to ( const_mapper2 )
# /TestConstantPropertyMapper


class TestSparkDataFrameTypes:
	def test_cast_df ( self, spark_session ):

		test_data = [ ("1", "Alice"), ("2", "Bob") ]
		df = spark_session.createDataFrame ( test_data, [ "id", "name" ] )

		spark_types = SparkDataFrameTypes ( 
			column_specs = { 
				"id": SparkDataFrameTypes.ColumnSpec ( IntegerType () ),
				# Ignored when te DF doesn't have it
				"foo": SparkDataFrameTypes.ColumnSpec ( StringType () ) 
			}
		)
		casted_df = spark_types.cast_df ( df )

		assert_that ( casted_df.schema [ "id" ].dataType, "Column 'id' is casted to IntegerType" ).is_instance_of ( IntegerType )
		assert_that ( casted_df.schema [ "name" ].dataType, "Column 'name' is unchanged and is StringType" ).is_instance_of ( StringType )

		# Check the values too
		expected_rows = [ (int ( id ), name ) for id, name in test_data ]
		casted_rows = casted_df.collect ()
		assert_that ( casted_rows, "Casted DataFrame has the expected rows" )\
			.is_equal_to ( expected_rows )
