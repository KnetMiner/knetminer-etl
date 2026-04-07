import logging

from assertpy import assert_that

from ketl.core import (ConstantPropertyMapper, GraphProperty, GraphTriple,
                       JSONBasedValueConverter)

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

	def test_pre_serializers ( self ):
		converter = JSONBasedValueConverter()
		default = "<NA>"
		# This is to test the addition method. You can set it straight with:
		#
		# converter.pre_serializer = ...
		# 
		# If you use `add_pre_serializers()`, you'll typically add to the default pre-serializer that the
		# converter sets. Usually, this is a function that turns any falsy value into `None`, or returns
		# the value unchanged. 
		#
		# The pre-serialisers chain might still land a None to your string, so you have do your own
		# null handing, so this has to be considered in your serialisers.
		# 
		# A further option is doing base filtering with Spark DataFrame transformations.
		#
		converter.add_pre_serializers ( lambda s: s or default ) # We might get None here
		converter.add_pre_serializers ( lambda s: s.upper () )

		s = "Hello"
		sv = converter.serialize ( s )
		assert_that ( sv, "Pre-serializer-equipped converter works on regular string" ).is_equal_to ( '"' + s.upper () + '"' )

		assert_that ( converter.serialize ( "" ), "Pre-serializer-equipped converter converts empty string to default" ).is_equal_to ( '"' + default + '"' )
		assert_that ( converter.serialize ( None ), "Pre-serializer-equipped converter converts None to default" ).is_equal_to ( '"' + default + '"' )
# /TestValueConverters

class TestConstantPropertyMapper:
	def test_basics ( self ):
		const_prop = "hasHello"
		const_value = "Hello, World!"
		const_mapper = ConstantPropertyMapper ( const_prop, const_value )

		triple_id = "N001"
		triple = const_mapper.triple ( triple_id )

		assert_that ( triple, "ConstPropertyMapper.triple() returns a GraphTriple" ).is_instance_of ( GraphTriple )
		assert_that ( triple.id, "Triple ID is as expected" ).is_equal_to ( triple_id )
		assert_that ( triple.key, "Triple key is as expected" ).is_equal_to ( const_prop )
		assert_that ( triple.value, "Triple value is as expected" ).is_equal_to ( f'"{const_value}"' )
	
	def test_none_value ( self ):
		"""TODO: Does this edge case make sense?"""
		const_prop = "hasNothing"
		const_value = None
		const_mapper = ConstantPropertyMapper ( const_prop, const_value )

		triple_id = "N001"
		triple = const_mapper.triple ( triple_id )

		assert_that ( triple, "ConstPropertyMapper.triple() returns None for None value" ).is_none ()

	def test_empty_string_value ( self ):
		const_prop = "hasEmptyString"
		const_value = ""
		const_mapper = ConstantPropertyMapper ( const_prop, const_value )

		triple_id = "N001"
		triple = const_mapper.triple ( triple_id )

		assert_that ( triple, "ConstPropertyMapper.triple() returns None for empty strings" ).is_none ()

	def test_for_type_helper ( self ):
		type_label = "TestType"
		const_mapper = ConstantPropertyMapper.for_type ( type_label )

		triple_id = "N001"
		triple = const_mapper.triple ( triple_id )

		assert_that ( triple, "ConstPropertyMapper.for_type().triple() returns a GraphTriple" ).is_instance_of ( GraphTriple )
		assert_that ( triple.id, "Triple ID is as expected" ).is_equal_to ( triple_id )
		assert_that ( triple.key, "Triple key is as expected" ).is_equal_to ( GraphTriple.TYPE_KEY )
		assert_that ( triple.value, "Triple value is as expected" ).is_equal_to ( type_label )
# /TestConstantPropertyMapper
