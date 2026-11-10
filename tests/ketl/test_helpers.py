from assertpy import assert_that
import pandas as pd
import pytest

from ketl.core import GraphTriple, ValueConverter
import ketl.helpers as khelpers

def test_pd_triples_by_id ():
	test_triples_pd_df = pd.DataFrame ( [
		{ "id": "A", "key": "k1", "value": "v1" },
		{ "id": "A", "key": "k1", "value": "v2" },
		{ "id": "A", "key": "k2", "value": 1 },
		{ "id": "A", "key": "k2", "value": 2 },
		{ "id": "B", "key": "k1", "value": 3 },
	] )
	a_triples_pd_df = khelpers.pd_triples_by_id ( test_triples_pd_df, id = "A" )
	assert_that ( set ( a_triples_pd_df [ "id" ] ) ).is_equal_to ( { "A" } )
	assert_that ( set ( a_triples_pd_df [ "key" ] ) ).is_equal_to ( { "k1", "k2" } )

@pytest.mark.parametrize ( "case", [ "base", "no_unserialize", "with_duplicates" ] )
def test_pd_triples_value ( case: str ):
	"""
	Tests `pd_triples_value()`.
	"""
	test_triples_pd_df = pd.DataFrame ( [
		{ "id": "A", "key": "k1", "value": "\"v1\"" },
		{ "id": "A", "key": "k1", "value": "\"v2\"" },
		{ "id": "A", "key": "k2", "value": "1" },
		{ "id": "A", "key": "k2", "value": "2" },
		{ "id": "A", "key": "k2", "value": "2" }
	] )

	key = "k1"
	if case == "with_duplicates": key = "k2"

	result = khelpers.pd_triples_value ( 
		test_triples_pd_df, key = key, 
		do_unserialize = ( case != "no_unserialize" ),
		do_drop_duplicates = ( case != "with_duplicates" ) 
	)

	expected = test_triples_pd_df [ test_triples_pd_df [ "key" ] == key ] [ "value" ]
	if case != "no_unserialize":
		expected = expected.apply ( ValueConverter.get_default ().unserialize )
	if case != "with_duplicates":
		expected = expected.drop_duplicates ()

	assert_that ( set ( result ), f"Case {case} works" )\
		.is_equal_to ( set ( expected.to_list () ) )
