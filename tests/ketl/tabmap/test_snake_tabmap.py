"""
Tests that the tabmap functions work correctly with SnakeMake. It uses a test Snakefile, which 
uses the most common features of the module.
"""

import json
import os
import subprocess

import pytest
from assertpy import assert_that
from deepdiff import DeepDiff

KETL_DATA_DIR_PATH = "/tmp/ketl"

@pytest.mark.integration
def test_snake_tabmap_nodes ():

	out_nodes_path = KETL_DATA_DIR_PATH + "/output/nodes-pg.json"
	test_entry = { 
		"type": "node",
		"id": "ENSMBL0008",
		"labels": ["Gene"], 
		"properties": { 
			"hasAccession": ["ENSMBL0008"], 
			"hasChromosomeBegin": [21967751]
		}
	}
	check_tabmap_out_file ( out_nodes_path, test_entry, expected_size = 15, type_str = "node" )
	

@pytest.mark.integration
def test_snake_tabmap_edges ():
	out_edges_path = KETL_DATA_DIR_PATH + "/output/edges-pg.json"

	test_entry = { 
		"type": "edge",
		"id": "encodes-protein_ENSMBL0003_QA03",
		"labels": ["encodes-protein"],
		"from": "ENSMBL0003", 
		"to": "QA03",
		"properties": {
			"link notes": ["text mining"],
			"source": ["SnakeTest"]
		},
	}

	check_tabmap_out_file ( out_edges_path, test_entry, expected_size = 8, type_str = "edge" )


@pytest.fixture ( scope = "module", autouse = True )
def run_base_tabmap_test_file () -> None:
	run_snakefile ( "tabmap-test.snakefile" )


def run_snakefile ( snakefile_path: str, snake_target: str = "all" ) -> None:
	"""
	Helper to run our SnakeMake test files

	`snakefile_path` is the path to the Snakefile to run. If it's not absolute, it's rooted into
	<project_root>/tests/resources. 
	"""

	my_dir = os.path.dirname ( os.path.abspath ( __file__ ) )
	prj_dir = os.path.abspath ( my_dir + "/.." * 3 )
	test_dir = prj_dir + "/tests"
	
	if not snakefile_path.startswith ( "/" ):
		snakefile_path = f"{test_dir}/resources/{snakefile_path}"

	os.chdir ( prj_dir )

	
	# Clean the output dir, if it exists
	if os.path.exists ( KETL_DATA_DIR_PATH ):
		subprocess.run ( [ "rm", "-rf", KETL_DATA_DIR_PATH ], check = True )

	subprocess.run ( 
		[ "snakemake", "-s", snakefile_path, "--cores", "all", snake_target ], 
		check = True,
		env = { **os.environ, "KETL_DATA": KETL_DATA_DIR_PATH, "PYTHONPATH": "tests" }
	)


def check_tabmap_out_file ( pg_jsonl_path: str, expected_entry: dict[str, any], expected_size: int,type_str: str ) -> None:
	"""
	Checks the output of a node/edge PG JSONL file
	"""
	def check_entry ( js_line: dict[str, any] ) -> bool:
		"""
		Checks that js_line contains expected_entry.
		"""
		diff = DeepDiff ( js_line, expected_entry, ignore_order = True, report_repetition = True )
		# It's OK if the DeepDiff report is empty (operands are identical) or contains dictionary_item_removed only
		return not diff or "dictionary_item_removed" in diff and len ( diff ) == 1

	with open ( pg_jsonl_path, "r" ) as f:
		lines = f.readlines ()
		assert_that ( len ( lines ), f"The {type_str} output has the right size" )\
			.is_equal_to ( expected_size )

	with open ( pg_jsonl_path, "r" ) as f:
		lines = map ( lambda line: json.loads ( line ), f.readlines () )
		is_ok = any ( line for line in lines if check_entry ( line ) )
		assert_that ( is_ok, f"The {type_str} output has the expected content" )\
			.is_true ()
