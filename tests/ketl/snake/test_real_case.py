"""
Tests and end-to-end Snake workflow, which includes various mappers and Neo loading.
"""
import os
import pandas as pd
import pytest
import neo4j
from testcontainers.neo4j import Neo4jContainer

from assertpy import assert_that
from ketl.io.neoloader import NeoLoaderConfig

from conftest import run_snakefile

KETL_DATA_DIR_PATH = "/tmp/ketl"

MY_DIR = os.path.dirname ( os.path.abspath ( __file__ ) ) # tests/ketl/snake
PRJ_DIR = os.path.abspath ( MY_DIR + "/.." * 3 ) # <project_root>
TEST_DIR = PRJ_DIR + "/tests"


@pytest.mark.integration
def test_neo_nodes ( neo_driver: neo4j.Driver, genes_csv: pd.DataFrame ) -> None:
	n_expected_genes = genes_csv["ENSEMBL ID"].nunique ()
	n_expected_proteins = genes_csv["UniProt ID"].nunique ()
	n_expected_accessions = n_expected_genes + n_expected_proteins
	n_expected_nodes = n_expected_genes + n_expected_proteins + n_expected_accessions

	with neo_driver.session () as session:
		for label, n_exp in [
			( "Node", n_expected_nodes ),
			( "Gene", n_expected_genes ),
			( "Protein", n_expected_proteins ),
			( "Accession", n_expected_accessions )
		]:
			qry = f"MATCH (n:{label}) RETURN COUNT(n) AS n"
			result = session.run ( qry ).single ()
			n_nodes = result["n"]
			assert_that ( n_nodes, f"Number of {label} nodes in Neo4j is correct" )\
				.is_equal_to ( n_exp )


def test_neo_edges ( neo_driver: neo4j.Driver, genes_csv: pd.DataFrame ) -> None:
	# Equals the number of unique rows
	n_expected_encodes = genes_csv[ [ "ENSEMBL ID", "UniProt ID" ] ].drop_duplicates ().shape [ 0 ]
	n_expected_genes = genes_csv["ENSEMBL ID"].nunique ()
	n_expected_proteins = genes_csv["UniProt ID"].nunique ()

	with neo_driver.session () as session:
		for rel_type, from_label, to_label, n_exp in [
			( "encodesProtein", "Gene", "Protein", n_expected_encodes ),
			( "hasAccession", "Gene", "Accession", n_expected_genes ),
			( "hasAccession", "Protein", "Accession", n_expected_proteins )
		]:
			qry = f"MATCH (n:{from_label}) - [r:{rel_type}] -> (m:{to_label}) RETURN COUNT(r) AS n"
			result = session.run ( qry ).single ()
			n_rels = result["n"]
			assert_that ( n_rels, f"Number of {rel_type} relationships in Neo4j is correct" )\
				.is_equal_to ( n_exp )


@pytest.fixture ( scope = "module", autouse = True )
def run_snake ( neo4j_container: Neo4jContainer ) -> None:
	snakefile_path = f"{TEST_DIR}/resources/real_test_case/workflow.snakefile"
	env = { 
		"NEO4J_URL": neo4j_container.get_connection_url (),
		"NEO4J_USER": neo4j_container.username,
		"NEO4J_PASSWORD": neo4j_container.password
	}
	run_snakefile ( snakefile_path, ketl_data_dir_path = KETL_DATA_DIR_PATH, custom_env = env )

@pytest.fixture ( scope = "module" )
def genes_csv () -> pd.DataFrame:
	csv_path = f"{TEST_DIR}/resources/real_test_case/ensmbl-uniprot-genes.tsv"
	df = pd.read_csv ( csv_path, sep = "\t" )
	return df
