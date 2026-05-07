
"""
Reproduces a real mapping file in the old Ondex-based pipeline.

(`/home/data/knetminer/pub/config/arabidopsis_thaliana/gene-protein-ensembl.xml`)

This maps ENSEMBL IDs from the input TSV to gene nodes, enriching the result with constants
like the source.
"""

from ketl.core import ConstantTripleMapper
from ketl.tabmap.core import ColumnTripleMapper, ColumnValueMapper, RowTripleMapper, RowValueMapper, TabFileMapper, SparkDataFrameMapper
from ketl.io.neoloader import NeoLoaderConfig, NeoLoaderPropertyConfig

import ketl.helpers as khelpers
import ketl.tabmap.helpers as tbhelpers


def make_accession_mappers_for_source ( 
	source_id: str, acc_col_id: str, source_id_mapper: RowValueMapper 
) -> tuple[TabFileMapper, TabFileMapper]:
	"""
	Helper to make a pair of :class:`TabFileMapper`(s), one that maps an accession column and a known 
	constant accession source to an Accession node, the other that maps to the relationship linking
	the owner node to the accession.

	## Parameters
	- `source_id`: the accession source ID, eg, "ENSEMBL"
	- `acc_col_id`: the column ID where to take the accession value from
	- `source_id_mapper`: the ID mapper of the source node, to be used to create relationships that 
	  link the accession nodes to their respective source nodes

	TODO: move it to a utils module.

	"""
	acc_mapper = TabFileMapper (
		id_mapper = tbhelpers.row_value_mapper ( 
			lambda row: f"accession:{source_id}:{row[acc_col_id]}"
		),
		mapper_components = [
			ColumnTripleMapper ( column_id = acc_col_id, property = "value" ),
			khelpers.type_triple_mapper ( "Accession" ),
			ConstantTripleMapper ( property = "source", constant_value = source_id )
		]
	)

	rel_mapper = TabFileMapper (
		id_mapper = SparkDataFrameMapper.AutoEdgeId (),
		mapper_components = [
			# TODO: check AgriSchemas
			khelpers.type_triple_mapper ( "hasAccession" ),
			tbhelpers.edge_source_row_triple_mapper ( source_id_mapper ),
			tbhelpers.edge_target_row_triple_mapper ( acc_mapper.id_mapper )
		]
	)

	return acc_mapper, rel_mapper


# E2U = ENSEMBL to UniProt file. 

E2U_ENSEMBL_GENE_MAPPER = TabFileMapper (
	# The node ID is usually a prefix + the accession for this type. The prefix is often needed
	# because the same accessions are used for multiple related types, eg, genes and proteins.
	# The fluent chain here is a typical way to add a prefix to a value mapper.
	id_mapper = ColumnValueMapper ( column_id = "ENSEMBL ID" )\
		.with_value_wrapper ( khelpers.string_value_wrapper ( prefix = "gene:" ) ),
	mapper_components = [ 
		khelpers.type_triple_mapper ( "Gene" ),
		ConstantTripleMapper ( property = "dataSources", constant_value = "ENSEMBL-Plants" ),
	]
)

E2U_ENSEMBL_GENE_ACCESSION_MAPPERS = make_accession_mappers_for_source ( 
	source_id = "ENSEMBL-Plants",
	acc_col_id = "ENSEMBL ID", 
	source_id_mapper = E2U_ENSEMBL_GENE_MAPPER.id_mapper
)


E2U_ENSEMBL_PROTEIN_MAPPER = TabFileMapper (
	# The node ID is usually a prefix + the accession for this type. The prefix is often needed
	# because the same accessions are used for multiple related types, eg, genes and proteins.
	id_mapper = ColumnValueMapper ( column_id = "UniProt ID" )\
		.with_value_wrapper ( khelpers.string_value_wrapper ( prefix = "protein:" ) ),
	mapper_components = [ 
		khelpers.type_triple_mapper ( "Protein" ),
		ConstantTripleMapper ( property = "dataSources", constant_value = "ENSEMBL-Plants" ),
		ConstantTripleMapper ( property = "dataSources", constant_value = "TAIR" )
	]
)
"""
This maps UniProt IDs, coming from the same file that :class:`E2U_ENSEMBL_GENE_MAPPER` uses (see it).
"""

E2U_UNIPROT_ACCESSION_MAPPERS = make_accession_mappers_for_source ( 
	source_id = "UniProt", 
	acc_col_id = "UniProt ID", 
	source_id_mapper = E2U_ENSEMBL_PROTEIN_MAPPER.id_mapper
)

E2U_TAIR_PROTEIN_ACCESSION_MAPPERS = make_accession_mappers_for_source ( 
	source_id = "TAIR", 
	acc_col_id = "UniProt ID", 
	source_id_mapper = E2U_ENSEMBL_PROTEIN_MAPPER.id_mapper
)


E2U_GENE2PROTEIN_MAPPER = TabFileMapper (
	id_mapper = SparkDataFrameMapper.AutoEdgeId (),
	mapper_components = [
		# endpoint ID mappers can often be reused
		tbhelpers.edge_source_row_triple_mapper ( E2U_ENSEMBL_GENE_MAPPER.id_mapper ),
		tbhelpers.edge_target_row_triple_mapper ( E2U_ENSEMBL_PROTEIN_MAPPER.id_mapper ),
		khelpers.type_triple_mapper ( "encodesProtein" ),
		ConstantTripleMapper ( property = "dataSources", constant_value = "ENSEMBL Plants" ),
		ConstantTripleMapper ( property = "dataSources", constant_value = "TAIR" )
	]
)
"""
This maps the gene-protein relationships, coming from the same file that 
:class:`E2U_ENSEMBL_GENE_MAPPER` and :class:`E2U_ENSEMBL_PROTEIN_MAPPER` use (see them).
"""


NEO_LOADER_CONFIG = NeoLoaderConfig ( 
	property_configs = {
		# In general, an entity can have multiple sources, so, here it is
		"dataSources": NeoLoaderPropertyConfig ( multi_value_mode = "multiple" )
	}
)
"""
The config for the NeoLoader mainly deals with shaping PG properties
"""
