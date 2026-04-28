
from ketl.core import ConstantPropertyMapper, GraphTriple
from ketl.tabmap.core import ColumnMapper, IdColumnMapper, TabFileMapper, SparkDataFrameMapper
from ketl.io.neoloader import NeoLoaderConfig, NeoLoaderPropertyConfig


def make_accession_mappers_for_source ( 
	source_id: str, acc_col_id: str, source_id_mapper: IdColumnMapper 
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
		id_mapper = IdColumnMapper.from_extractor ( 
			extractor = lambda row: f"accession:{source_id}:{row[acc_col_id]}",
			column_ids = [ acc_col_id ]
		),
		row_mappers = [
			ColumnMapper ( column_id = acc_col_id, property = "value" ),
		],
		const_prop_mappers = [
			ConstantPropertyMapper.for_type ( "Accession" ),
			ConstantPropertyMapper ( property = "source", constant_value = source_id )
		]
	)

	rel_mapper = TabFileMapper (
		id_mapper = SparkDataFrameMapper.AutoEdgeId ( "has-accession" ),
		const_prop_mappers = [
			# TODO: check AgriSchemas
			ConstantPropertyMapper.for_type ( "has-accession" ),
		],
		row_mappers = [
			ColumnMapper.from_extractor ( source_id_mapper.value, GraphTriple.FROM_KEY, source_id_mapper.column_ids ),
			ColumnMapper.from_extractor ( acc_mapper.id_mapper.value, GraphTriple.TO_KEY, acc_mapper.id_mapper.column_ids )
		]
	)

	return acc_mapper, rel_mapper


# E2U = ENSEMBL to UniProt file. 

E2U_ENSEMBL_GENE_MAPPER = TabFileMapper (
	# The node ID is usually a prefix + the accession for this type. The prefix is often needed
	# because the same accessions are used for multiple related types, eg, genes and proteins.
	id_mapper = IdColumnMapper.from_extractor ( 
		extractor = lambda row: f"gene:{row['ENSEMBL ID']}",
		column_ids = [ "ENSEMBL ID" ]
	),
	const_prop_mappers = [ 
		ConstantPropertyMapper.for_type ( "Gene" ),
		ConstantPropertyMapper ( property = "dataSources", constant_value = "ENSEMBL-Plants" ),
	]
)
"""
Reproduces a real mapping file in the old Ondex-based pipeline.

(`/home/data/knetminer/pub/config/arabidopsis_thaliana/gene-protein-ensembl.xml`)

This maps ENSEMBL IDs from the input TSV to gene nodes, enriching the result with constants
like the source.
"""

E2U_ENSEMBL_GENE_ACCESSION_MAPPERS = make_accession_mappers_for_source ( 
	source_id = "ENSEMBL-Plants",
	acc_col_id = "ENSEMBL ID", 
	source_id_mapper = E2U_ENSEMBL_GENE_MAPPER.id_mapper
)


E2U_ENSEMBL_PROTEIN_MAPPER = TabFileMapper (
	# The node ID is usually a prefix + the accession for this type. The prefix is often needed
	# because the same accessions are used for multiple related types, eg, genes and proteins.
	id_mapper = IdColumnMapper.from_extractor (
		extractor = lambda row: f"protein:{row['UniProt ID']}",
		column_ids = [ "UniProt ID" ]
	),
	const_prop_mappers = [ 
		ConstantPropertyMapper.for_type ( "Protein" ),
		ConstantPropertyMapper ( property = "dataSources", constant_value = "ENSEMBL-Plants" ),
		ConstantPropertyMapper ( property = "dataSources", constant_value = "TAIR" )
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
	# Overrides the auto-generated ID, to avoid 'gene:' and 'protein:' redundancy placed here
	id_mapper = ColumnMapper.from_extractor ( 
		extractor = lambda row: f"encodes-protein-{row['ENSEMBL ID']}-{row['UniProt ID']}",
		property = GraphTriple.ID_KEY,
		column_ids = [ "ENSEMBL ID", "UniProt ID" ]
	),
	row_mappers = [
		# endpoint ID mappers can often be reused
		ColumnMapper.from_extractor ( E2U_ENSEMBL_GENE_MAPPER.id_mapper.value, GraphTriple.FROM_KEY, E2U_ENSEMBL_GENE_MAPPER.id_mapper.column_ids ),
		ColumnMapper.from_extractor ( E2U_ENSEMBL_PROTEIN_MAPPER.id_mapper.value, GraphTriple.TO_KEY, E2U_ENSEMBL_PROTEIN_MAPPER.id_mapper.column_ids ),
	],
	const_prop_mappers = [ 
		ConstantPropertyMapper.for_type ( "encodes-protein" ),
		ConstantPropertyMapper ( property = "dataSources", constant_value = "ENSEMBL Plants" ),
		ConstantPropertyMapper ( property = "dataSources", constant_value = "TAIR" )
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