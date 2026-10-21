from typing import Sequence
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def df_join_filter ( 
	target_df: DataFrame, reference_df: DataFrame, join_cols_or_expr
) -> DataFrame:
	"""
	Filters the target dataframe `target_df`, keeping the rows that have an existing reference in the reference
	dataframe `reference_df`.

	The references are checked through the `join_cols_or_expr` parameter, which can be either a pair of column names
	(ie, the target DF column that needs to match rows in the reference DF column), a tuple/list with a single column
	(meaning target and ref columns for joining are the same), or a string containing an SQL expression to establish 
	the joining condition. The expression must fit a `WHERE EXISTS` clause, where the aliases
	`target` and `ref` are used, eg, `"target.gene_id = ref.gene_id OR target.gene_name = ref.gene_name"`.

	Note that conditions using OR or non-equality comparisons can't be executed as hash joins, Spark falls back
	to nested-loop joins, which can be slow when both dataframes are big.

  ## Returns 
	A new `DataFrame` which represents `target_df` minus the rows without references in `reference_df`.
	The operation is lazy, nothing really happens until the target is executed/consumed.
	"""

	if not isinstance ( join_cols_or_expr, str ):
		if not isinstance ( join_cols_or_expr, Sequence ):
			raise ValueError ( f"Wrong type {type(join_cols_or_expr)} for join_cols_or_expr" )
		# Build the expression using these
		if len ( join_cols_or_expr ) < 1:
			raise ValueError ( "At least one column needs to be specified for joining" )
		target_col = join_cols_or_expr [ 0 ]
		ref_col = join_cols_or_expr [ 1 ] if len ( join_cols_or_expr ) == 2 else target_col
		# left_semi == WHERE EXISTS: keeps target_df's rows/columns only, no duplicates
		join_cols_or_expr = f"target.{target_col} = ref.{ref_col}"
		
	# left_semi == WHERE EXISTS: keeps target_df's rows/columns only, no duplicates
	return target_df.alias ( "target" ).join (
		reference_df.alias ( "ref" ), F.expr ( join_cols_or_expr ), how = "left_semi"
	)
