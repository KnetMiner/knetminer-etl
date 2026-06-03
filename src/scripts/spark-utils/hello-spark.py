# A simple Spark app, which can be used to test if a Spark cluster is working.
# In a knetminer-etl installation, you can run this via:
#
# <Create and activate a virtualenv with this project installed>
# $ spark-submit --master spark://<master_host>:7077 $(which hello-spark.py)
# 
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def main ():
	size = 1000
	bin_size = 0.2
	with SparkSession.builder.appName ( "HelloSpark" ).getOrCreate() as spark:
    # Create a DF with the column 'value' with 'size' random values between 0 and 1.
		df = spark.range ( size ).select ( F.rand().alias ( "value" ) )
		# Create a histogram DF, with 'bin' and 'count' columns, using 'bin_size'
		# 'bin' column ranging in bin_size, bin_size*2, ... 1
		histogram_df = (
			df
			.withColumn ( 
				"bin", 
				F.round ( 
					(F.floor ( df["value"] / bin_size ) + 1) * bin_size,
					1
				)
			)
			.groupBy("bin")
			.count()
			.orderBy("bin")
		)

		print ( "\n\n|=== Result:" )
		histogram_df.show()
		print ( "\nDone!\n" )
    
if __name__ == "__main__":
    main()