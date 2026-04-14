from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

def run_silver():
    warehouse_path = os.path.abspath("data/warehouse")
    
    spark = SparkSession.builder \
        .appName("SilverIcebergStep") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", warehouse_path) \
        .config("spark.sql.defaultCatalog", "local") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
    
    bronze_dir = "data/bronze/airports"
    if not os.path.exists(bronze_dir) or not os.listdir(bronze_dir):
        print("Bronze directory not found or empty. Please run the consumer_bronze.py and producer.py first.")
        return
        
    print(f"Reading Parquet from Bronze Layer...")
    # Load all parquet files in the folder
    df_bronze = spark.read.parquet(bronze_dir)
    
    print("Original Schema in Bronze layer:")
    df_bronze.printSchema()
    
    print("Transforming to Silver: Casting types and handling deduplication...")
    # Transformations: Cast String types to numeric types, deduplicate and filter invalid rows
    df_silver = df_bronze \
        .withColumn("airport_id", col("airport_id").cast("integer")) \
        .withColumn("latitude", col("latitude").cast("double")) \
        .withColumn("longitude", col("longitude").cast("double")) \
        .withColumn("altitude", col("altitude").cast("double")) \
        .filter(col("airport_id").isNotNull()) \
        .dropDuplicates(["airport_id"])
        
    print("Writing to Silver table via Iceberg format...")
    # Ensure namespaces exist
    spark.sql("CREATE NAMESPACE IF NOT EXISTS silver")
    
    # Save the dataframe as an iceberg table
    df_silver.write \
        .format("iceberg") \
        .mode("overwrite") \
        .saveAsTable("local.silver.airports")
        
    print("Silver table 'local.silver.airports' updated successfully!")
    print(f"Total valid airports processed: {df_silver.count()}")
    
    spark.stop()

if __name__ == "__main__":
    run_silver()
