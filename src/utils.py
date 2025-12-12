from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from schema_validation import validate_schema


# Schema definition
LASTFM_SCHEMA = StructType([
    StructField("user_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("artist_id", StringType(), True),
    StructField("artist_name", StringType(), True),
    StructField("track_id", StringType(), True),
    StructField("track_name", StringType(), True),
])


# Central SparkSession builder
def create_spark(app_name: str = "LastFM ETL"):
    """
    Creates (or returns existing) SparkSession with consistent local configuration.

    Parameters:-
    app_name: str, optional
        Name of Spark application.

    Returns:-
    pyspark.sql.SparkSession
        An active SparkSession instance
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "8g")
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    # Reduce log noise
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# TSV to Parquet Conversion
def tsv_to_parquet(input_path: str, output_path: str):
    """
    Reads a TSV file using the predefined LASTFM_SCHEMA
    and writes it as a Parquet dataset.
    """
    spark = create_spark("TSV to Parquet Conversion")

    df = (
        spark.read
        .option("header", "false")
        .option("delimiter", "\t")
        .schema(LASTFM_SCHEMA)
        .csv(input_path)
    )

    # Schema Validation
    validate_schema(df, LASTFM_SCHEMA)

    df.write.mode("overwrite").parquet(output_path)

    print(f"TSV -> Parquet conversion complete!\n   Output folder: {output_path}")


# Inspect Parquet Folder
def inspect_parquet_folder(input_path: str):
    """
    Loads parquet files into a DataFrame,
    prints schema + sample rows,
    and returns (df, row_count, col_count).
    """
    spark = create_spark("Inspect Parquet Folder")

    print(f"\n Loading Parquet data from: {input_path}")

    df = spark.read.parquet(input_path)

    print("\n DATAFRAME SCHEMA ")
    df.printSchema()

    print("\n SAMPLE ROWS ")
    df.show(10, truncate=False)

    return df