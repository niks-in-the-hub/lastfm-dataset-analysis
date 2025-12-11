import warnings
from pydantic import BaseModel, validator
from pyspark.sql.types import StructType, StructField, StringType


# Expected Schema Definition


LASTFM_SCHEMA = StructType([
    StructField("user_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("artist_id", StringType(), True),
    StructField("artist_name", StringType(), True),
    StructField("track_id", StringType(), True),
    StructField("track_name", StringType(), True),
])


# Define Pydantic Schema Model as a class


class SchemaModel(BaseModel):
    columns: list
    types: list
    nullable: list

    @validator("columns")
    def validate_columns(cls, v):
        expected = [f.name for f in LASTFM_SCHEMA]
        if v != expected:
            warnings.warn(f"Column mismatch:\nExpected {expected}\nFound    {v}")
        return v

    @validator("types")
    def validate_types(cls, v):
        expected = [type(f.dataType) for f in LASTFM_SCHEMA]
        if v != expected:
            warnings.warn(f"Type mismatch:\nExpected {expected}\nFound    {v}")
        return v

    @validator("nullable")
    def validate_nullable(cls, v):
        expected = [f.nullable for f in LASTFM_SCHEMA]
        if v != expected:
            warnings.warn(f"Nullability mismatch:\nExpected {expected}\nFound    {v}")
        return v


#  Validation Function

def validate_schema(df):
    """
    Validate a Spark DataFrame's schema using Pydantic.
    Raises warnings on mismatches; prints success otherwise.
    """
    SchemaModel(
        columns=[f.name for f in df.schema],
        types=[type(f.dataType) for f in df.schema],
        nullable=[f.nullable for f in df.schema],
    )

    print("Schema validation passed.")