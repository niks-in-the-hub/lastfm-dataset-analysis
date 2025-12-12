from pyspark.sql.types import StructType
from pyspark.sql import DataFrame

def validate_schema(df: DataFrame, expected_schema: StructType) -> None:
    """
    Validates a Spark DataFrame against an expected schema.
    Performs:
      - Column name validation
      - Data type validation
      - Nullability validation
    Raises ValueError if any check fails.

    Parameters:-
    df: pyspark.sql.DataFrame
        The Spark DataFrame whose schema will be validated.
    expected_schema : pyspark.sql.types.StructType
        The schema the DataFrame is will be confirmed against.

    Returns:-
    None
        This function performs validation only and does not return a value.
    """

    # COLUMN CHECK
    def check_columns() -> None:
        """
        Ensure column names and their order match the expected schema.
        """
        actual_cols = [f.name for f in df.schema]
        expected_cols = [f.name for f in expected_schema]

        if actual_cols != expected_cols:
            raise ValueError(
                f"\n COLUMN MISMATCH:\n"
                f"Expected columns: {expected_cols}\n"
                f"Found columns:    {actual_cols}\n"
            )
        print(" Column check passed.")

    # TYPE CHECK
    def check_types() -> None:
        """
        Validate that each column has the expected Spark data type.

        Only the data type classes are compared (e.g., StringType,
        IntegerType), not metadata details.
        """
        actual_types = [type(f.dataType) for f in df.schema]
        expected_types = [type(f.dataType) for f in expected_schema]

        if actual_types != expected_types:
            raise ValueError(
                f"\n TYPE MISMATCH:\n"
                f"Expected: {[t.__name__ for t in expected_types]}\n"
                f"Found:    {[t.__name__ for t in actual_types]}\n"
            )
        print(" Type check passed.")

    # NULLABILITY CHECK
    def check_nullability() -> None:
        """
        Check whether each column's nullability matches the expectation.

        This ensures required (non-nullable) columns are enforced and
        optional columns are explicitly allowed to be null.
        """
        actual_nulls = [f.nullable for f in df.schema]
        expected_nulls = [f.nullable for f in expected_schema]

        if actual_nulls != expected_nulls:
            raise ValueError(
                f"\n NULLABILITY MISMATCH:\n"
                f"Expected: {expected_nulls}\n"
                f"Found:    {actual_nulls}\n"
            )
        print(" Nullability check passed.")

    # Run all validations
    check_columns()
    check_types()
    check_nullability()

    print("\n Schema validation PASSED!\n")
