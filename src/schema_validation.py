from pyspark.sql.types import StructType

def validate_schema(df, expected_schema: StructType):
    """
    Validates a Spark DataFrame against an expected schema.
    Performs:
      - Column name validation
      - Data type validation
      - Nullability validation
    Raises ValueError if any check fails.
    """

    # COLUMN CHECK
    def check_columns():
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
    def check_types():
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
    def check_nullability():
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