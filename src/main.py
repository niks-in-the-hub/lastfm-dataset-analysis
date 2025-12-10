import os
from utils import tsv_to_parquet, inspect_parquet_folder
from analysis import final_analysis


def main():
    """
    Main ETL + analysis pipeline for LastFM dataset.
    """

    # ---------------------------------------------------------
    # 1. Define input and output paths
    # ---------------------------------------------------------
    tsv_input_path = os.getenv("TSV_INPUT_PATH")
    parquet_output_path = os.getenv("PARQUET_OUTPUT_PATH", "/app/output_data/parquet")
    tsv_final_output = os.getenv("FINAL_TSV_OUTPUT", "/app/output_data/results")

    if not tsv_input_path:
        raise ValueError(
            "ERROR: Input file not found"
        )

    print(f"\n=== Running with configuration ===")
    print(f"Input TSV:       {tsv_input_path}")
    print(f"Parquet Output:  {parquet_output_path}")
    print(f"Final TSV Output:{tsv_final_output}\n")

    # ---------------------------------------------------------
    # 2. Convert TSV -> Parquet
    # ---------------------------------------------------------
    tsv_to_parquet(tsv_input_path, parquet_output_path)

    # ---------------------------------------------------------
    # 3. Load parquet dataset
    # ---------------------------------------------------------
    df, row_count, col_count = inspect_parquet_folder(parquet_output_path)

    # ---------------------------------------------------------
    # 4. Run final analysis
    # ---------------------------------------------------------
    print("\n===== Top 10 Songs (Final Analysis) =====")
    result_df = final_analysis(df, show_output=True)

    # ---------------------------------------------------------
    # 5. Save results as TSV
    # ---------------------------------------------------------
    (
        result_df.coalesce(1)
        .write
        .mode("overwrite")
        .option("delimiter", "\t")
        .option("header", "true")
        .csv(tsv_final_output)
    )

    print(f"\n Final results written to: {tsv_final_output}\n")


if __name__ == "__main__":
    main()