import os
from utils import tsv_to_parquet, inspect_parquet_folder
from analysis import final_analysis
import argparse
from argparse import Namespace


def parse_args() -> Namespace:
    """
    Parse command-line arguments for the local PySpark job.

    Defines and validates the required input and output paths provided when running the script from the command line.

    Returns:-
    argparse.Namespace
        Parsed command-line arguments containing:
        - input: path to the input TSV file
        - output: path to the output directory
    """
    parser = argparse.ArgumentParser(description="PySpark local job")
    
    parser.add_argument(
         "-i", "--input",
        required=True,
        help="Path to input tsv file "
    )

    parser.add_argument(
        "-o", "--output",
        required=True,
        default=os.getcwd(),
        help="Path to output folder "
    )

    return parser.parse_args()


def main() -> None:
    """
    Main ETL + analysis pipeline for LastFM dataset.

    This function orchestrates the entire workflow:
      1. Parse user-provided command-line arguments
      2. Convert the raw TSV input into Parquet format
      3. Load and inspect the Parquet dataset
      4. Run the final transformations and logic
      5. Write the results to a TSV file
    """

   #Initialise parser and define path arguments
    
    args = parse_args()

    tsv_input_path = args.input
    output_path = args.output

    # Parquet directory inside output folder
    parquet_path = os.path.join(output_path, "parquet")
    os.makedirs(parquet_path, exist_ok=True)

    result_dir = os.path.join(output_path, "final_results")
    os.makedirs(result_dir, exist_ok=True)

    if not os.path.exists(tsv_input_path):
        raise ValueError(
            "ERROR: Input file not found"
        )

    print(f"\n Running Analysis with the following user provided parameters")
    print(f"Input TSV: {tsv_input_path}\n")
    print(f"Parquet output path: {parquet_path}\n")
    print(f"Final CSV Output: {result_dir}\n")

    #Convert TSV -> Parquet
    tsv_to_parquet(tsv_input_path, parquet_path)

    #Load parquet dataset
    df = inspect_parquet_folder(parquet_path)

    #Run final analysis
    print("\n Top 10 Songs:")
    result_df = final_analysis(df, show_output=True)

    #Save results as TSV
    (
        result_df.coalesce(1)
        .write
        .mode("overwrite")
        .option("delimiter", "\t")
        .option("header", "true")
        .option("compression", "none")  # Disable compression to reduce memory usage
        .csv(result_dir)
    )

    print(f"\n Final results written to: {result_dir}\n")


if __name__ == "__main__":
    main()