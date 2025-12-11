import os
from utils import tsv_to_parquet, inspect_parquet_folder
from analysis import final_analysis
import argparse


def parse_args():
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


def main():
    """
    Main ETL + analysis pipeline for LastFM dataset.
    """

   #Initialise parser and define path arguments
    
    args = parse_args()

    tsv_input_path = args.input
    output_path = args.output

    # Parquet directory inside output folder
    parquet_path = os.path.join(output_path, "parquet")
    os.makedirs(parquet_path, exist_ok=True)

    if not os.path.exists(tsv_input_path):
        raise ValueError(
            "ERROR: Input file not found"
        )

    print(f"\n Running Analysis with the following user provided parameters")
    print(f"Input TSV: {tsv_input_path}\n")
    print(f"Parquet output path: {parquet_path}\n")
    print(f"Final CSV Output: {output_path}\n")

    #Convert TSV -> Parquet
    tsv_to_parquet(tsv_input_path, parquet_path)

    #Load parquet dataset
    df, *_ = inspect_parquet_folder(parquet_path)

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
        .csv(output_path)
    )

    print(f"\n Final results written to: {output_path}\n")


if __name__ == "__main__":
    main()