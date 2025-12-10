# lastfm-dataset-analysis

# lastfm-dataset-analysis
LastFM Listening Behavior Analysis (PySpark + Docker)

This project analyzes the LastFM 1K user listening dataset using PySpark, entirely inside a Docker container.
There is no need to install any dependencies on your machine, Docker handles everything.

The workflow:

1. Convert a large TSV dataset (3GB+) into Parquet

2. Detect user listening sessions based on 20-minute inactivity gaps

3. Identify the top 50 sessions with the most tracks played

4. Compute the Top 10 most listened-to tracks from those sessions

5. Write the final output as a TSV file

Because the dataset is too large for GitHub, users provide their own local TSV file.

Project Structure

case_study/
│
├── src/
│   ├── main.py              # Entry point: orchestrates the entire pipeline
│   ├── utils.py             # TSV → Parquet conversion + Parquet inspection
│   ├── analysis.py          # Sessionization + Top 10 songs logic
│   └── __init__.py
│
├── Dockerfile               # Builds a full PySpark environment
├── requirements.txt
├── output_data/             # Output folder (created locally on your machine)
└── README.md

Note: The actual dataset (TSV, ~2.5GB) is not included in the repo.
You can mount your own file into the Docker container at runtime (instruction given below)

1. Install Docker

You must have Docker Desktop (macOS/Windows) or Docker Engine (Linux) installed.

macOS: https://docs.docker.com/desktop/mac/install

Windows: https://docs.docker.com/desktop/windows/install

Linux: https://docs.docker.com/engine/install

Once installed, verify:

docker --version

2. Clone the Repository
git clone https://github.com/<your-username>/<your-repo>.git
cd <your-repo>

3. Build the Docker Image

Run this from the root of the repo:

docker build -t case-study-spark .


This image contains:

Python 3.10

Java 17 (Temurin)

PySpark

The analysis scripts

4. Prepare Your Dataset

You must have the full TSV dataset stored locally.

Expected format:
Each row must contain:

user_id  timestamp  artist_id  artist_name  track_id  track_name


The file is tab-separated and usually named something like:

userid-timestamp-artid-artname-traid-traname.tsv


This file is not uploaded to GitHub because it's too large.
Instead, you mount it into the container when running the analysis.

5. Run the Pipeline Using Docker

Below are OS-specific examples for running the project.

On macOS or Linux (Bash terminal)
docker run --rm \
  -v /absolute/path/to/your/tsvfile.tsv:/app/input_data/full.tsv \
  -v $(pwd)/output_data:/app/output_data \
  -e TSV_INPUT_PATH=/app/input_data/full.tsv \
  case-study-spark


Explanation:

-v local_path:container_path mounts your local file inside Docker

Docker reads the file from /app/input_data/full.tsv

Results are written to your local machine in:
output_data/results

On Windows (Command Prompt / CMD)

Use %cd%:

docker run --rm ^
  -v C:\full\path\to\your\data.tsv:/app/input_data/full.tsv ^
  -v %cd%\output_data:/app/output_data ^
  -e TSV_INPUT_PATH=/app/input_data/full.tsv ^
  case-study-spark

6. Viewing the Output

Once the container finishes running, your results will be available on your machine in:

output_data/results/


Inside that folder, you will see:

part-00000-... .csv (or .tsv depending on your settings)


This file contains the Top 10 songs computed from the dataset. The output can also be viewed on the terminal (as the result gets printed before writing to a file)

* Important Notes About Mounting

Mounting allows Docker to see files on your machine.

-v local_path:container_path

Examples:

Local Machine	Inside Docker
/Users/me/data/file.tsv	/app/input_data/full.tsv
$(pwd)/output_data	/app/output_data
C:\Users\Me\Desktop\data.tsv	/app/input_data/full.tsv

This way:

Docker reads your big TSV file

Docker writes results back onto your machine

Mounting is required so users do not have to modify the container, rebuild, or upload large datasets.

* Troubleshooting
❌ “Path does not exist”

Check that your local path is absolute:

macOS/Linux:

/Users/yourname/folder/file.tsv


Windows:

C:\Users\yourname\folder\file.tsv


Relative paths do not work in mounts.

❌ “Unable to clear output directory”

Simply remove the output folder before re-running:

rm -rf output_data
mkdir output_data


This happens because Spark overwrites the directory during each run.

❌ “Permission denied”

Ensure Docker has access to the folder.
On macOS, go to Docker Desktop → Settings → Resources → File Sharing.

Summary

Once Docker is installed, users only need one command to run the entire analysis:

docker run --rm \
  -v /path/to/your/local-data.tsv:/app/input_data/full.tsv \
  -v $(pwd)/output_data:/app/output_data \
  -e TSV_INPUT_PATH=/app/input_data/full.tsv \
  case-study-spark


And the results will appear in:

output_data/results/


* ARCHITECTURE DIAGRAM

                    +----------------------------+
                    |        User Machine        |
                    |   (macOS / Linux / Windows)|
                    +-------------+--------------+
                                  |
                                  | Mount local TSV file
                                  | (-v local.tsv:/app/input_data/full.tsv)
                                  v
                      +-----------+------------+
                      |          Docker        |
                      |   case-study-spark     |
                      +-----------+------------+
                                  |
                                  | Environment variable:
                                  |   TSV_INPUT_PATH=/app/input_data/full.tsv
                                  |
                                  v
                  +---------------+----------------+
                  |              PySpark           |
                  |    (inside Docker container)   |
                  +---------------+----------------+
                                  |
     +----------------------------+-----------------------------+
     |                                                            |
     | 1. Read TSV (3GB) from mounted path                        |
     | 2. Convert to Parquet                                      |
     | 3. Analyze sessions (20-min gap)                           |
     | 4. Compute Top 50 sessions                                 |
     | 5. Find Top 10 tracks                                      |
     | 6. Write output TSV to: /app/output_data/results           |
     |                                                            |
     +----------------------------+-----------------------------+
                                  |
                                  | Mounted output folder
                                  | (-v ./output_data:/app/output_data)
                                  v
                      +-----------+-----------+
                      |  Results on User's    |
                      |       Machine         |
                      |  output_data/results/ |
                      +-----------------------+
