# lastfm-dataset-analysis

## 1. General Description
The repo contains a pyspark application to produce an analysis of top 10 songs played in the top 50 longest sessions by tracks
count on the lastfm dataset. It is designed to be executed from the CLI with command line arguments.
## 2. Environment setup

#### 1. Clone the Repository
```bash
git clone https://github.com/your-username/lastfm-dataset-analysis.git
cd lastfm-dataset-analysis
````

#### 2. Project Structure

Verify the project structure:
```bash
lastfm-dataset-analysis/
├── Dockerfile
├── requirements.txt
├── src/
│   ├── main.py
│   ├── utils.py
│   └── analysis.py
└── data/
    └── userid-timestamp-artid-artname-traid-traname.tsv
```

- Place your input TSV inside the data/ folder

- Create an empty output/ folder (optional — Docker will write to it)

- Build the Docker Image
```bash
docker build -t lastfm-analysis .
```

This creates a Docker image named lastfm-analysis using Python 3.10 and OpenJDK 17.


## 3. Execution instructions

Run the container with volume mounts for data and output:
```bash
docker run --rm \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/output:/app/output" \
  lastfm-analysis \
  python src/main.py \
  -i "/app/data/userid-timestamp-artid-artname-traid-traname.tsv" \
  -o "/app/output"
```

Spark will create the results inside a subdirectory like:
```bash
/app/output/final_results/
```

On the host computer, the output file will appear in:
```bash
output/final_results/
```