# EBD Aggregator

The eBird Basic Dataset (EBD) Aggregator is a bunch of scripts to aggregate the eBird data into a SQLite database and individual region pack files. This data is used on OpenBirding.org ([see repo](https://github.com/rawcomposition/openbirding)) and the OpenBirding mobile app ([see repo](https://github.com/rawcomposition/openbirding-rn)).

## Requirements

- Python 3.8+
- aria2c (for downloading): `brew install aria2`
- pigz (for fast decompression): `brew install pigz`
- pv (for upload progress): `brew install pv`

## Setup

Create a virtual environment and install Python dependencies:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Copy the example environment file and configure:

```bash
cp .env.example .env
```

### Environment Variables

| Variable                  | Required       | Default | Description                                        |
| ------------------------- | -------------- | ------- | -------------------------------------------------- |
| `OUTPUT_PATH`             | No             | -       | Base path for datasets and output directories      |
| `EBIRD_API_KEY`           | For packs      | -       | eBird API key (required for Generate Packs)        |
| `S3_KEY_ID`               | For packupload | -       | S3 access key ID                                   |
| `S3_SECRET`               | For upload     | -       | S3 secret access key                               |
| `S3_BUCKET`               | For upload     | -       | S3 bucket name                                     |
| `S3_ENDPOINT`             | For upload     | -       | S3 endpoint URL                                    |
| `S3_DIR`                  | No             | -       | S3 directory prefix (e.g., `v1`)                   |
| `S3_PUBLIC_URL`           | No             | -       | Public URL for pack files (e.g., `https://cdn.example.com/`) |
| `MEMORY_LIMIT`            | No             | 24      | DuckDB memory limit in GB                          |
| `THREADS`                 | No             | 8       | Number of threads for DuckDB                       |
| `WILSON_SCORE_Z_INDEX`    | No             | 1.96    | Z-index for Wilson score calculation               |
| `SSH_USER`                | For SQLite upload | -    | SSH username for remote server                     |
| `SSH_HOST`                | For SQLite upload | -    | SSH host/IP for remote server                      |
| `DOCKER_VOLUME`           | For SQLite upload | -    | Docker volume name on remote server                |
| `NTFY_NOTIFICATION_TOPIC` | No             | -       | [ntfy.sh](https://ntfy.sh) topic for notifications |

## Usage

Activate the virtual environment and run the interactive CLI:

```bash
source venv/bin/activate
python cli.py
```

The CLI will prompt you to:

1. Choose which dataset to use (current or previous month)
2. Choose which step to run:
   - **Download Species** - Download the eBird Basic Dataset
   - **Extract Species** - Extract the gzipped species data from the tar
   - **Filter Species** - Extract required columns and filter to hotspots
   - **Download Sampling** - Download the eBird Sampling Dataset
   - **Extract Sampling** - Extract the gzipped sampling data from the tar
   - **Filter Sampling** - Extract required columns and filter to hotspots
   - **Build Database** - Generate the SQLite database
   - **Generate Packs** - Generate compressed JSON packs for each region
   - **All (without upload)** - Run all steps except upload
   - **Upload Packs** - Upload packs to S3-compatible storage
   - **Upload SQLite** - Upload the SQLite database to the remote server

Each step skips automatically if its output file already exists.

### Output Structure

```
aggregator/
├── datasets/           # Downloaded and intermediate data files
└── output/
    ├── targets-{month}-{year}.db
    └── packs/
        ├── packs.json.gz
        └── {month}-{year}/
            ├── US.json.gz
            ├── US-CA.json.gz
            └── ...
```

## Database Schema

```sql
CREATE TABLE species (
    id INTEGER PRIMARY KEY,
    sci_name TEXT NOT NULL,
    name TEXT NOT NULL,
    code TEXT NOT NULL UNIQUE,
    taxon_order INTEGER NOT NULL,
    search_codes TEXT
);

CREATE TABLE hotspots (
    id TEXT PRIMARY KEY,
    name TEXT,
    country_code TEXT,
    subnational1_code TEXT,
    subnational2_code TEXT,
    region_code TEXT,
    lat REAL,
    lng REAL
);

CREATE TABLE month_obs (
    location_id TEXT NOT NULL,
    month INTEGER NOT NULL,
    species_id INTEGER NOT NULL,
    obs INTEGER NOT NULL,
    samples INTEGER NOT NULL,
    score REAL NOT NULL
);

CREATE TABLE year_obs (
    location_id TEXT NOT NULL,
    species_id INTEGER NOT NULL,
    obs INTEGER NOT NULL,
    samples INTEGER NOT NULL,
    score REAL NOT NULL
);
```

## Notes

- Only includes hotspot locations (`LOCALITY TYPE = H`)
- Only includes complete checklists (`ALL SPECIES REPORTED = 1`)
- Group checklists are deduplicated
- Only species-level taxa are included (`CATEGORY` = 'species' or 'issf')
- The `score` column uses Wilson score lower bound for ranking
