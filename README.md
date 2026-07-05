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
   - **Filter Species** - Extract required columns from complete checklists
   - **Download Sampling** - Download the eBird Sampling Dataset
   - **Extract Sampling** - Extract the gzipped sampling data from the tar
   - **Filter Sampling** - Extract required columns from complete checklists
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

CREATE TABLE regions (
    id INTEGER PRIMARY KEY,
    code TEXT NOT NULL
);

CREATE TABLE month_obs (
    location_id TEXT NOT NULL,
    month INTEGER NOT NULL,
    species_id INTEGER NOT NULL,
    obs INTEGER NOT NULL,
    samples INTEGER NOT NULL,
    score REAL NOT NULL,
    PRIMARY KEY (location_id, month, species_id)
) WITHOUT ROWID;

CREATE TABLE year_obs (
    location_id TEXT NOT NULL,
    species_id INTEGER NOT NULL,
    obs INTEGER NOT NULL,
    samples INTEGER NOT NULL,
    score REAL NOT NULL,
    PRIMARY KEY (location_id, species_id)
) WITHOUT ROWID;

CREATE TABLE region_month_obs (
    region_id INTEGER NOT NULL,
    month INTEGER NOT NULL,
    species_id INTEGER NOT NULL,
    obs INTEGER NOT NULL,
    PRIMARY KEY (region_id, month, species_id)
) WITHOUT ROWID;

CREATE TABLE region_month_samples (
    region_id INTEGER NOT NULL,
    month INTEGER NOT NULL,
    samples INTEGER NOT NULL,
    PRIMARY KEY (region_id, month)
) WITHOUT ROWID;

CREATE TABLE metadata (
    version TEXT NOT NULL,
    version_year TEXT NOT NULL,
    version_month TEXT NOT NULL,
    generated_at TEXT NOT NULL
);

CREATE INDEX idx_mo_species_score ON month_obs(species_id, score DESC);
CREATE INDEX idx_yo_species_score ON year_obs(species_id, score DESC);
CREATE UNIQUE INDEX idx_regions_code ON regions(code);
CREATE INDEX idx_hotspots_country ON hotspots(country_code);
CREATE INDEX idx_hotspots_subnational1 ON hotspots(subnational1_code);
CREATE INDEX idx_hotspots_subnational2 ON hotspots(subnational2_code);
CREATE INDEX idx_hotspots_region ON hotspots(region_code);
```

## Notes

- Only includes complete checklists (`ALL SPECIES REPORTED = 1`)
- `month_obs` and `year_obs` only include hotspot locations (`LOCALITY TYPE = H`)
- `region_month_obs` includes complete checklists from hotspots and personal locations
- Group checklists are deduplicated
- Only species-level taxa are included (`CATEGORY` = 'species' or 'issf')
- The `score` column uses Wilson score lower bound for ranking
