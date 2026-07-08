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
./venv/bin/pip install -r requirements.txt
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

Run the interactive CLI:

```bash
./cli
```

The `./cli` launcher uses `venv/bin/python` automatically, so you do not need
to activate the virtual environment first.

The CLI will prompt you to:

1. Choose which dataset to use (`Current` or `Previous`)
2. Choose which step to run:
   - **Download Species** - Download the eBird Basic Dataset
   - **Extract Species** - Extract the gzipped species data from the tar
   - **Filter Species** - Extract required columns from complete checklists
   - **Download Sampling** - Download the eBird Sampling Dataset
   - **Extract Sampling** - Extract the gzipped sampling data from the tar
   - **Filter Sampling** - Extract required columns from complete checklists
   - **Build Database** - Generate the SQLite database
   - **Build Occurrences DB** - Derive `occurrences-{month}-{year}.db` from the
     targets db: species-occurrence frequencies per hotspot and per H3 cell
     (res 3-4, rolled up from the res-6 h3 tables), plus a typed-array blob
     cache. Powers the web app's "Best Hotspots" tool.
   - **Generate Packs** - Generate compressed JSON packs for each region
   - **All (without upload)** - Run all steps except upload
   - **Upload Packs** - Upload packs to S3-compatible storage
   - **Upload SQLite** - Upload the targets and occurrences databases to the
     remote server (both staged as `.db.new`), then hot-swap each via the
     admin API — no restart needed

Each step skips automatically if its output file already exists.

Dataset release schedule: eBird publishes the previous month's dataset on the
15th of the current month. For example, the June dataset becomes the `Current`
option on July 15, and before July 15 the `Current` option remains May.

### Output Structure

```
aggregator/
├── datasets/           # Downloaded and intermediate data files
└── output/
    ├── targets-{month}-{year}.db
    ├── occurrences-{month}-{year}.db
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

### H3 grid tables

The **Build H3 Tables** step folds `h3_*` tables into the same targets db,
reusing its `species` table. These bin *every* complete checklist — hotspot
and personal location alike — into [H3](https://h3geo.org) grid cells, for
"most likely species within a polygon/radius" queries with monthly bar charts.

Only res 6 is stored; coarser grids roll up from it on demand. The 64-bit H3
index lives once per cell in `h3_cells`; the large obs/samples tables reference
cells by a dense, ~3-byte `cell_ref` (assigned in H3 order). `region_code` is
the winning region per cell (majority by checklist count). Counts are raw and
aggregable, so a client sums cells across an area before dividing obs by
samples to get a frequency.

```sql
CREATE TABLE h3_cells (
    res INTEGER NOT NULL,
    cell_ref INTEGER NOT NULL,      -- dense per-resolution id, assigned in H3 order
    h3 INTEGER NOT NULL,            -- 64-bit H3 cell index
    region_code TEXT,               -- winning region (majority by checklist count)
    lat REAL,
    lng REAL,
    PRIMARY KEY (res, cell_ref)
);

CREATE TABLE h3_cell_obs (
    res INTEGER NOT NULL,
    cell_ref INTEGER NOT NULL,
    month INTEGER NOT NULL,
    species_id INTEGER NOT NULL,
    obs INTEGER NOT NULL,           -- numerator (distinct checklists with the species)
    PRIMARY KEY (res, cell_ref, month, species_id)
) WITHOUT ROWID;

CREATE TABLE h3_cell_samples (
    res INTEGER NOT NULL,
    cell_ref INTEGER NOT NULL,
    month INTEGER NOT NULL,
    samples INTEGER NOT NULL,       -- denominator (distinct checklists in the cell-month)
    PRIMARY KEY (res, cell_ref, month)
) WITHOUT ROWID;

CREATE TABLE h3_metadata (
    version TEXT,
    res INTEGER NOT NULL,
    generated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX idx_h3_cells_h3 ON h3_cells(res, h3);
CREATE INDEX idx_h3_cells_res_latlng ON h3_cells(res, lat, lng);
```

A polygon query maps its covering H3 cells to `cell_ref` via
`idx_h3_cells_h3`, then filters `h3_cell_obs` / `h3_cell_samples` by
`(res, cell_ref, month)`.

## Notes

- Only includes complete checklists (`ALL SPECIES REPORTED = 1`)
- `month_obs` and `year_obs` only include hotspot locations (`LOCALITY TYPE = H`)
- `region_month_obs` includes complete checklists from hotspots and personal locations
- Group checklists are deduplicated
- Only species-level taxa are included (`CATEGORY` = 'species' or 'issf')
- The `score` column uses Wilson score lower bound for ranking

## Occurrences Database (side artifact)

The **Build Occurrences DB** step derives a compact companion database
(`occurrences-{month}-{year}.db`) from the targets db. It powers the web app's
"Best Hotspots" (lifer targets) tool, whose core query is
`lifers[loc] = qCount[loc] − seenCount[loc]`; `qCount` (species above a
frequency threshold at a location) is user-independent and precomputed here per
bucket, so the API answers worldwide queries by scanning only the user's seen
species.

It holds two parallel table families — `loc_*` for named hotspots and `zone_*`
for H3 grid cells (rolled up from the targets db's res-6 tables to res 3–4) —
plus `blob_cache`, the same data pre-packed as little-endian typed-array BLOBs.
That lets the Node API load its in-memory index in ~0.5 s of memcpy-speed reads
instead of iterating tens of millions of rows (~60 s); the blob layout is a
contract shared with `web/api/lib/lifers-index.ts`.

Frequency floors differ by family: hotspots keep rows at ≥5% frequency and ≥25
checklists (the smallest values the UI offers); grid cells use a permissive ≥1%
floor, since a cell's frequency is diluted by unrelated effort inside it, with
the same ≥25-checklist floor.

```sql
CREATE TABLE metadata (
    version TEXT, version_year TEXT, version_month TEXT,
    generated_at TEXT, buckets TEXT,           -- JSON array of frequency thresholds
    min_score REAL, min_checklists INTEGER
);

CREATE TABLE species (
    id INTEGER PRIMARY KEY,
    code TEXT NOT NULL,
    name TEXT NOT NULL,
    sci_name TEXT NOT NULL,
    sci_lower TEXT NOT NULL,                    -- lowercased for case-insensitive search
    name_lower TEXT NOT NULL,
    taxon_order INTEGER NOT NULL
);

-- Named hotspots
CREATE TABLE loc_meta (
    loc_ref INTEGER PRIMARY KEY,               -- dense id, assigned by descending sample count
    location_id TEXT NOT NULL,
    name TEXT,
    lat REAL, lng REAL,
    country_code TEXT,
    subnational1_code TEXT,
    subnational2_code TEXT,
    region_code TEXT,
    samples INTEGER NOT NULL
);

CREATE TABLE loc_species (
    species_id INTEGER NOT NULL,
    loc_ref INTEGER NOT NULL,
    bucket_level INTEGER NOT NULL,             -- highest frequency bucket the species meets here
    PRIMARY KEY (species_id, loc_ref)
) WITHOUT ROWID;

CREATE TABLE loc_qcount (
    bucket INTEGER NOT NULL,
    loc_ref INTEGER NOT NULL,
    q_count INTEGER NOT NULL,                  -- # species at/above this bucket at this location
    PRIMARY KEY (bucket, loc_ref)
) WITHOUT ROWID;

-- H3 grid cells (per resolution)
CREATE TABLE zone_meta (
    res INTEGER NOT NULL,
    cell_ref INTEGER NOT NULL,
    h3 INTEGER,
    lat REAL, lng REAL,
    samples INTEGER NOT NULL,
    PRIMARY KEY (res, cell_ref)
) WITHOUT ROWID;

CREATE TABLE zone_species (
    res INTEGER NOT NULL,
    species_id INTEGER NOT NULL,
    cell_ref INTEGER NOT NULL,
    bucket_level INTEGER NOT NULL,
    PRIMARY KEY (res, species_id, cell_ref)
) WITHOUT ROWID;

CREATE TABLE zone_qcount (
    res INTEGER NOT NULL,
    bucket INTEGER NOT NULL,
    cell_ref INTEGER NOT NULL,
    q_count INTEGER NOT NULL,
    PRIMARY KEY (res, bucket, cell_ref)
) WITHOUT ROWID;

-- Each hotspot's H3 cell per zone resolution; feeds the blob cache's
-- loc:cellRef:{res} map (hotspot -> zone cell_ref) so the API can count named
-- hotspots per cell without H3 math at request time. Not read directly.
CREATE TABLE loc_zone_h3 (
    res INTEGER NOT NULL,
    loc_ref INTEGER NOT NULL,
    h3 INTEGER NOT NULL,
    PRIMARY KEY (res, loc_ref)
) WITHOUT ROWID;

-- Pre-packed typed-array BLOBs mirroring the tables above (see lifers-index.ts)
CREATE TABLE blob_cache (
    key TEXT PRIMARY KEY,
    data BLOB NOT NULL
) WITHOUT ROWID;

CREATE INDEX idx_species_sci ON species(sci_lower);
CREATE INDEX idx_species_name ON species(name_lower);
CREATE INDEX idx_species_code ON species(code);
CREATE UNIQUE INDEX idx_loc_meta_locid ON loc_meta(location_id);
CREATE INDEX idx_loc_meta_region ON loc_meta(region_code);
```

If the targets db carries a `citation_metadata` table, it is copied across
verbatim (with its indexes) to keep citation/export metadata in sync between
the two databases.
