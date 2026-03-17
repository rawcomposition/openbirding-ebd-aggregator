#!/usr/bin/env python3
"""
Generate eBird observation statistics database.

Uses DuckDB to efficiently process large TSV files (100+ GB) without loading
them entirely into memory.

Usage:
    python generate_data.py <species_file> <sampling_file> <output.db> \
        --version-year <year> --version-month <month>

Example:
    python generate_data.py ebd_filtered.tsv sampling_filtered.tsv ebird.db \
        --version-year 2025 --version-month Feb

For very large files (100+ GB), you may want to:
    - Use --temp-dir to specify a fast SSD for intermediate data
    - Use --memory-limit to control DuckDB's memory usage (default: 80% of RAM)
    - Use --threads to control parallelism (default: all cores)
"""

import argparse
from datetime import datetime, timezone
import os
import shutil
import sqlite3
import sys
import time
from pathlib import Path
from typing import Optional

import duckdb
import requests

from utils import format_duration


KiB = 1024
MiB = 1024 * KiB
GiB = 1024 * MiB

# Use larger pages so the final DB stays smaller.
SQLITE_BUILD_PAGE_SIZE = 16 * KiB

# Let SQLite read part of the file more efficiently (build time only).
SQLITE_BUILD_MMAP_SIZE = 1 * GiB

# Keep the build cache from being too small (build time only).
SQLITE_BUILD_CACHE_MIN_BYTES = 512 * MiB

# Keep the build cache from growing too large (build time only).
SQLITE_BUILD_CACHE_MAX_BYTES = 4 * GiB

# Leave free disk so temp files do not fill the drive (build time only).
DUCKDB_TEMP_HEADROOM_BYTES = 4 * GiB


def get_duckdb_max_temp_directory_size(temp_directory: Path) -> Optional[str]:
    """Reserve some free space while still allowing large spill-heavy queries."""
    try:
        free_bytes = shutil.disk_usage(temp_directory).free
    except OSError:
        return None

    if free_bytes <= DUCKDB_TEMP_HEADROOM_BYTES:
        return None

    max_gib = (free_bytes - DUCKDB_TEMP_HEADROOM_BYTES) // (1024 ** 3)
    if max_gib <= 0:
        return None
    return f"{max_gib}GiB"


def get_sqlite_build_cache_kib() -> int:
    """Choose an aggressive but bounded SQLite cache size for scratch builds."""
    try:
        total_bytes = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
        target_bytes = total_bytes // 8
    except (AttributeError, OSError, ValueError):
        target_bytes = 1024 * 1024 * 1024

    target_bytes = max(SQLITE_BUILD_CACHE_MIN_BYTES, target_bytes)
    target_bytes = min(SQLITE_BUILD_CACHE_MAX_BYTES, target_bytes)
    return target_bytes // 1024


def open_sqlite_build_connection(
    db_path: Path,
    initialize_page_size: bool = False,
) -> sqlite3.Connection:
    """
    Open a SQLite connection with aggressive build-time settings.

    The database is scratch-built from source data, so we can trade crash
    durability for faster bulk writes.
    """
    sqlite_con = sqlite3.connect(db_path)
    if initialize_page_size:
        sqlite_con.execute(f"PRAGMA page_size = {SQLITE_BUILD_PAGE_SIZE}")
    sqlite_con.execute("PRAGMA journal_mode = MEMORY")
    sqlite_con.execute("PRAGMA synchronous = OFF")
    sqlite_con.execute("PRAGMA locking_mode = EXCLUSIVE")
    sqlite_con.execute("PRAGMA temp_store = MEMORY")
    sqlite_con.execute(f"PRAGMA cache_size = -{get_sqlite_build_cache_kib()}")
    sqlite_con.execute(f"PRAGMA mmap_size = {SQLITE_BUILD_MMAP_SIZE}")
    return sqlite_con


def download_taxonomy(sqlite_con: sqlite3.Connection) -> int:
    """
    Download eBird taxonomy and insert into species table.
    Returns the number of species inserted.
    """
    url = "https://api.ebird.org/v2/ref/taxonomy/ebird?fmt=json&cat=species"

    response = requests.get(url, timeout=60)
    response.raise_for_status()
    taxonomy = response.json()

    # Create species table
    sqlite_con.execute("DROP TABLE IF EXISTS species")
    sqlite_con.execute("""
        CREATE TABLE species (
            id INTEGER PRIMARY KEY,
            sci_name TEXT NOT NULL,
            name TEXT NOT NULL,
            code TEXT NOT NULL UNIQUE,
            taxon_order INTEGER NOT NULL,
            search_codes TEXT
        )
    """)

    # Insert species
    for i, sp in enumerate(taxonomy, start=1):
        # Merge bandingCodes and comNameCodes into space-separated search_codes
        codes = []
        if sp.get("bandingCodes"):
            codes.extend(sp["bandingCodes"])
        if sp.get("comNameCodes"):
            codes.extend(sp["comNameCodes"])
        search_codes = " ".join(codes) if codes else None

        sqlite_con.execute(
            "INSERT INTO species (id, sci_name, name, code, taxon_order, search_codes) VALUES (?, ?, ?, ?, ?, ?)",
            (i, sp["sciName"], sp["comName"], sp["speciesCode"], sp["taxonOrder"], search_codes)
        )

    sqlite_con.commit()
    return len(taxonomy)


def build_database(
    species_file: Path,
    sampling_file: Path,
    output_db: Path,
    version_year: str,
    version_month: str,
    temp_dir: Optional[Path] = None,
    memory_limit: Optional[str] = None,
    threads: Optional[int] = None,
    wilson_z: float = 1.96,
) -> None:
    """
    Build the month_obs database from eBird species and sampling files.
    """
    start_time = time.time()

    # Wilson score constants derived from z-index
    z_sq = wilson_z * wilson_z
    z_sq_half = z_sq / 2
    z_sq_quarter = z_sq / 4

    # Configure DuckDB for large file processing
    config = {}
    duckdb_temp_dir = temp_dir.resolve() if temp_dir else (Path.cwd() / ".tmp").resolve()
    duckdb_temp_dir.mkdir(parents=True, exist_ok=True)
    config["temp_directory"] = str(duckdb_temp_dir)
    if threads:
        config["threads"] = threads

    con = duckdb.connect(config=config) if config else duckdb.connect()

    # Set memory limit if specified
    if memory_limit:
        con.execute(f"SET memory_limit = '{memory_limit}'")
    con.execute("SET preserve_insertion_order = false")
    max_temp_directory_size = get_duckdb_max_temp_directory_size(duckdb_temp_dir)
    if max_temp_directory_size:
        con.execute(f"SET max_temp_directory_size = '{max_temp_directory_size}'")

    # Install and load SQLite extension for direct export
    con.execute("INSTALL sqlite; LOAD sqlite;")

    print(f"Processing species file: {species_file}")
    print(f"Processing sampling file: {sampling_file}")
    print(f"Output database: {output_db}")
    print(f"Temp directory: {duckdb_temp_dir}")
    if memory_limit:
        print(f"Memory limit: {memory_limit}")
    if threads:
        print(f"Threads: {threads}")

    total_steps = 8
    step_num = 0
    initialize_page_size = not output_db.exists() or output_db.stat().st_size == 0

    # Step 1: Download taxonomy
    step_num += 1
    print(f"\nStep {step_num}/{total_steps}: Downloading eBird taxonomy...")
    step_start = time.time()
    sqlite_con = open_sqlite_build_connection(
        output_db,
        initialize_page_size=initialize_page_size,
    )
    taxonomy_count = download_taxonomy(sqlite_con)
    sqlite_con.close()
    print(f"  Downloaded {taxonomy_count:,} species ({format_duration(time.time() - step_start)})")

    # Create the final month_obs table up front so DuckDB can insert rows in
    # primary-key order directly into the compact WITHOUT ROWID layout.
    sqlite_con = open_sqlite_build_connection(output_db)
    sqlite_con.execute("DROP TABLE IF EXISTS month_obs")
    sqlite_con.execute("""
        CREATE TABLE month_obs (
            location_id TEXT NOT NULL,
            month INTEGER NOT NULL,
            species_id INTEGER NOT NULL,
            obs INTEGER NOT NULL,
            samples INTEGER NOT NULL,
            score REAL NOT NULL,
            PRIMARY KEY (location_id, month, species_id)
        ) WITHOUT ROWID
    """)
    sqlite_con.commit()
    sqlite_con.close()

    # Attach SQLite database for output
    con.execute(f"ATTACH '{output_db}' AS sqlite_db (TYPE SQLITE)")

    # Step 2: Calculate samples per (location, month) and (location, year) from sampling file
    step_num += 1
    print(f"\nStep {step_num}/{total_steps}: Calculating samples per location...")
    step_start = time.time()
    con.execute(f"""
        CREATE TEMP TABLE samples_agg AS
        SELECT
            "LOCALITY ID" AS location_id,
            EXTRACT(MONTH FROM CAST("OBSERVATION DATE" AS DATE)) AS month,
            COUNT(DISTINCT COALESCE(NULLIF("GROUP IDENTIFIER", ''), "SAMPLING EVENT IDENTIFIER")) AS samples
        FROM read_csv(
            '{sampling_file}',
            delim='\t',
            header=true,
            quote='',
            ignore_errors=true
        )
        GROUP BY location_id, month
    """)
    # Also create yearly samples aggregation
    con.execute("""
        CREATE TEMP TABLE year_samples_agg AS
        SELECT
            location_id,
            SUM(samples) AS samples
        FROM samples_agg
        GROUP BY location_id
    """)
    print(f"  Done ({format_duration(time.time() - step_start)})")

    # Step 3: Calculate observations and join with samples
    step_num += 1
    print(f"\nStep {step_num}/{total_steps}: Calculating observations...")
    step_start = time.time()
    con.execute(f"""
        CREATE TEMP TABLE observations_agg AS
        SELECT
            o.location_id,
            o.month,
            sp.id AS species_id,
            o.obs,
            s.samples
        FROM (
            SELECT
                "LOCALITY ID" AS location_id,
                EXTRACT(MONTH FROM CAST("OBSERVATION DATE" AS DATE)) AS month,
                "SCIENTIFIC NAME" AS scientific_name,
                COUNT(DISTINCT COALESCE(NULLIF("GROUP IDENTIFIER", ''), "SAMPLING EVENT IDENTIFIER")) AS obs
            FROM read_csv(
                '{species_file}',
                delim='\t',
                header=true,
                quote='',
                ignore_errors=true
            )
            GROUP BY location_id, month, scientific_name
        ) o
        JOIN sqlite_db.species sp ON o.scientific_name = sp.sci_name
        JOIN samples_agg s
            ON o.location_id = s.location_id
            AND o.month = s.month
    """)
    month_obs_count = con.execute("SELECT COUNT(*) FROM observations_agg").fetchone()[0]
    loc_count = con.execute("SELECT COUNT(DISTINCT location_id) FROM observations_agg").fetchone()[0]
    species_count = con.execute("SELECT COUNT(DISTINCT species_id) FROM observations_agg").fetchone()[0]
    print(f"  Done ({format_duration(time.time() - step_start)})")

    # Step 4: Insert month_obs directly into the final WITHOUT ROWID table.
    # Ordering by the primary key dramatically reduces SQLite B-tree churn.
    step_num += 1
    print(f"\nStep {step_num}/{total_steps}: Inserting month_obs into SQLite...")
    step_start = time.time()
    con.execute(f"""
        INSERT INTO sqlite_db.month_obs (location_id, month, species_id, obs, samples, score)
        SELECT
            o.location_id,
            o.month,
            o.species_id,
            o.obs,
            o.samples,
            -- Wilson score lower bound (z={wilson_z})
            (o.obs + {z_sq_half} - {wilson_z} * sqrt(o.obs * (o.samples - o.obs) / o.samples + {z_sq_quarter}))
                / (o.samples + {z_sq}) AS score
        FROM observations_agg o
        ORDER BY o.location_id, o.month, o.species_id
    """)
    print(f"  Inserted {month_obs_count:,} rows ({format_duration(time.time() - step_start)})")

    # Step 5: Create and populate year_obs table
    # Aggregate from observations_agg (not month_obs) to avoid losing data filtered at month level
    step_num += 1
    print(f"\nStep {step_num}/{total_steps}: Creating year_obs table...")
    step_start = time.time()

    con.execute("DROP TABLE IF EXISTS sqlite_db.year_obs")
    con.execute("""
        CREATE TABLE sqlite_db.year_obs (
            location_id TEXT NOT NULL,
            species_id INTEGER NOT NULL,
            obs INTEGER NOT NULL,
            samples INTEGER NOT NULL,
            score REAL NOT NULL
        )
    """)

    con.execute(f"""
        INSERT INTO sqlite_db.year_obs (location_id, species_id, obs, samples, score)
        SELECT
            agg.location_id,
            agg.species_id,
            agg.obs,
            ys.samples,
            -- Wilson score lower bound (z={wilson_z})
            (agg.obs + {z_sq_half} - {wilson_z} * sqrt(agg.obs * (ys.samples - agg.obs) / ys.samples + {z_sq_quarter}))
                / (ys.samples + {z_sq}) AS score
        FROM (
            SELECT
                o.location_id,
                o.species_id,
                SUM(o.obs) AS obs
            FROM observations_agg o
            GROUP BY o.location_id, o.species_id
        ) agg
        JOIN year_samples_agg ys ON agg.location_id = ys.location_id
    """)

    year_obs_count = con.execute("SELECT COUNT(*) FROM sqlite_db.year_obs").fetchone()[0]
    print(f"  Created {year_obs_count:,} rows ({format_duration(time.time() - step_start)})")

    # Step 6: Extract hotspots from sampling data
    step_num += 1
    print(f"\nStep {step_num}/{total_steps}: Extracting hotspots from sampling data...")
    step_start = time.time()

    con.execute("DROP TABLE IF EXISTS sqlite_db.hotspots")
    con.execute("""
        CREATE TABLE sqlite_db.hotspots (
            id TEXT PRIMARY KEY,
            name TEXT,
            country_code TEXT,
            subnational1_code TEXT,
            subnational2_code TEXT,
            region_code TEXT,
            lat REAL,
            lng REAL
        )
    """)

    # Extract unique locations from sampling data
    # Filter out invalid subnational1 codes (e.g., "CO-" without state)
    # Set region_code to most specific available: subnational2 > subnational1 > country
    con.execute(f"""
        INSERT INTO sqlite_db.hotspots (id, name, country_code, subnational1_code, subnational2_code, region_code, lat, lng)
        SELECT
            "LOCALITY ID" AS id,
            MAX("LOCALITY") AS name,
            MAX("COUNTRY CODE") AS country_code,
            CASE
                WHEN MAX("STATE CODE") IS NOT NULL
                     AND MAX("STATE CODE") != ''
                     AND LENGTH(MAX("STATE CODE")) > LENGTH(SPLIT_PART(MAX("STATE CODE"), '-', 1)) + 1
                THEN MAX("STATE CODE")
                ELSE NULL
            END AS subnational1_code,
            NULLIF(MAX("COUNTY CODE"), '') AS subnational2_code,
            -- region_code: most specific available
            COALESCE(
                NULLIF(MAX("COUNTY CODE"), ''),
                CASE
                    WHEN MAX("STATE CODE") IS NOT NULL
                         AND MAX("STATE CODE") != ''
                         AND LENGTH(MAX("STATE CODE")) > LENGTH(SPLIT_PART(MAX("STATE CODE"), '-', 1)) + 1
                    THEN MAX("STATE CODE")
                    ELSE NULL
                END,
                MAX("COUNTRY CODE")
            ) AS region_code,
            MAX("LATITUDE") AS lat,
            MAX("LONGITUDE") AS lng
        FROM read_csv(
            '{sampling_file}',
            delim='\t',
            header=true,
            quote='',
            ignore_errors=true
        )
        GROUP BY "LOCALITY ID"
    """)

    hotspot_count = con.execute("SELECT COUNT(*) FROM sqlite_db.hotspots").fetchone()[0]
    print(f"  Extracted {hotspot_count:,} locations ({format_duration(time.time() - step_start)})")

    con.close()

    # Step 7: Create indexes using sqlite3
    step_num += 1
    print(f"\nStep {step_num}/{total_steps}: Creating indexes...")
    step_start = time.time()
    indexes = [
        # Species-based queries (sorted by score)
        "CREATE INDEX IF NOT EXISTS idx_mo_species_score ON month_obs(species_id, score DESC)",
        "CREATE INDEX IF NOT EXISTS idx_yo_species_score ON year_obs(species_id, score DESC)",
        # Hotspot indexes
        "CREATE INDEX IF NOT EXISTS idx_hotspots_country ON hotspots(country_code)",
        "CREATE INDEX IF NOT EXISTS idx_hotspots_subnational1 ON hotspots(subnational1_code)",
        "CREATE INDEX IF NOT EXISTS idx_hotspots_subnational2 ON hotspots(subnational2_code)",
        "CREATE INDEX IF NOT EXISTS idx_hotspots_region ON hotspots(region_code)",
        # Note: location-based queries for month_obs are covered by the PRIMARY KEY and WITHOUT ROWID optimizations
    ]

    sqlite_con = open_sqlite_build_connection(output_db)
    for index_sql in indexes:
        sqlite_con.execute(index_sql)
    sqlite_con.commit()
    sqlite_con.close()

    print(f"  Done ({format_duration(time.time() - step_start)})")

    # Step 8: Create metadata table
    step_num += 1
    print(f"\nStep {step_num}/{total_steps}: Writing metadata...")
    step_start = time.time()
    version = f"{version_month.lower()}-{version_year}"
    sqlite_con = open_sqlite_build_connection(output_db)
    sqlite_con.execute("DROP TABLE IF EXISTS metadata")
    sqlite_con.execute("""
        CREATE TABLE metadata (
            version TEXT NOT NULL,
            version_year TEXT NOT NULL,
            version_month TEXT NOT NULL,
            generated_at TEXT NOT NULL
        )
    """)
    sqlite_con.execute(
        "INSERT INTO metadata (version, version_year, version_month, generated_at) VALUES (?, ?, ?, ?)",
        (
            version,
            version_year,
            version_month,
            datetime.now(timezone.utc).isoformat(),
        ),
    )
    sqlite_con.commit()
    sqlite_con.close()
    print(f"  Done ({format_duration(time.time() - step_start)})")

    # Summary
    total_time = time.time() - start_time
    print("\n" + "=" * 50)
    print("Summary:")
    print(f"  Total month_obs rows: {month_obs_count:,}")
    print(f"  Total year_obs rows: {year_obs_count:,}")
    print(f"  Total locations: {loc_count:,}")
    print(f"  Unique species: {species_count:,}")
    print(f"  Hotspots: {hotspot_count:,}")
    print(f"  Total time: {format_duration(total_time)}")
    print(f"\nDatabase written to: {output_db}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate eBird observation statistics database.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  python generate_data.py ebd_filtered.tsv sampling_filtered.tsv output.db \
      --version-year 2025 --version-month Feb

  # Large dataset with memory and temp directory settings
  python generate_data.py ebd_filtered.tsv sampling_filtered.tsv output.db \\
      --version-year 2025 --version-month Feb \\
      --memory-limit 24GB --threads 8
        """,
    )
    parser.add_argument(
        "species_file",
        type=Path,
        help="Path to species observations file (TSV/TXT)",
    )
    parser.add_argument(
        "sampling_file",
        type=Path,
        help="Path to sampling/checklists file (TSV/TXT)",
    )
    parser.add_argument(
        "output_db",
        type=Path,
        help="Path to output SQLite database",
    )
    parser.add_argument(
        "--temp-dir",
        type=Path,
        help="Directory for DuckDB temp files (use fast SSD for large datasets)",
    )
    parser.add_argument(
        "--memory-limit",
        type=str,
        help="Memory limit for DuckDB (e.g., '32GB', '80%%')",
    )
    parser.add_argument(
        "--threads",
        type=int,
        help="Number of threads for DuckDB (default: all cores)",
    )
    parser.add_argument(
        "--wilson-z",
        type=float,
        default=1.96,
        help="Z-index for Wilson score calculation (default: 1.96 for 95%% confidence)",
    )
    parser.add_argument(
        "--version-year",
        type=str,
        required=True,
        help="Year of the eBird data version (e.g., 2025)",
    )
    parser.add_argument(
        "--version-month",
        type=str,
        required=True,
        help="Month of the eBird data version (e.g., Feb)",
    )

    args = parser.parse_args()

    if not args.species_file.exists():
        print(f"Error: Species file not found: {args.species_file}", file=sys.stderr)
        sys.exit(1)

    if not args.sampling_file.exists():
        print(f"Error: Sampling file not found: {args.sampling_file}", file=sys.stderr)
        sys.exit(1)

    if args.temp_dir and not args.temp_dir.exists():
        print(f"Error: Temp directory not found: {args.temp_dir}", file=sys.stderr)
        sys.exit(1)

    build_database(
        args.species_file,
        args.sampling_file,
        args.output_db,
        version_year=args.version_year,
        version_month=args.version_month,
        temp_dir=args.temp_dir,
        memory_limit=args.memory_limit,
        threads=args.threads,
        wilson_z=args.wilson_z,
    )


if __name__ == "__main__":
    main()
