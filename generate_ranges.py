#!/usr/bin/env python3
"""
Derive per-region species range sizes from the res-6 h3_* tables in a targets db.

Usage:
    python generate_ranges.py <targets.db> [--min-obs 2] [--min-samples 10] \
        [--memory-limit 24GB] [--threads 8]
"""

import argparse
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import duckdb

from generate_data import fetch_taxonomy
from utils import format_duration

SOURCE_RES = 6
RANGE_RES = 4
WORLD = "world"
DEFAULT_MIN_OBS = 2
DEFAULT_MIN_SAMPLES = 2

RANGE_TABLES = ("species_ranges", "region_surveyed_cells", "species_ranges_metadata")


def prepare_tables(target_db: Path) -> None:
    sq = sqlite3.connect(target_db)
    for table in RANGE_TABLES:
        sq.execute(f"DROP TABLE IF EXISTS {table}")
    sq.execute(
        """
        CREATE TABLE species_ranges (
            region_code TEXT NOT NULL,
            species_id INTEGER NOT NULL,
            occupied_cells INTEGER NOT NULL,
            obs INTEGER NOT NULL,
            PRIMARY KEY (region_code, species_id)
        ) WITHOUT ROWID
        """
    )
    sq.execute(
        """
        CREATE TABLE region_surveyed_cells (
            region_code TEXT PRIMARY KEY,
            surveyed_cells INTEGER NOT NULL
        ) WITHOUT ROWID
        """
    )
    sq.execute(
        """
        CREATE TABLE species_ranges_metadata (
            res INTEGER NOT NULL,
            min_obs INTEGER NOT NULL,
            min_samples INTEGER NOT NULL,
            generated_at TEXT NOT NULL
        )
        """
    )
    sq.commit()
    sq.close()


def build_cell_totals(con: duckdb.DuckDBPyConnection) -> None:
    print("  - mapping res-6 cells to res-4 parents and countries...")
    con.execute(
        f"""
        CREATE TEMP TABLE child_cells AS
        SELECT
            cell_ref,
            CAST(h3_cell_to_parent(h3, {RANGE_RES}) AS BIGINT) AS parent,
            NULLIF(split_part(region_code, '-', 1), '') AS country
        FROM tdb.h3_cells
        WHERE res = {SOURCE_RES}
        """
    )
    print("  - summing obs across months (full scan of h3_cell_obs)...")
    con.execute(
        f"""
        CREATE TEMP TABLE child_obs AS
        SELECT cell_ref, species_id, SUM(obs) AS obs
        FROM tdb.h3_cell_obs
        WHERE res = {SOURCE_RES}
        GROUP BY cell_ref, species_id
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE child_samples AS
        SELECT cell_ref, SUM(samples) AS samples
        FROM tdb.h3_cell_samples
        WHERE res = {SOURCE_RES}
        GROUP BY cell_ref
        """
    )
    print("  - rolling up to res-4 per region (world + countries)...")
    con.execute(
        f"""
        CREATE TEMP TABLE region_cell_samples AS
        SELECT '{WORLD}' AS region_code, c.parent, SUM(s.samples) AS samples
        FROM child_cells c JOIN child_samples s USING (cell_ref)
        GROUP BY c.parent
        UNION ALL
        SELECT c.country, c.parent, SUM(s.samples)
        FROM child_cells c JOIN child_samples s USING (cell_ref)
        WHERE c.country IS NOT NULL
        GROUP BY c.country, c.parent
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE region_cell_obs AS
        SELECT '{WORLD}' AS region_code, c.parent, o.species_id, SUM(o.obs) AS obs
        FROM child_cells c JOIN child_obs o USING (cell_ref)
        GROUP BY c.parent, o.species_id
        UNION ALL
        SELECT c.country, c.parent, o.species_id, SUM(o.obs)
        FROM child_cells c JOIN child_obs o USING (cell_ref)
        WHERE c.country IS NOT NULL
        GROUP BY c.country, c.parent, o.species_id
        """
    )


def extinct_species_codes(target_db: Path) -> set:
    sq = sqlite3.connect(target_db)
    (version,) = sq.execute("SELECT taxonomy_version FROM metadata").fetchone()
    sq.close()
    return {t["speciesCode"] for t in fetch_taxonomy(version) if t.get("extinct")}


def load_extinct_species(con: duckdb.DuckDBPyConnection, extinct_codes: set) -> int:
    con.execute("CREATE TEMP TABLE extinct_species (species_id INTEGER)")
    if extinct_codes:
        placeholders = ", ".join("?" for _ in extinct_codes)
        con.execute(
            f"INSERT INTO extinct_species SELECT id FROM tdb.species WHERE code IN ({placeholders})",
            list(extinct_codes),
        )
    return con.execute("SELECT COUNT(*) FROM extinct_species").fetchone()[0]


def write_ranges(con: duckdb.DuckDBPyConnection, min_obs: int, min_samples: int) -> None:
    con.execute(
        """
        CREATE TEMP TABLE surveyed AS
        SELECT region_code, parent FROM region_cell_samples WHERE samples >= ?
        """,
        [min_samples],
    )
    con.execute(
        """
        INSERT INTO tdb.region_surveyed_cells (region_code, surveyed_cells)
        SELECT region_code, COUNT(*) FROM surveyed
        GROUP BY region_code
        ORDER BY region_code
        """
    )
    con.execute(
        """
        INSERT INTO tdb.species_ranges (region_code, species_id, occupied_cells, obs)
        SELECT o.region_code, o.species_id, COUNT(*), SUM(o.obs)
        FROM region_cell_obs o
        JOIN surveyed s USING (region_code, parent)
        WHERE o.obs >= ?
          AND o.species_id NOT IN (SELECT species_id FROM extinct_species)
        GROUP BY o.region_code, o.species_id
        ORDER BY o.region_code, o.species_id
        """,
        [min_obs],
    )


def build(
    target_db: Path,
    min_obs: int,
    min_samples: int,
    memory_limit: Optional[str],
    threads: Optional[int],
) -> None:
    start = time.time()
    config = {"threads": threads} if threads else {}
    con = duckdb.connect(config=config)
    if memory_limit:
        con.execute(f"SET memory_limit = '{memory_limit}'")
    con.execute("SET preserve_insertion_order = false")
    con.execute("INSTALL h3 FROM community; LOAD h3;")
    con.execute("INSTALL sqlite; LOAD sqlite;")

    print(f"Targets db:  {target_db}")
    print(f"Range res:   {RANGE_RES} (from res {SOURCE_RES})")
    print(f"Occupied:    obs >= {min_obs}")
    print(f"Surveyed:    samples >= {min_samples}")

    print("\nStep 1/2: Aggregating cells...")
    t = time.time()
    extinct_codes = extinct_species_codes(target_db)
    prepare_tables(target_db)
    con.execute(f"ATTACH '{target_db}' AS tdb (TYPE SQLITE)")
    n_extinct = load_extinct_species(con, extinct_codes)
    print(f"  - excluding {n_extinct:,} extinct species")
    build_cell_totals(con)
    print(f"  ({format_duration(time.time() - t)})")

    print("\nStep 2/2: Writing range tables...")
    t = time.time()
    write_ranges(con, min_obs, min_samples)
    con.execute("DETACH tdb")
    con.close()

    sq = sqlite3.connect(target_db)
    sq.execute(
        "INSERT INTO species_ranges_metadata (res, min_obs, min_samples, generated_at) "
        "VALUES (?, ?, ?, ?)",
        (RANGE_RES, min_obs, min_samples, datetime.now(timezone.utc).isoformat()),
    )
    sq.execute("ANALYZE species_ranges")
    sq.execute("ANALYZE region_surveyed_cells")
    sq.commit()
    ranges = sq.execute("SELECT COUNT(*) FROM species_ranges").fetchone()[0]
    regions = sq.execute("SELECT COUNT(*) FROM region_surveyed_cells").fetchone()[0]
    world = sq.execute(
        "SELECT surveyed_cells FROM region_surveyed_cells WHERE region_code = ?", (WORLD,)
    ).fetchone()
    sq.close()
    print(f"  {ranges:,} region-species rows across {regions:,} regions")
    print(f"  world surveyed cells: {world[0] if world else 0:,}")
    print(f"  ({format_duration(time.time() - t)})")

    print(f"\nDone in {format_duration(time.time() - start)}")


def main():
    parser = argparse.ArgumentParser(
        description="Fold per-region species range sizes into a targets database."
    )
    parser.add_argument("target_db", type=Path)
    parser.add_argument("--min-obs", type=int, default=DEFAULT_MIN_OBS)
    parser.add_argument("--min-samples", type=int, default=DEFAULT_MIN_SAMPLES)
    parser.add_argument("--memory-limit", type=str)
    parser.add_argument("--threads", type=int)
    args = parser.parse_args()

    if not args.target_db.exists():
        print(f"Error: targets db not found: {args.target_db}", file=sys.stderr)
        sys.exit(1)

    build(
        target_db=args.target_db,
        min_obs=args.min_obs,
        min_samples=args.min_samples,
        memory_limit=args.memory_limit,
        threads=args.threads,
    )


if __name__ == "__main__":
    main()
