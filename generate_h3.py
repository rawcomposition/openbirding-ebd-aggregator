#!/usr/bin/env python3
"""
Generate H3-binned observation data from the eBird filtered datasets.

Unlike generate_data.py (which keys observations by eBird locality and keeps
only hotspots), this script bins *every* complete checklist -- hotspot and
personal location alike -- into H3 grid cells. This supports "most likely
species within radius R of a map point" queries with monthly bar charts,
without storing per-personal-location rows (which would explode pack size).

The h3_* tables are folded into an existing targets database (built by
generate_data.py), reusing its ``species`` table. Optimized for polygon ->
most-common-species queries (e.g. BirdPlan):

  h3_cells(cell_ref PK, h3, region_code)        -- H3<->dense-ref map + region
  h3_cell_obs(cell_ref, month, species_id, obs) -- numerator, clustered by cell
  h3_cell_samples(cell_ref, month, samples)     -- denominator (per cell-month)
  h3_metadata(version, res, generated_at)

The 64-bit H3 index is stored once per cell in h3_cells; the large obs/samples
tables reference cells by a dense ``cell_ref`` (~3-byte int) to stay slim. The
region_code on h3_cells is the winning region per cell (majority by checklist
count), used for the optional per-region mobile pack breakout (see
encode_region_pack); the binary pack encoder remains in this file for that.

Coordinates and region come from the sampling file; species observations come
from the (much larger) species file and are joined to cells via locality id.

Usage:
    python generate_h3.py <species_file> <sampling_file> <targets.db> \
        --version may-2026 [--res 6] \
        [--memory-limit 24GB] [--threads 8] [--temp-dir .tmp]
"""

import argparse
import gzip
import json
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import duckdb

from utils import format_duration

SCRIPT_DIR = Path(__file__).parent.resolve()

# Binary pack format. Bump if the on-disk layout changes.
PACK_MAGIC = b"H3P1"


# ---------------------------------------------------------------------------
# Binary pack encoding
# ---------------------------------------------------------------------------
#
# A pack file (after gunzip) is:
#
#   magic        4 bytes  "H3P1"
#   res          uint8    H3 resolution (e.g. 6)
#   cell_count   uvarint
#   repeat cell_count times, cells sorted ascending by cell id:
#       cell_delta     uvarint   delta from previous cell id (first = absolute)
#       sample_mask    uint16    bit m set => month m+1 has a sample count
#       sample[m]      uvarint   for each set bit, low month first
#       species_count  uvarint
#       repeat species_count times, sorted ascending by species id:
#           species_delta  uvarint   delta from previous species id
#           obs_mask       uint16    bit m set => month m+1 has an obs count
#           obs[m]         uvarint   for each set bit, low month first
#
# All integers are aggregable raw counts (not quantized), so a client can sum
# the cells within a radius and only then divide obs/samples to get frequency.


def _write_uvarint(buf: bytearray, n: int) -> None:
    if n < 0:
        raise ValueError(f"uvarint cannot encode negative value {n}")
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            buf.append(b | 0x80)
        else:
            buf.append(b)
            return


def _read_uvarint(data: bytes, pos: int) -> tuple[int, int]:
    shift = 0
    result = 0
    while True:
        b = data[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            return result, pos
        shift += 7


def encode_pack(cells: list[tuple], res: int) -> bytes:
    """
    Encode pack cells.

    ``cells`` is a list of (cell_id, samples, species) sorted by cell_id, where
    ``samples`` is a list of 12 ints (0 for absent months) and ``species`` is a
    list of (species_id, obs12) sorted by species_id with obs12 a list of 12
    ints.
    """
    buf = bytearray()
    buf += PACK_MAGIC
    buf.append(res)
    _write_uvarint(buf, len(cells))

    prev_cell = 0
    for cell_id, samples, species in cells:
        _write_uvarint(buf, cell_id - prev_cell)
        prev_cell = cell_id

        smask = 0
        for m in range(12):
            if samples[m]:
                smask |= 1 << m
        buf += smask.to_bytes(2, "little")
        for m in range(12):
            if smask & (1 << m):
                _write_uvarint(buf, samples[m])

        _write_uvarint(buf, len(species))
        prev_sp = 0
        for sp_id, obs in species:
            _write_uvarint(buf, sp_id - prev_sp)
            prev_sp = sp_id
            omask = 0
            for m in range(12):
                if obs[m]:
                    omask |= 1 << m
            buf += omask.to_bytes(2, "little")
            for m in range(12):
                if omask & (1 << m):
                    _write_uvarint(buf, obs[m])

    return bytes(buf)


def decode_pack(data: bytes) -> dict:
    """Inverse of encode_pack. Used for validation and as a reference decoder."""
    assert data[:4] == PACK_MAGIC, "bad magic"
    res = data[4]
    pos = 5
    cell_count, pos = _read_uvarint(data, pos)
    cells = []
    prev_cell = 0
    for _ in range(cell_count):
        delta, pos = _read_uvarint(data, pos)
        cell_id = prev_cell + delta
        prev_cell = cell_id

        smask = int.from_bytes(data[pos : pos + 2], "little")
        pos += 2
        samples = [0] * 12
        for m in range(12):
            if smask & (1 << m):
                samples[m], pos = _read_uvarint(data, pos)

        species_count, pos = _read_uvarint(data, pos)
        species = []
        prev_sp = 0
        for _ in range(species_count):
            d, pos = _read_uvarint(data, pos)
            sp_id = prev_sp + d
            prev_sp = sp_id
            omask = int.from_bytes(data[pos : pos + 2], "little")
            pos += 2
            obs = [0] * 12
            for m in range(12):
                if omask & (1 << m):
                    obs[m], pos = _read_uvarint(data, pos)
            species.append((sp_id, obs))
        cells.append((cell_id, samples, species))
    return {"res": res, "cells": cells}


# ---------------------------------------------------------------------------
# Pack hierarchy
# ---------------------------------------------------------------------------

def load_finest_packs() -> list[dict]:
    """
    Load packs.json and keep only the finest-granularity regions: a region is
    dropped if another region is a strict child of it (startswith region + '-').
    """
    with open(SCRIPT_DIR / "packs.json") as f:
        packs = json.load(f)
    regions = {p["region"] for p in packs}

    def has_child(region: str) -> bool:
        prefix = region + "-"
        return any(r != region and r.startswith(prefix) for r in regions)

    return [p for p in packs if not has_child(p["region"])]


# ---------------------------------------------------------------------------
# DuckDB build
# ---------------------------------------------------------------------------

# h3_* tables folded into the targets db (drop order: children before parents).
H3_TABLES = ("h3_cell_obs", "h3_cell_samples", "h3_cells", "h3_metadata")


def prepare_h3_tables(target_db: Path) -> None:
    """
    (Re)create the empty h3_* tables in the targets db before the bulk insert.

    Done with a plain sqlite3 connection (not DuckDB) because the WITHOUT ROWID
    layout must be created before DuckDB attaches and inserts in PK order.
    """
    sq = sqlite3.connect(target_db)
    sq.execute("PRAGMA journal_mode = MEMORY")
    sq.execute("PRAGMA synchronous = OFF")
    sq.execute("DROP INDEX IF EXISTS idx_h3_cells_h3")
    for table in H3_TABLES:
        sq.execute(f"DROP TABLE IF EXISTS {table}")
    sq.execute(
        """
        CREATE TABLE h3_cells (
            cell_ref INTEGER PRIMARY KEY,
            h3 INTEGER NOT NULL,
            region_code TEXT
        )
        """
    )
    sq.execute(
        """
        CREATE TABLE h3_cell_obs (
            cell_ref INTEGER NOT NULL,
            month INTEGER NOT NULL,
            species_id INTEGER NOT NULL,
            obs INTEGER NOT NULL,
            PRIMARY KEY (cell_ref, month, species_id)
        ) WITHOUT ROWID
        """
    )
    sq.execute(
        """
        CREATE TABLE h3_cell_samples (
            cell_ref INTEGER NOT NULL,
            month INTEGER NOT NULL,
            samples INTEGER NOT NULL,
            PRIMARY KEY (cell_ref, month)
        ) WITHOUT ROWID
        """
    )
    sq.execute(
        """
        CREATE TABLE h3_metadata (
            version TEXT,
            res INTEGER NOT NULL,
            generated_at TEXT NOT NULL
        )
        """
    )
    sq.commit()
    sq.close()


def build_aggregates(
    con: duckdb.DuckDBPyConnection,
    species_file: Path,
    sampling_file: Path,
    res: int,
) -> None:
    """Build region_cell_obs and region_cell_samples temp tables in DuckDB."""

    read_sampling = (
        f"read_csv('{sampling_file}', delim='\t', header=true, quote='', "
        "ignore_errors=true)"
    )
    read_species = (
        f"read_csv('{species_file}', delim='\t', header=true, quote='', "
        "ignore_errors=true)"
    )

    # State-or-country region code, mirroring generate_data.py's subnational1
    # detection ("US-CA" has a real subnational part; a bare "US" does not).
    region_expr = (
        "CASE WHEN MAX(\"STATE CODE\") IS NOT NULL AND MAX(\"STATE CODE\") != '' "
        "AND LENGTH(MAX(\"STATE CODE\")) > "
        "LENGTH(SPLIT_PART(MAX(\"STATE CODE\"), '-', 1)) + 1 "
        "THEN MAX(\"STATE CODE\") ELSE MAX(\"COUNTRY CODE\") END"
    )

    print("  - location_dim (locality -> cell, region) from sampling...")
    con.execute(
        f"""
        CREATE TEMP TABLE location_dim AS
        SELECT
            "LOCALITY ID" AS location_id,
            CAST(h3_latlng_to_cell(MAX("LATITUDE"), MAX("LONGITUDE"), {res}) AS BIGINT) AS cell,
            {region_expr} AS region_code
        FROM {read_sampling}
        WHERE "LATITUDE" IS NOT NULL AND "LONGITUDE" IS NOT NULL
        GROUP BY location_id
        """
    )

    print("  - samples per locality-month from sampling...")
    con.execute(
        f"""
        CREATE TEMP TABLE samples_loc AS
        SELECT
            "LOCALITY ID" AS location_id,
            EXTRACT(MONTH FROM CAST("OBSERVATION DATE" AS DATE)) AS month,
            COUNT(DISTINCT COALESCE(NULLIF("GROUP IDENTIFIER", ''), "SAMPLING EVENT IDENTIFIER")) AS samples
        FROM {read_sampling}
        GROUP BY location_id, month
        """
    )

    print("  - observations per locality-month-species from species file (large scan)...")
    con.execute(
        f"""
        CREATE TEMP TABLE obs_loc AS
        SELECT
            "LOCALITY ID" AS location_id,
            EXTRACT(MONTH FROM CAST("OBSERVATION DATE" AS DATE)) AS month,
            "SCIENTIFIC NAME" AS sci_name,
            COUNT(DISTINCT COALESCE(NULLIF("GROUP IDENTIFIER", ''), "SAMPLING EVENT IDENTIFIER")) AS obs
        FROM {read_species}
        GROUP BY location_id, month, sci_name
        """
    )

    print("  - cell_obs_agg (cell, month, species -> obs), full unsplit cells...")
    con.execute(
        """
        CREATE TEMP TABLE cell_obs_agg AS
        SELECT
            ld.cell,
            o.month,
            sp.id AS species_id,
            SUM(o.obs) AS obs
        FROM obs_loc o
        JOIN species sp ON o.sci_name = sp.sci_name
        JOIN location_dim ld ON o.location_id = ld.location_id
        WHERE ld.cell IS NOT NULL
        GROUP BY ld.cell, o.month, sp.id
        """
    )

    print("  - cell_samples_agg (cell, month -> samples), full unsplit cells...")
    con.execute(
        """
        CREATE TEMP TABLE cell_samples_agg AS
        SELECT
            ld.cell,
            s.month,
            SUM(s.samples) AS samples
        FROM samples_loc s
        JOIN location_dim ld ON s.location_id = ld.location_id
        WHERE ld.cell IS NOT NULL
        GROUP BY ld.cell, s.month
        """
    )

    # Per-cell activity by region, used only to pick the winning region per cell
    # (majority by checklist count). Months collapsed; one row per cell-region.
    print("  - cell_region_samples (cell, region -> samples) for winner pick...")
    con.execute(
        """
        CREATE TEMP TABLE cell_region_samples AS
        SELECT
            ld.cell,
            ld.region_code,
            SUM(s.samples) AS samples
        FROM samples_loc s
        JOIN location_dim ld ON s.location_id = ld.location_id
        WHERE ld.cell IS NOT NULL AND ld.region_code IS NOT NULL
        GROUP BY ld.cell, ld.region_code
        """
    )


def write_h3_tables(
    con: duckdb.DuckDBPyConnection,
    target_db: Path,
    version: str,
    res: int,
) -> dict:
    """
    Fill the (pre-created) h3_* tables in the targets db, attached as ``tdb``.

    The large obs/samples tables reference cells by a dense ``cell_ref`` (~3-byte
    int) assigned in H3 order; the 64-bit H3 index lives once per cell in
    h3_cells, with its winning region (majority by checklist count). A polygon
    query maps H3 cells -> cell_ref via h3_cells.h3, then filters by
    (cell_ref, month).
    """
    con.execute(
        """
        CREATE TEMP TABLE cell_dim AS
        SELECT
            CAST(ROW_NUMBER() OVER (ORDER BY h3) AS BIGINT) AS cell_ref,
            h3,
            region_code
        FROM (
            SELECT cell AS h3, region_code FROM (
                SELECT cell, region_code,
                       ROW_NUMBER() OVER (
                           PARTITION BY cell ORDER BY samples DESC, region_code
                       ) AS rn
                FROM cell_region_samples
            ) WHERE rn = 1
        )
        """
    )
    con.execute(
        """
        INSERT INTO tdb.h3_cells (cell_ref, h3, region_code)
        SELECT cell_ref, h3, region_code FROM cell_dim ORDER BY cell_ref
        """
    )
    con.execute(
        """
        INSERT INTO tdb.h3_cell_obs (cell_ref, month, species_id, obs)
        SELECT d.cell_ref, a.month, a.species_id, a.obs
        FROM cell_obs_agg a
        JOIN cell_dim d ON a.cell = d.h3
        ORDER BY d.cell_ref, a.month, a.species_id
        """
    )
    con.execute(
        """
        INSERT INTO tdb.h3_cell_samples (cell_ref, month, samples)
        SELECT d.cell_ref, a.month, a.samples
        FROM cell_samples_agg a
        JOIN cell_dim d ON a.cell = d.h3
        ORDER BY d.cell_ref, a.month
        """
    )

    orphan = con.execute(
        "SELECT COUNT(*) FROM (SELECT DISTINCT cell FROM cell_obs_agg "
        "WHERE cell NOT IN (SELECT h3 FROM cell_dim))"
    ).fetchone()[0]
    con.execute("DETACH tdb")

    # DuckDB's SQLite scanner cannot read back WITHOUT ROWID tables (it relies
    # on ROWID), so gather stats and finish with a plain sqlite3 connection.
    sq = sqlite3.connect(target_db)
    stats = {
        "cell_obs": sq.execute("SELECT COUNT(*) FROM h3_cell_obs").fetchone()[0],
        "cell_samples": sq.execute("SELECT COUNT(*) FROM h3_cell_samples").fetchone()[0],
        "cells": sq.execute("SELECT COUNT(*) FROM h3_cells").fetchone()[0],
        "orphan_cells": orphan,
    }
    sq.execute(
        "INSERT INTO h3_metadata (version, res, generated_at) VALUES (?, ?, ?)",
        (version, res, datetime.now(timezone.utc).isoformat()),
    )
    # h3 -> cell_ref lookup for translating polygon cells at query time.
    sq.execute("CREATE UNIQUE INDEX idx_h3_cells_h3 ON h3_cells(h3)")
    # ANALYZE just the new tables (no whole-db VACUUM -- targets db is large).
    sq.execute("ANALYZE h3_cells")
    sq.execute("ANALYZE h3_cell_obs")
    sq.execute("ANALYZE h3_cell_samples")
    sq.commit()
    sq.close()
    return stats


# ---------------------------------------------------------------------------
# Pack emission (reusable toolkit for the later per-region breakout).
#
# Not invoked by the main build, which produces a single master db. When the
# packs are broken out, slice the master db by cell_region.region_code under a
# pack region, pass the rows through assemble_cells + encode_pack, and gzip.
# ---------------------------------------------------------------------------

def assemble_cells(obs_rows: list[tuple], samp_rows: list[tuple]) -> list[tuple]:
    """
    Merge sorted obs/sample rows into per-cell structures.

    obs_rows:  (cell, species_id, month, obs)  sorted by cell, species_id, month
    samp_rows: (cell, month, samples)          sorted by cell, month
    Returns [(cell, samples12, [(species_id, obs12), ...]), ...] sorted by cell.
    """
    samples_by_cell: dict[int, list[int]] = {}
    for cell, month, samples in samp_rows:
        arr = samples_by_cell.get(cell)
        if arr is None:
            arr = [0] * 12
            samples_by_cell[cell] = arr
        arr[month - 1] = samples

    species_by_cell: dict[int, list[tuple]] = {}
    cur_cell = None
    cur_list = None
    cur_sp = None
    cur_obs = None
    for cell, species_id, month, obs in obs_rows:
        if cell != cur_cell:
            cur_cell = cell
            cur_list = []
            species_by_cell[cell] = cur_list
            cur_sp = None
        if species_id != cur_sp:
            cur_sp = species_id
            cur_obs = [0] * 12
            cur_list.append((species_id, cur_obs))
        cur_obs[month - 1] = obs

    all_cells = sorted(set(samples_by_cell) | set(species_by_cell))
    result = []
    for cell in all_cells:
        samples = samples_by_cell.get(cell, [0] * 12)
        species = species_by_cell.get(cell, [])
        result.append((cell, samples, species))
    return result


def encode_region_pack(h3_db: Path, region: str, res: int) -> bytes:
    """
    Build a single region's gzipped binary pack from the master db, selecting
    whole cells whose winning region falls under ``region`` (prefix match).
    Provided for the later breakout; not used by the main build.
    """
    con = sqlite3.connect(f"file:{h3_db}?mode=ro", uri=True)
    ref_filter = (
        "cell_ref IN (SELECT cell_ref FROM h3_cells "
        "WHERE region_code = ? OR region_code LIKE ? || '-%')"
    )
    # Emit the app-facing H3 ids (not the internal cell_ref).
    obs_rows = con.execute(
        f"SELECT c.h3, o.species_id, o.month, o.obs "
        f"FROM h3_cell_obs o JOIN h3_cells c ON c.cell_ref = o.cell_ref "
        f"WHERE o.{ref_filter} ORDER BY c.h3, o.species_id, o.month",
        (region, region),
    ).fetchall()
    samp_rows = con.execute(
        f"SELECT c.h3, s.month, s.samples "
        f"FROM h3_cell_samples s JOIN h3_cells c ON c.cell_ref = s.cell_ref "
        f"WHERE s.{ref_filter} ORDER BY c.h3, s.month",
        (region, region),
    ).fetchall()
    con.close()
    cells = assemble_cells(obs_rows, samp_rows)
    return gzip.compress(encode_pack(cells, res), compresslevel=9)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def build(
    species_file: Path,
    sampling_file: Path,
    target_db: Path,
    version: str,
    res: int,
    memory_limit: Optional[str],
    threads: Optional[int],
    temp_dir: Optional[Path],
) -> None:
    start = time.time()

    config = {}
    duck_temp = (temp_dir or (Path.cwd() / ".tmp")).resolve()
    duck_temp.mkdir(parents=True, exist_ok=True)
    config["temp_directory"] = str(duck_temp)
    if threads:
        config["threads"] = threads

    con = duckdb.connect(config=config)
    if memory_limit:
        con.execute(f"SET memory_limit = '{memory_limit}'")
    con.execute("SET preserve_insertion_order = false")
    con.execute("INSTALL h3 FROM community; LOAD h3;")
    con.execute("INSTALL sqlite; LOAD sqlite;")

    print(f"H3 resolution: {res}")
    print(f"Species file:  {species_file}")
    print(f"Sampling file: {sampling_file}")
    print(f"Targets db:    {target_db}")
    print(f"Temp dir:      {duck_temp}")

    print("\nStep 1/3: Preparing h3_ tables + loading species...")
    t = time.time()
    prepare_h3_tables(target_db)
    con.execute(f"ATTACH '{target_db}' AS tdb (TYPE SQLITE)")
    # sci_name joins the source SCIENTIFIC NAME; ids match the targets species.
    con.execute("CREATE TABLE species AS SELECT id, sci_name, code FROM tdb.species")
    n_species = con.execute("SELECT COUNT(*) FROM species").fetchone()[0]
    print(f"  {n_species:,} species ({format_duration(time.time() - t)})")

    print("\nStep 2/3: Building cell aggregates from source...")
    t = time.time()
    build_aggregates(con, species_file, sampling_file, res)
    n_loc = con.execute("SELECT COUNT(*) FROM location_dim").fetchone()[0]
    n_obs = con.execute("SELECT COUNT(*) FROM cell_obs_agg").fetchone()[0]
    print(
        f"  {n_loc:,} localities, {n_obs:,} cell-month-species rows "
        f"({format_duration(time.time() - t)})"
    )

    print("\nStep 3/3: Writing h3_ tables into targets db...")
    t = time.time()
    stats = write_h3_tables(con, target_db, version, res)
    con.close()
    print(
        f"  cells: {stats['cells']:,}  h3_cell_obs: {stats['cell_obs']:,}  "
        f"h3_cell_samples: {stats['cell_samples']:,}"
    )
    if stats["orphan_cells"]:
        print(f"  note: {stats['orphan_cells']:,} cells have obs but no region winner")
    print(f"  ({format_duration(time.time() - t)})")

    print(f"\nDone in {format_duration(time.time() - start)}")


def main():
    parser = argparse.ArgumentParser(
        description="Fold H3-binned eBird data into a targets database."
    )
    parser.add_argument("species_file", type=Path)
    parser.add_argument("sampling_file", type=Path)
    parser.add_argument(
        "target_db",
        type=Path,
        help="Existing targets SQLite db to fold h3_ tables into (must have a species table)",
    )
    parser.add_argument("--version", type=str, required=True, help="e.g. may-2026")
    parser.add_argument("--res", type=int, default=6)
    parser.add_argument("--memory-limit", type=str)
    parser.add_argument("--threads", type=int)
    parser.add_argument("--temp-dir", type=Path)
    args = parser.parse_args()

    for f in (args.species_file, args.sampling_file):
        if not f.exists():
            print(f"Error: file not found: {f}", file=sys.stderr)
            sys.exit(1)
    if not args.target_db.exists():
        print(
            f"Error: targets db not found: {args.target_db}\n"
            "Run the Build Database step first.",
            file=sys.stderr,
        )
        sys.exit(1)

    build(
        species_file=args.species_file,
        sampling_file=args.sampling_file,
        target_db=args.target_db,
        version=args.version,
        res=args.res,
        memory_limit=args.memory_limit,
        threads=args.threads,
        temp_dir=args.temp_dir,
    )


if __name__ == "__main__":
    main()
