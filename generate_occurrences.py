#!/usr/bin/env python3
"""
Build occurrences.db — a compact companion database of species-occurrence
frequencies per hotspot and per H3 cell, derived from targets.db.

It currently powers the web app's "Best Hotspots" tool (lifer targets), whose
core query is:

    lifers[loc] = qCount[loc] - seenCount[loc]

where qCount (species above a frequency threshold per location) is
user-independent and precomputed here for a fixed set of threshold "buckets",
so the API answers worldwide queries by scanning only the user's seen species.

Output tables: metadata, species, loc_meta, loc_species, loc_qcount,
zone_meta, zone_species, zone_qcount — plus `blob_cache`, the same data
pre-packed as little-endian typed-array BLOBs so the Node API loads its
in-memory index with a few memcpy-speed reads (~0.5 s) instead of iterating
tens of millions of rows (~60 s).

BLOB CONTRACT (consumed by web/api/lib/lifers-index.ts — keep in sync):
  loc:samples   int32[numLocs]            zone:{res}:samples  int32[size]
  loc:lat       float32[numLocs]          zone:{res}:lat      float32[size]
  loc:lng       float32[numLocs]          zone:{res}:lng      float32[size]
  loc:qcount    int32[buckets * numLocs]  zone:{res}:qcount   int32[buckets * size]
  loc:spOff     int32[maxSpeciesId + 2]   zone:{res}:spOff    int32[maxSpeciesId + 2]
  loc:csrRef    int32[totalRows]          zone:{res}:csrRef   int32[totalRows]
  loc:csrLvl    uint8[totalRows]          zone:{res}:csrLvl   uint8[totalRows]
                                          zone:{res}:h3       int64[size]
  qcount is bucket-major (bucket b occupies [b*n, (b+1)*n)); the CSR is sorted
  by (species_id, ref); size = max cell_ref + 1 (refs are dense per res).

The zone (H3) tables are rolled up here from the finest resolution present in
targets.db (res 6) — targets.db does not need to carry the coarser resolutions.

Usage:
    python3 generate_occurrences.py targets.db occurrences.db [--zone-res 3,4]
"""

import argparse
import json
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import numpy as np

# Rows below these floors are noise and are dropped.
MIN_SCORE = 0.01  # 1% adjusted frequency
MIN_CHECKLISTS = 10  # per-location total checklists

# Frequency thresholds a user can pick from (adjusted frequency, 0-1).
# qCount is precomputed per location/cell for each of these.
FREQUENCY_BUCKETS = [0.01, 0.05, 0.1, 0.2, 0.3, 0.5]

# Grid resolutions served by the web UI. Finer than 4 balloons the in-memory
# index (res 5/6 were ~85% of zone rows) for zoom levels the map never uses.
DEFAULT_ZONE_RESOLUTIONS = [3, 4]


def log(msg: str):
    print(f"[occurrences] {datetime.now().strftime('%H:%M:%S')} {msg}")
    sys.stdout.flush()


def bucket_level_expr(score_expr: str) -> str:
    """(# thresholds <= score) - 1, i.e. the highest bucket index met."""
    return "(" + " + ".join(f"({score_expr} >= {t})" for t in FREQUENCY_BUCKETS) + " - 1)"


# --- Row tables (plain SQLite; the SQL mirrors the original TS build) ---------


def zone_parent_map(src: Path, zone_resolutions: list[int]) -> tuple[int, list[tuple]]:
    """
    Child -> parent mapping for the H3 roll-up, computed with DuckDB's h3
    extension (both connections only read src, so no lock contention).

    Returns (finest_res, rows) where rows are
    (target_res, child_cell_ref, parent_h3, parent_lat, parent_lng)
    for every h3_cells row at the finest resolution in targets.db.
    """
    con = duckdb.connect()
    con.execute("INSTALL h3 FROM community; LOAD h3;")
    con.execute("INSTALL sqlite; LOAD sqlite;")
    con.execute(f"ATTACH '{src}' AS t (TYPE SQLITE, READ_ONLY)")
    finest = con.execute("SELECT MAX(res) FROM t.h3_cells").fetchone()[0]
    if finest is None:
        raise SystemExit("targets.db has no h3_cells rows — run the Build H3 step first")
    bad = [r for r in zone_resolutions if r >= finest]
    if bad:
        raise SystemExit(f"--zone-res {bad} must be coarser than the source resolution {finest}")
    rows: list[tuple] = []
    for res in zone_resolutions:
        rows.extend(
            con.execute(
                f"""
                SELECT {res}, cell_ref,
                       CAST(h3_cell_to_parent(h3, {res}) AS BIGINT),
                       h3_cell_to_lat(h3_cell_to_parent(h3, {res})),
                       h3_cell_to_lng(h3_cell_to_parent(h3, {res}))
                FROM t.h3_cells WHERE res = {finest}
                """
            ).fetchall()
        )
    con.close()
    return finest, rows


def build_tables(src: Path, out: Path, zone_resolutions: list[int]):
    finest_res, h3map_rows = zone_parent_map(src, zone_resolutions)
    db = sqlite3.connect(f"file:{out}?mode=rwc", uri=True)
    db.executescript(
        """
        PRAGMA journal_mode = OFF;
        PRAGMA synchronous = OFF;
        PRAGMA temp_store = MEMORY;
        """
    )
    db.execute("PRAGMA cache_size = -1000000")
    db.execute(f"ATTACH DATABASE 'file:{src}?immutable=1' AS t")

    db.executescript(
        """
        CREATE TABLE metadata (
          version TEXT, version_year TEXT, version_month TEXT,
          generated_at TEXT, buckets TEXT,
          min_score REAL, min_checklists INTEGER
        );

        CREATE TABLE species (
          id INTEGER PRIMARY KEY,
          code TEXT NOT NULL,
          name TEXT NOT NULL,
          sci_name TEXT NOT NULL,
          sci_lower TEXT NOT NULL,
          name_lower TEXT NOT NULL,
          taxon_order INTEGER NOT NULL
        );

        CREATE TABLE loc_meta (
          loc_ref INTEGER PRIMARY KEY,
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
          bucket_level INTEGER NOT NULL,
          PRIMARY KEY (species_id, loc_ref)
        ) WITHOUT ROWID;

        CREATE TABLE loc_qcount (
          bucket INTEGER NOT NULL,
          loc_ref INTEGER NOT NULL,
          q_count INTEGER NOT NULL,
          PRIMARY KEY (bucket, loc_ref)
        ) WITHOUT ROWID;

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
        """
    )

    meta = db.execute(
        "SELECT version, version_year, version_month FROM t.metadata"
    ).fetchone()
    db.execute(
        "INSERT INTO metadata VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            meta[0],
            meta[1],
            meta[2],
            datetime.now(timezone.utc).isoformat(),
            json.dumps(FREQUENCY_BUCKETS),
            MIN_SCORE,
            MIN_CHECKLISTS,
        ),
    )

    log("Copying species...")
    db.executescript(
        """
        INSERT INTO species (id, code, name, sci_name, sci_lower, name_lower, taxon_order)
        SELECT id, code, name, sci_name, lower(sci_name), lower(name), taxon_order
        FROM t.species;
        CREATE INDEX idx_species_sci ON species(sci_lower);
        CREATE INDEX idx_species_name ON species(name_lower);
        CREATE INDEX idx_species_code ON species(code);
        """
    )

    # samples is constant per location in year_obs; MAX is the location total.
    # loc_ref is a dense integer id, assigned by descending sample count.
    log("Building loc_meta...")
    db.execute(
        f"""
        INSERT INTO loc_meta
          (loc_ref, location_id, name, lat, lng, country_code, subnational1_code,
           subnational2_code, region_code, samples)
        SELECT
          ROW_NUMBER() OVER (ORDER BY ls.total_samples DESC, ls.location_id) - 1,
          h.id, h.name, h.lat, h.lng, h.country_code, h.subnational1_code,
          h.subnational2_code, h.region_code, ls.total_samples
        FROM (
          SELECT location_id, MAX(samples) AS total_samples
          FROM t.year_obs
          GROUP BY location_id
          HAVING total_samples >= {MIN_CHECKLISTS}
        ) ls
        JOIN t.hotspots h ON h.id = ls.location_id
        """
    )
    db.executescript(
        """
        CREATE UNIQUE INDEX idx_loc_meta_locid ON loc_meta(location_id);
        CREATE INDEX idx_loc_meta_region ON loc_meta(region_code);
        """
    )
    log(f"  loc_meta: {db.execute('SELECT COUNT(*) FROM loc_meta').fetchone()[0]}")

    log("Building loc_species (the big one)...")
    db.execute(
        f"""
        INSERT INTO loc_species (species_id, loc_ref, bucket_level)
        SELECT yo.species_id, m.loc_ref, {bucket_level_expr('yo.score')}
        FROM t.year_obs yo
        JOIN loc_meta m ON m.location_id = yo.location_id
        WHERE yo.score >= {MIN_SCORE}
        """
    )
    log(f"  loc_species: {db.execute('SELECT COUNT(*) FROM loc_species').fetchone()[0]}")

    log("Building loc_qcount...")
    for bucket in range(len(FREQUENCY_BUCKETS)):
        db.execute(
            """
            INSERT INTO loc_qcount (bucket, loc_ref, q_count)
            SELECT ?, loc_ref, COUNT(*) FROM loc_species
            WHERE bucket_level >= ? GROUP BY loc_ref
            """,
            (bucket, bucket),
        )

    # Zones: rolled up from the finest resolution in targets.db (res 6) to each
    # coarser resolution the UI serves. targets.db carries only res 6.
    log(f"Loading H3 parent map (res {finest_res} -> {zone_resolutions}, {len(h3map_rows)} rows)...")
    db.execute("CREATE TEMP TABLE h3map (res INTEGER, child_ref INTEGER, parent INTEGER, lat REAL, lng REAL)")
    db.executemany("INSERT INTO h3map VALUES (?, ?, ?, ?, ?)", h3map_rows)
    db.execute("CREATE INDEX temp.idx_h3map_child ON h3map(child_ref)")

    # Year-level totals per finest-res child cell (sum across months).
    db.execute(
        f"""
        CREATE TEMP TABLE child_samples AS
        SELECT cell_ref, SUM(samples) AS s
        FROM t.h3_cell_samples WHERE res = {finest_res}
        GROUP BY cell_ref
        """
    )

    log("Building zone_meta (rolling up)...")
    db.execute(
        f"""
        INSERT INTO zone_meta (res, cell_ref, h3, lat, lng, samples)
        SELECT p.res,
               ROW_NUMBER() OVER (PARTITION BY p.res ORDER BY p.parent) - 1,
               p.parent, p.lat, p.lng, p.total_samples
        FROM (
          SELECT m.res, m.parent, MIN(m.lat) AS lat, MIN(m.lng) AS lng,
                 SUM(cs.s) AS total_samples
          FROM h3map m JOIN child_samples cs ON cs.cell_ref = m.child_ref
          GROUP BY m.res, m.parent
          HAVING total_samples >= {MIN_CHECKLISTS}
        ) p
        """
    )
    log(f"  zone_meta: {db.execute('SELECT COUNT(*) FROM zone_meta').fetchone()[0]}")

    # child_ref (finest res) -> zone cell_ref, per target resolution.
    db.execute("CREATE INDEX idx_zone_meta_h3 ON zone_meta(res, h3)")
    db.execute(
        """
        CREATE TEMP TABLE child_zone AS
        SELECT m.res, m.child_ref, z.cell_ref, z.samples
        FROM h3map m JOIN zone_meta z ON z.res = m.res AND z.h3 = m.parent
        """
    )
    db.execute("CREATE INDEX temp.idx_child_zone ON child_zone(child_ref)")

    log("Building zone_species (aggregating H3 months + children)...")
    db.execute(
        f"""
        INSERT INTO zone_species (res, species_id, cell_ref, bucket_level)
        SELECT cz.res, o.species_id, cz.cell_ref,
               {bucket_level_expr('(SUM(o.obs) * 1.0 / cz.samples)')}
        FROM t.h3_cell_obs o
        JOIN child_zone cz ON cz.child_ref = o.cell_ref
        WHERE o.res = {finest_res}
        GROUP BY cz.res, cz.cell_ref, o.species_id
        HAVING (SUM(o.obs) * 1.0 / cz.samples) >= {MIN_SCORE}
        """
    )
    db.execute("DROP INDEX idx_zone_meta_h3")
    log(f"  zone_species: {db.execute('SELECT COUNT(*) FROM zone_species').fetchone()[0]}")

    log("Building zone_qcount...")
    for bucket in range(len(FREQUENCY_BUCKETS)):
        db.execute(
            """
            INSERT INTO zone_qcount (res, bucket, cell_ref, q_count)
            SELECT res, ?, cell_ref, COUNT(*) FROM zone_species
            WHERE bucket_level >= ? GROUP BY res, cell_ref
            """,
            (bucket, bucket),
        )

    log("Analyzing...")
    db.execute("ANALYZE main")  # not bare ANALYZE — that also hits read-only attached t
    db.commit()
    db.close()


# --- Blob cache (DuckDB reads -> numpy arrays -> SQLite blobs) ----------------


def as_i32(a) -> np.ndarray:
    return np.ascontiguousarray(a, dtype="<i4")


def csr_blobs(sid: np.ndarray, ref: np.ndarray, lvl: np.ndarray, max_sid: int):
    """CSR arrays from rows already sorted by (species_id, ref)."""
    counts = np.bincount(sid, minlength=max_sid + 1)
    sp_off = np.zeros(max_sid + 2, dtype="<i4")
    sp_off[1:] = np.cumsum(counts)
    return sp_off, as_i32(ref), np.ascontiguousarray(lvl, dtype="u1")


def dense(idx: np.ndarray, values: np.ndarray, size: int, dtype: str) -> np.ndarray:
    out = np.zeros(size, dtype=dtype)
    out[idx] = values
    return out


def pack_blobs(out: Path):
    # Phase 1: read everything into arrays via DuckDB. The SQLite writer opens
    # only after DuckDB detaches — mixing a live writer (journal OFF => exclusive
    # lock) with DuckDB reads on the same file fails with "database is locked".
    con = duckdb.connect()
    con.execute("INSTALL sqlite; LOAD sqlite;")
    con.execute(f"ATTACH '{out}' AS occ (TYPE SQLITE, READ_ONLY)")

    blobs: list = []

    def put(key: str, arr: np.ndarray):
        blobs.append((key, arr.tobytes()))

    n_buckets = len(FREQUENCY_BUCKETS)

    # Hotspots
    n = con.execute("SELECT COUNT(*) FROM occ.loc_meta").fetchone()[0]
    log(f"packing hotspots ({n} locations)...")
    m = con.execute(
        "SELECT loc_ref, COALESCE(lat, 0) lat, COALESCE(lng, 0) lng, samples FROM occ.loc_meta"
    ).fetchnumpy()
    refs = np.asarray(m["loc_ref"], dtype=np.int64)
    put("loc:samples", dense(refs, np.asarray(m["samples"]), n, "<i4"))
    put("loc:lat", dense(refs, np.asarray(m["lat"]), n, "<f4"))
    put("loc:lng", dense(refs, np.asarray(m["lng"]), n, "<f4"))

    q = con.execute("SELECT bucket, loc_ref, q_count FROM occ.loc_qcount").fetchnumpy()
    put(
        "loc:qcount",
        dense(
            np.asarray(q["bucket"], dtype=np.int64) * n + np.asarray(q["loc_ref"], dtype=np.int64),
            np.asarray(q["q_count"]),
            n_buckets * n,
            "<i4",
        ),
    )

    max_sid = con.execute("SELECT MAX(species_id) FROM occ.loc_species").fetchone()[0]
    rows = con.execute(
        "SELECT species_id, loc_ref, bucket_level FROM occ.loc_species ORDER BY species_id, loc_ref"
    ).fetchnumpy()
    sp_off, csr_ref, csr_lvl = csr_blobs(
        np.asarray(rows["species_id"], dtype=np.int64),
        np.asarray(rows["loc_ref"]),
        np.asarray(rows["bucket_level"]),
        max_sid,
    )
    put("loc:spOff", sp_off)
    put("loc:csrRef", csr_ref)
    put("loc:csrLvl", csr_lvl)
    del m, q, rows

    # Zones, per resolution
    res_list = [r[0] for r in con.execute("SELECT DISTINCT res FROM occ.zone_meta ORDER BY res").fetchall()]
    for res in res_list:
        size = con.execute("SELECT MAX(cell_ref) + 1 FROM occ.zone_meta WHERE res = ?", [res]).fetchone()[0]
        log(f"packing zones res {res} ({size} cell refs)...")
        m = con.execute(
            """SELECT cell_ref, COALESCE(h3, 0) h3, COALESCE(lat, 0) lat,
                      COALESCE(lng, 0) lng, samples
               FROM occ.zone_meta WHERE res = ?""",
            [res],
        ).fetchnumpy()
        refs = np.asarray(m["cell_ref"], dtype=np.int64)
        put(f"zone:{res}:samples", dense(refs, np.asarray(m["samples"]), size, "<i4"))
        put(f"zone:{res}:lat", dense(refs, np.asarray(m["lat"]), size, "<f4"))
        put(f"zone:{res}:lng", dense(refs, np.asarray(m["lng"]), size, "<f4"))
        put(f"zone:{res}:h3", dense(refs, np.asarray(m["h3"]), size, "<i8"))

        q = con.execute(
            "SELECT bucket, cell_ref, q_count FROM occ.zone_qcount WHERE res = ?", [res]
        ).fetchnumpy()
        put(
            f"zone:{res}:qcount",
            dense(
                np.asarray(q["bucket"], dtype=np.int64) * size
                + np.asarray(q["cell_ref"], dtype=np.int64),
                np.asarray(q["q_count"]),
                n_buckets * size,
                "<i4",
            ),
        )

        max_sid = con.execute("SELECT MAX(species_id) FROM occ.zone_species WHERE res = ?", [res]).fetchone()[0]
        rows = con.execute(
            """SELECT species_id, cell_ref, bucket_level FROM occ.zone_species
               WHERE res = ? ORDER BY species_id, cell_ref""",
            [res],
        ).fetchnumpy()
        sp_off, csr_ref, csr_lvl = csr_blobs(
            np.asarray(rows["species_id"], dtype=np.int64),
            np.asarray(rows["cell_ref"]),
            np.asarray(rows["bucket_level"]),
            max_sid,
        )
        put(f"zone:{res}:spOff", sp_off)
        put(f"zone:{res}:csrRef", csr_ref)
        put(f"zone:{res}:csrLvl", csr_lvl)
        del m, q, rows

    con.close()

    # Phase 2: write the collected blobs.
    log(f"writing {len(blobs)} blobs...")
    sq = sqlite3.connect(out)
    sq.execute("PRAGMA journal_mode = OFF")
    sq.execute("PRAGMA synchronous = OFF")
    sq.execute("DROP TABLE IF EXISTS blob_cache")
    sq.execute("CREATE TABLE blob_cache (key TEXT PRIMARY KEY, data BLOB NOT NULL) WITHOUT ROWID")
    sq.executemany("INSERT INTO blob_cache (key, data) VALUES (?, ?)", blobs)
    sq.commit()
    sq.close()


def main():
    parser = argparse.ArgumentParser(description="Build occurrences.db from targets.db")
    parser.add_argument("targets_db", type=Path)
    parser.add_argument("occurrences_db", type=Path)
    parser.add_argument(
        "--zone-res",
        type=str,
        default=",".join(map(str, DEFAULT_ZONE_RESOLUTIONS)),
        help="Comma-separated H3 grid resolutions to emit (rolled up from targets.db's finest)",
    )
    args = parser.parse_args()
    try:
        zone_resolutions = sorted({int(r) for r in args.zone_res.split(",")})
    except ValueError:
        print("Error: --zone-res must be comma-separated integers")
        sys.exit(1)

    if not args.targets_db.exists():
        print(f"Error: targets db not found: {args.targets_db}")
        sys.exit(1)
    for suffix in ["", "-wal", "-shm"]:
        p = Path(str(args.occurrences_db) + suffix)
        if p.exists():
            p.unlink()

    start = time.time()
    log(f"Source: {args.targets_db}")
    log(f"Output: {args.occurrences_db}")
    build_tables(args.targets_db, args.occurrences_db, zone_resolutions)
    log("Packing blob cache...")
    pack_blobs(args.occurrences_db)
    log(f"Done in {time.time() - start:.0f}s: {args.occurrences_db}")


if __name__ == "__main__":
    main()
