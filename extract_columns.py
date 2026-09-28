#!/usr/bin/env python3
"""
Extract only the columns needed for generate_data.py from a gzipped eBird file.

This reduces the file size significantly by keeping only essential columns,
making subsequent processing faster and requiring less disk space.

Keeps all complete checklists so hotspot and region aggregates can both be
built from the same extracted file.

Scientific names are normalized to the species level using the eBird taxonomy:
species are kept as-is, other taxa (subspecies groups, forms, etc.) are rolled
up to their reportAs species, and taxa with neither (spuhs, slashes, hybrids,
undescribed forms) are dropped. Exotic ("X") records are also dropped.

Usage:
    python extract_columns.py <input.txt.gz> <output.tsv>

Example:
    python extract_columns.py ebd_relDec-2025.txt.gz ebd_filtered.tsv
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

import requests

from utils import format_duration, format_size

# Columns needed by generate_data.py
REQUIRED_COLUMNS = [
    "LOCALITY ID",
    "OBSERVATION DATE",
    "SAMPLING EVENT IDENTIFIER",
    "GROUP IDENTIFIER",
    "SCIENTIFIC NAME",
]

TAXONOMY_URL = "https://api.ebird.org/v2/ref/taxonomy/ebird?fmt=json"


def load_species_names() -> dict:
    """
    Map each countable taxon's scientific name to its species-level scientific
    name. Species map to themselves; other taxa map to their reportAs species.
    """
    response = requests.get(TAXONOMY_URL, timeout=60)
    response.raise_for_status()
    taxonomy = response.json()

    sci_name_by_code = {t["speciesCode"]: t["sciName"] for t in taxonomy}
    species_names = {}
    for t in taxonomy:
        if t["category"] == "species":
            species_names[t["sciName"]] = t["sciName"]
        elif t.get("reportAs"):
            species_names[t["sciName"]] = sci_name_by_code[t["reportAs"]]
    return species_names


def extract_columns(input_file: Path, output_file: Path) -> None:
    """
    Stream through gzipped input and write filtered TSV output.

    Uses pigz for parallel decompression and simple string splitting
    for faster parsing.

    Args:
        input_file: Path to gzipped eBird species file
        output_file: Path to output TSV file
    """
    start_time = time.time()
    rows_processed = 0
    rows_skipped = 0

    print(f"Input: {input_file}")
    print(f"Output: {output_file}")
    print(f"Extracting columns: {', '.join(REQUIRED_COLUMNS)}")

    species_names = load_species_names()
    print(f"Loaded {len(species_names):,} taxon names from eBird taxonomy")
    print()

    # Use pigz for parallel decompression (much faster than Python's gzip)
    proc = subprocess.Popen(
        ["pigz", "-dc", str(input_file)],
        stdout=subprocess.PIPE,
        bufsize=1024 * 1024,  # 1MB buffer
    )

    with open(output_file, "w", encoding="utf-8") as outfile:
        # Read and parse header line
        header_line = proc.stdout.readline().decode("utf-8", errors="replace")
        header_cols = header_line.rstrip("\n").split("\t")

        # Build index mapping for required columns
        try:
            col_indices = [header_cols.index(col) for col in REQUIRED_COLUMNS]
        except ValueError as e:
            print(f"Error: Missing column in input file: {e}", file=sys.stderr)
            proc.terminate()
            sys.exit(1)

        # Find indices for filter columns
        all_species_idx = header_cols.index("ALL SPECIES REPORTED")
        exotic_idx = header_cols.index("EXOTIC CODE")
        sci_name_idx = header_cols.index("SCIENTIFIC NAME")

        # Write header
        outfile.write("\t".join(REQUIRED_COLUMNS) + "\n")

        # Process data rows
        for line_bytes in proc.stdout:
            cols = line_bytes.decode("utf-8", errors="replace").rstrip("\n").split("\t")

            # Filter: complete checklists, non-escapees, species-level taxa only
            if cols[all_species_idx] != "1" or cols[exotic_idx] == "X":
                rows_skipped += 1
                continue

            species_name = species_names.get(cols[sci_name_idx])
            if species_name is None:
                rows_skipped += 1
                continue
            cols[sci_name_idx] = species_name

            # Extract only required columns
            outfile.write("\t".join(cols[i] for i in col_indices) + "\n")
            rows_processed += 1

            # Progress update every 1 million rows
            if rows_processed % 1_000_000 == 0:
                elapsed = time.time() - start_time
                rate = rows_processed / elapsed
                print(
                    f"  Processed {rows_processed:,} rows "
                    f"({format_duration(elapsed)}, {rate:,.0f} rows/sec)"
                )

    proc.wait()

    # Final stats
    elapsed = time.time() - start_time
    output_size = output_file.stat().st_size
    total_rows = rows_processed + rows_skipped

    print()
    print("=" * 50)
    print(f"Total rows read: {total_rows:,}")
    print(f"Rows written: {rows_processed:,}")
    print(f"Rows skipped (incomplete/exotic/non-species): {rows_skipped:,}")
    print(f"Output size: {format_size(output_size)}")
    print(f"Total time: {format_duration(elapsed)}")
    print(f"\nOutput written to: {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Extract required columns from gzipped eBird file."
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Path to gzipped eBird species file (.txt.gz)",
    )
    parser.add_argument(
        "output_file",
        type=Path,
        help="Path to output TSV file",
    )

    args = parser.parse_args()

    if not args.input_file.exists():
        print(f"Error: Input file not found: {args.input_file}", file=sys.stderr)
        sys.exit(1)

    extract_columns(args.input_file, args.output_file)


if __name__ == "__main__":
    main()
