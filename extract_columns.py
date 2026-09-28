#!/usr/bin/env python3
"""
Extract only the columns needed for generate_data.py from a gzipped eBird file.

This reduces the file size significantly by keeping only essential columns,
making subsequent processing faster and requiring less disk space.

Keeps all complete checklists so hotspot and region aggregates can both be
built from the same extracted file.

Drops exotic ("X") records and spuhs, slashes, and hybrids. Sub-species taxa
(issf, form, intergrade, domestic) are kept; the EBD reports them under their
parent species' SCIENTIFIC NAME. Any that don't roll up to a species (e.g.
undescribed forms) are dropped when generate_data.py joins to the taxonomy.

Also writes a small taxa file next to the output listing the distinct taxa
seen, which generate_data.py uses to pick the matching eBird taxonomy version.

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

from utils import format_duration, format_size, taxa_file_path

# Columns needed by generate_data.py
REQUIRED_COLUMNS = [
    "LOCALITY ID",
    "OBSERVATION DATE",
    "SAMPLING EVENT IDENTIFIER",
    "GROUP IDENTIFIER",
    "SCIENTIFIC NAME",
]

# Categories that can roll up to a species
VALID_CATEGORIES = ("species", "issf", "form", "intergrade", "domestic")


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
    print()

    # category -> {scientific name: taxonomic order}, for the taxa file
    taxa_by_category = {category: {} for category in VALID_CATEGORIES}

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
        category_idx = header_cols.index("CATEGORY")
        sci_name_idx = header_cols.index("SCIENTIFIC NAME")
        taxon_order_idx = header_cols.index("TAXONOMIC ORDER")

        # Write header
        outfile.write("\t".join(REQUIRED_COLUMNS) + "\n")

        # Process data rows
        for line_bytes in proc.stdout:
            cols = line_bytes.decode("utf-8", errors="replace").rstrip("\n").split("\t")

            # Filter: complete checklists, non-escapees, valid categories only
            if cols[all_species_idx] != "1" or cols[exotic_idx] == "X":
                rows_skipped += 1
                continue

            taxa = taxa_by_category.get(cols[category_idx])
            if taxa is None:
                rows_skipped += 1
                continue

            sci_name = cols[sci_name_idx]
            if sci_name not in taxa:
                taxa[sci_name] = cols[taxon_order_idx]

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

    taxa_file = taxa_file_path(output_file)
    with open(taxa_file, "w", encoding="utf-8") as f:
        f.write("CATEGORY\tSCIENTIFIC NAME\tTAXONOMIC ORDER\n")
        for category, taxa in taxa_by_category.items():
            for sci_name, taxon_order in taxa.items():
                f.write(f"{category}\t{sci_name}\t{taxon_order}\n")

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
    print(f"Taxa written to: {taxa_file}")


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
