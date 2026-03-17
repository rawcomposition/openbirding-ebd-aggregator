#!/usr/bin/env python3
"""
Interactive CLI for eBird Basic Dataset Aggregator.

Downloads and processes eBird Basic Dataset to build a SQLite database
of bird observation statistics.

Usage:
    python cli.py
"""

import subprocess
import sys
from datetime import datetime
from pathlib import Path

import requests

try:
    from simple_term_menu import TerminalMenu
except ImportError:
    print("Error: 'simple-term-menu' library required.")
    print("Install with: python3 -m pip install simple-term-menu")
    sys.exit(1)

from utils import load_env_file

# Get script directory for relative paths
SCRIPT_DIR = Path(__file__).parent.resolve()


def get_output_paths(env_vars: dict) -> tuple[Path, Path, Path]:
    """
    Get dataset, output, and packs directories based on OUTPUT_PATH env var.

    Returns:
        Tuple of (datasets_dir, outputs_dir, packs_dir)
    """
    output_path = env_vars.get("OUTPUT_PATH", "")
    if output_path:
        base_dir = (SCRIPT_DIR / output_path).resolve()
    else:
        base_dir = SCRIPT_DIR

    datasets_dir = base_dir / "datasets"
    outputs_dir = base_dir / "output"
    packs_dir = outputs_dir / "packs"

    return datasets_dir, outputs_dir, packs_dir


def send_notification(
    topic: str, title: str, message: str, success: bool = True
) -> None:
    """Send a notification to an ntfy.sh topic."""
    emoji = "\u2705" if success else "\u274c"
    try:
        requests.post(
            f"https://ntfy.sh/{topic}",
            data=f"{message} {emoji}".encode("utf-8"),
            headers={"Title": title},
            timeout=10,
        )
    except requests.RequestException:
        pass  # Silently ignore notification failures


def get_month_options() -> list[tuple[str, str, str]]:
    """
    Get the 3 most recent months as options.
    Returns list of (display_name, month_abbrev, year) tuples.
    """
    from dateutil.relativedelta import relativedelta

    options = []
    now = datetime.now()

    for i in range(3):
        date = now - relativedelta(months=i)
        month_abbrev = date.strftime("%b")
        year = date.strftime("%Y")
        options.append((f"{month_abbrev} {year}", month_abbrev, year))

    return options


def get_file_paths(
    month_abbrev: str,
    year: str,
    datasets_dir: Path,
    outputs_dir: Path
) -> dict:
    """
    Get all file paths for a given month/year.
    """
    month_lower = month_abbrev.lower()
    base_name = f"ebd-{month_lower}-{year}"
    sampling_base = f"ebd-sampling-{month_lower}-{year}"
    ebird_release = f"ebd_rel{month_abbrev}-{year}"
    sampling_release = f"ebd_sampling_rel{month_abbrev}-{year}"

    return {
        # Species observations
        "tar": datasets_dir / f"{base_name}.tar",
        "txt_gz": datasets_dir / f"{base_name}.txt.gz",
        "filtered": datasets_dir / f"{base_name}-filtered.tsv",
        "ebird_release": ebird_release,
        "download_url": (
            f"https://download.ebird.org/ebd/prepackaged/{ebird_release}.tar"
        ),
        # Sampling (checklists)
        "sampling_tar": datasets_dir / f"{sampling_base}.tar",
        "sampling_txt_gz": datasets_dir / f"{sampling_base}.txt.gz",
        "sampling_filtered": datasets_dir / f"{sampling_base}-filtered.tsv",
        "sampling_release": sampling_release,
        "sampling_download_url": (
            f"https://download.ebird.org/ebd/prepackaged/{sampling_release}.tar"
        ),
        # Output
        "db": outputs_dir / f"targets-{month_lower}-{year}.db",
        # Version info
        "month_abbrev": month_abbrev,
        "year": year,
        # Directories (for convenience)
        "datasets_dir": datasets_dir,
        "outputs_dir": outputs_dir,
    }


def print_header():
    """Print CLI header."""
    print()
    print("=" * 50)
    print("  EBD Aggregator")
    print("=" * 50)
    print()


def prompt_choice(prompt: str, options: list[str]) -> int:
    """
    Prompt user to select from a list of options using arrow keys.
    Returns the 0-based index of the selected option.
    """
    print(prompt)
    print()

    menu = TerminalMenu(
        options,
        menu_cursor="→ ",
        menu_cursor_style=("fg_cyan", "bold"),
        menu_highlight_style=("fg_cyan", "bold"),
    )

    idx = menu.show()

    if idx is None:
        print("\nExiting.")
        sys.exit(0)

    return idx


def run_download(paths: dict) -> bool:
    """
    Download the eBird Basic Dataset.
    Returns True if successful, False otherwise.
    """
    tar_file = paths["tar"]
    download_url = paths["download_url"]
    datasets_dir = paths["datasets_dir"]

    print("\n" + "-" * 50)
    print("Step: Download eBird Basic Dataset")
    print("-" * 50)
    sys.stdout.flush()

    if tar_file.exists():
        print(f"\nDataset already exists: {tar_file}")
        print("Skipping download. Delete the file to re-download.")
        return True

    print(f"\nDownloading: {download_url}")
    print(f"To: {tar_file}")
    print()
    sys.stdout.flush()

    # Ensure datasets directory exists
    datasets_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Use aria2c for fast, resumable downloads
        subprocess.run(
            [
                "caffeinate", "-dimsu",
                "aria2c",
                "-d", str(datasets_dir),
                "-o", tar_file.name,
                "-c",  # Continue/resume download
                "-x", "2",  # Max connections per server
                "-s", "2",  # Split file into segments
                "-j", "1",  # Max concurrent downloads
                "--retry-wait=30",
                "--max-tries=0",  # Retry indefinitely
                download_url,
            ],
            check=True,
        )
        print("\nDownload complete!")
        return True
    except subprocess.CalledProcessError:
        # Clean up partial download file on failure
        if tar_file.exists():
            tar_file.unlink()
        print("\nDownload failed. The dataset may not be available yet.")
        print("eBird releases datasets around mid-month.")
        return False
    except FileNotFoundError:
        print("\nError: aria2c not found. Install it with: brew install aria2")
        return False


def run_extract(paths: dict) -> bool:
    """
    Extract the tar archive.
    Returns True if successful, False otherwise.
    """
    tar_file = paths["tar"]
    txt_gz_file = paths["txt_gz"]
    ebird_release = paths["ebird_release"]
    datasets_dir = paths["datasets_dir"]

    print("\n" + "-" * 50)
    print("Step: Extract Archive")
    print("-" * 50)
    sys.stdout.flush()

    if txt_gz_file.exists():
        print(f"\nExtracted file already exists: {txt_gz_file}")
        print("Skipping extraction. Delete the file to re-extract.")
        return True

    if not tar_file.exists():
        print(f"\nError: Tar file not found: {tar_file}")
        print("Please run the download step first.")
        return False

    print(f"\nExtracting: {tar_file}")
    print(f"Target file: {ebird_release}.txt.gz")
    print()
    sys.stdout.flush()

    try:
        # Extract only the required file from the tar
        subprocess.run(
            [
                "caffeinate", "-i",
                "tar", "-xf", str(tar_file),
                "-C", str(datasets_dir),
                f"{ebird_release}.txt.gz",
            ],
            check=True,
        )

        # Rename to our naming convention
        extracted_file = datasets_dir / f"{ebird_release}.txt.gz"
        if extracted_file.exists():
            extracted_file.rename(txt_gz_file)
            print(f"Extracted and renamed to: {txt_gz_file}")
        else:
            print(f"Warning: Expected file not found: {extracted_file}")
            return False

        print("\nExtraction complete!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\nExtraction failed: {e}")
        return False


def run_filter(paths: dict) -> bool:
    """
    Filter the dataset to extract required columns.
    Returns True if successful, False otherwise.
    """
    txt_gz_file = paths["txt_gz"]
    filtered_file = paths["filtered"]

    print("\n" + "-" * 50)
    print("Step: Filter Dataset")
    print("-" * 50)
    sys.stdout.flush()

    if filtered_file.exists():
        print(f"\nFiltered file already exists: {filtered_file}")
        print("Skipping filtering. Delete the file to re-filter.")
        return True

    if not txt_gz_file.exists():
        print(f"\nError: Gzipped file not found: {txt_gz_file}")
        print("Please run the extract step first.")
        return False

    print(f"\nInput: {txt_gz_file}")
    print(f"Output: {filtered_file}")
    print()
    sys.stdout.flush()

    extract_script = SCRIPT_DIR / "extract_columns.py"

    try:
        subprocess.run(
            [
                "caffeinate", "-dims",
                "python3", str(extract_script),
                str(txt_gz_file),
                str(filtered_file),
            ],
            check=True,
        )
        print("\nFiltering complete!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\nFiltering failed: {e}")
        return False


def run_download_sampling(paths: dict) -> bool:
    """
    Download the eBird Sampling Dataset.
    Returns True if successful, False otherwise.
    """
    tar_file = paths["sampling_tar"]
    download_url = paths["sampling_download_url"]
    datasets_dir = paths["datasets_dir"]

    print("\n" + "-" * 50)
    print("Step: Download eBird Sampling Dataset")
    print("-" * 50)
    sys.stdout.flush()

    if tar_file.exists():
        print(f"\nDataset already exists: {tar_file}")
        print("Skipping download. Delete the file to re-download.")
        return True

    print(f"\nDownloading: {download_url}")
    print(f"To: {tar_file}")
    print()
    sys.stdout.flush()

    # Ensure datasets directory exists
    datasets_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Use aria2c for fast, resumable downloads
        subprocess.run(
            [
                "caffeinate", "-dimsu",
                "aria2c",
                "-d", str(datasets_dir),
                "-o", tar_file.name,
                "-c",  # Continue/resume download
                "-x", "2",  # Max connections per server
                "-s", "2",  # Split file into segments
                "-j", "1",  # Max concurrent downloads
                "--retry-wait=30",
                "--max-tries=0",  # Retry indefinitely
                download_url,
            ],
            check=True,
        )
        print("\nDownload complete!")
        return True
    except subprocess.CalledProcessError:
        # Clean up partial download file on failure
        if tar_file.exists():
            tar_file.unlink()
        print("\nDownload failed. The dataset may not be available yet.")
        print("eBird releases datasets around mid-month.")
        return False
    except FileNotFoundError:
        print("\nError: aria2c not found. Install it with: brew install aria2")
        return False


def run_extract_sampling(paths: dict) -> bool:
    """
    Extract the sampling tar archive.
    Returns True if successful, False otherwise.
    """
    tar_file = paths["sampling_tar"]
    txt_gz_file = paths["sampling_txt_gz"]
    sampling_release = paths["sampling_release"]
    datasets_dir = paths["datasets_dir"]

    print("\n" + "-" * 50)
    print("Step: Extract Sampling Archive")
    print("-" * 50)
    sys.stdout.flush()

    if txt_gz_file.exists():
        print(f"\nExtracted file already exists: {txt_gz_file}")
        print("Skipping extraction. Delete the file to re-extract.")
        return True

    if not tar_file.exists():
        print(f"\nError: Tar file not found: {tar_file}")
        print("Please run the sampling download step first.")
        return False

    print(f"\nExtracting: {tar_file}")
    print(f"Target file: {sampling_release}.txt.gz")
    print()
    sys.stdout.flush()

    try:
        # Extract only the required file from the tar
        subprocess.run(
            [
                "caffeinate", "-i",
                "tar", "-xf", str(tar_file),
                "-C", str(datasets_dir),
                f"{sampling_release}.txt.gz",
            ],
            check=True,
        )

        # Rename to our naming convention
        extracted_file = datasets_dir / f"{sampling_release}.txt.gz"
        if extracted_file.exists():
            extracted_file.rename(txt_gz_file)
            print(f"Extracted and renamed to: {txt_gz_file}")
        else:
            print(f"Warning: Expected file not found: {extracted_file}")
            return False

        print("\nExtraction complete!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\nExtraction failed: {e}")
        return False


def run_filter_sampling(paths: dict) -> bool:
    """
    Filter the sampling dataset to extract required columns.
    Returns True if successful, False otherwise.
    """
    txt_gz_file = paths["sampling_txt_gz"]
    filtered_file = paths["sampling_filtered"]

    print("\n" + "-" * 50)
    print("Step: Filter Sampling Dataset")
    print("-" * 50)
    sys.stdout.flush()

    if filtered_file.exists():
        print(f"\nFiltered file already exists: {filtered_file}")
        print("Skipping filtering. Delete the file to re-filter.")
        return True

    if not txt_gz_file.exists():
        print(f"\nError: Gzipped file not found: {txt_gz_file}")
        print("Please run the sampling extract step first.")
        return False

    print(f"\nInput: {txt_gz_file}")
    print(f"Output: {filtered_file}")
    print()
    sys.stdout.flush()

    extract_script = SCRIPT_DIR / "extract_sampling.py"

    try:
        subprocess.run(
            [
                "caffeinate", "-dims",
                "python3", str(extract_script),
                str(txt_gz_file),
                str(filtered_file),
            ],
            check=True,
        )
        print("\nFiltering complete!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\nFiltering failed: {e}")
        return False


def run_build_db(paths: dict, env_vars: dict) -> bool:
    """
    Build the SQLite database.
    Returns True if successful, False otherwise.
    """
    filtered_file = paths["filtered"]
    sampling_file = paths["sampling_filtered"]
    db_file = paths["db"]
    outputs_dir = paths["outputs_dir"]

    print("\n" + "-" * 50)
    print("Step: Build SQLite Database")
    print("-" * 50)
    sys.stdout.flush()

    if not filtered_file.exists():
        print(f"\nError: Filtered species file not found: {filtered_file}")
        print("Please run the filter step first.")
        return False

    if not sampling_file.exists():
        print(f"\nError: Filtered sampling file not found: {sampling_file}")
        print("Please run the sampling filter step first.")
        return False

    # Ensure output directory exists
    outputs_dir.mkdir(parents=True, exist_ok=True)

    print(f"\nSpecies file: {filtered_file}")
    print(f"Sampling file: {sampling_file}")
    print(f"Output: {db_file}")

    # Get config from environment
    memory_limit = env_vars.get("MEMORY_LIMIT", "24")
    threads = env_vars.get("THREADS", "8")
    wilson_z = env_vars.get("WILSON_SCORE_Z_INDEX", "1.96")

    print(f"Memory limit: {memory_limit}GB")
    print(f"Threads: {threads}")
    print(f"Wilson z-index: {wilson_z}")
    print()
    sys.stdout.flush()

    generate_script = SCRIPT_DIR / "generate_data.py"

    cmd = [
        "caffeinate", "-dims",
        "python3", str(generate_script),
        str(filtered_file),
        str(sampling_file),
        str(db_file),
        "--memory-limit", f"{memory_limit}GB",
        "--threads", threads,
        "--wilson-z", wilson_z,
        "--version-year", paths["year"],
        "--version-month", paths["month_abbrev"],
    ]

    try:
        subprocess.run(cmd, check=True)
        print("\nDatabase build complete!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\nDatabase build failed: {e}")
        return False


def run_generate_packs(paths: dict, env_vars: dict, packs_dir: Path) -> bool:
    """
    Generate compressed JSON pack files for each region.
    Returns True if successful, False otherwise.
    """
    db_file = paths["db"]

    print("\n" + "-" * 50)
    print("Step: Generate Packs")
    print("-" * 50)
    sys.stdout.flush()

    if not db_file.exists():
        print(f"\nError: Database file not found: {db_file}")
        print("Please run the build database step first.")
        return False

    # Check for eBird API key
    api_key = env_vars.get("EBIRD_API_KEY")
    if not api_key:
        print("\nError: EBIRD_API_KEY not found in .env")
        print("Please add your eBird API key to the .env file.")
        return False

    print(f"\nDatabase: {db_file}")
    print(f"Output directory: {packs_dir}")
    print()
    sys.stdout.flush()

    generate_script = SCRIPT_DIR / "generate_packs.py"

    cmd = [
        "caffeinate", "-dims",
        "python3", str(generate_script),
        str(db_file),
        "--output-dir", str(packs_dir),
    ]

    try:
        subprocess.run(cmd, check=True)
        print("\nPack generation complete!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\nPack generation failed: {e}")
        return False


def run_upload_packs(
    paths: dict, env_vars: dict, packs_dir: Path, pack_version: str
) -> bool:
    """
    Upload packs to S3-compatible storage.
    Returns True if successful, False otherwise.
    """
    print("\n" + "-" * 50)
    print("Step: Upload Packs to S3")
    print("-" * 50)
    sys.stdout.flush()

    # Check for S3 credentials
    missing = []
    if not env_vars.get("S3_KEY_ID"):
        missing.append("S3_KEY_ID")
    if not env_vars.get("S3_SECRET"):
        missing.append("S3_SECRET")
    if not env_vars.get("S3_BUCKET"):
        missing.append("S3_BUCKET")
    if not env_vars.get("S3_ENDPOINT"):
        missing.append("S3_ENDPOINT")

    if missing:
        print(f"\nError: Missing S3 credentials in .env: {', '.join(missing)}")
        return False

    # Check that packs exist
    index_file = packs_dir / "packs.json.gz"
    version_dir = packs_dir / pack_version

    if not index_file.exists():
        print(f"\nError: Index file not found: {index_file}")
        print("Please run the generate packs step first.")
        return False

    if not version_dir.exists():
        print(f"\nError: Version directory not found: {version_dir}")
        print("Please run the generate packs step first.")
        return False

    print(f"\nPacks directory: {packs_dir}")
    print(f"Pack version: {pack_version}")
    print()
    sys.stdout.flush()

    upload_script = SCRIPT_DIR / "upload_packs.py"

    cmd = [
        "python3", str(upload_script),
        str(packs_dir),
        pack_version,
    ]

    try:
        subprocess.run(cmd, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"\nUpload failed: {e}")
        return False


def run_upload_sqlite(paths: dict, env_vars: dict) -> bool:
    """
    Upload the SQLite database to the remote server via SSH + Docker.
    Returns True if successful, False otherwise.
    """
    db_file = paths["db"]

    print("\n" + "-" * 50)
    print("Step: Upload SQLite Database")
    print("-" * 50)
    sys.stdout.flush()

    if not db_file.exists():
        print(f"\nError: Database file not found: {db_file}")
        print("Please run the build database step first.")
        return False

    # Check for SSH config
    ssh_user = env_vars.get("SSH_USER")
    ssh_host = env_vars.get("SSH_HOST")
    docker_volume = env_vars.get("DOCKER_VOLUME")

    missing = []
    if not ssh_user:
        missing.append("SSH_USER")
    if not ssh_host:
        missing.append("SSH_HOST")
    if not docker_volume:
        missing.append("DOCKER_VOLUME")

    if missing:
        print(f"\nError: Missing SSH config in .env: {', '.join(missing)}")
        return False

    print(f"\nDatabase: {db_file}")
    print(f"Remote: {ssh_user}@{ssh_host}")
    print(f"Docker volume: {docker_volume}")
    print()

    # Warn user to stop apps using the database
    print("WARNING: Make sure any apps using the SQLite database on the")
    print("remote server are stopped before continuing.")
    print()
    response = input("Continue? [y/N] ").strip().lower()
    if response != "y":
        print("\nUpload cancelled.")
        return False

    print()
    sys.stdout.flush()

    cmd = (
        f"pv \"{db_file}\" | ssh {ssh_user}@{ssh_host} "
        f"\"docker run --rm -i -v {docker_volume}:/data alpine "
        f"sh -c 'cat > /data/targets.db'\""
    )

    try:
        subprocess.run(cmd, shell=True, check=True)
        print("\nSQLite upload complete!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\nSQLite upload failed: {e}")
        return False


def run_all_steps(
    paths: dict,
    env_vars: dict,
    packs_dir: Path,
    pack_version: str,
) -> bool:
    """
    Run all pipeline steps in sequence (without upload).
    Returns True if all steps succeed, exits on failure.
    """
    print("\nRunning all steps...")

    steps = [
        (run_download, "Download species dataset failed."),
        (run_extract, "Extract species archive failed."),
        (run_filter, "Filter species dataset failed."),
        (run_download_sampling, "Download sampling dataset failed."),
        (run_extract_sampling, "Extract sampling archive failed."),
        (run_filter_sampling, "Filter sampling dataset failed."),
    ]

    for step_fn, error_msg in steps:
        if not step_fn(paths):
            print(f"\nAborting: {error_msg}")
            sys.exit(1)

    if not run_build_db(paths, env_vars):
        print("\nAborting: Build database step failed.")
        sys.exit(1)

    if not run_generate_packs(paths, env_vars, packs_dir):
        print("\nAborting: Generate packs step failed.")
        sys.exit(1)

    return True


def main():
    print_header()

    env_vars = load_env_file()

    # Get directories based on OUTPUT_PATH
    datasets_dir, outputs_dir, packs_dir = get_output_paths(env_vars)

    # Step 1: Choose dataset month
    month_options = get_month_options()
    month_display = [opt[0] for opt in month_options]

    month_idx = prompt_choice(
        "Which dataset do you want to use? Some may not be available yet.",
        month_display
    )
    _, month_abbrev, year = month_options[month_idx]
    paths = get_file_paths(month_abbrev, year, datasets_dir, outputs_dir)

    # Derive pack version from month/year (e.g., "dec-2025")
    pack_version = f"{month_abbrev.lower()}-{year}"

    # Step 2: Choose which operation to run
    operations = [
        "Download Species",
        "Extract Species",
        "Filter Species",
        "Download Sampling",
        "Extract Sampling",
        "Filter Sampling",
        "Build Database",
        "Generate Packs",
        "All (without upload)",
        "Upload Packs",
        "Upload SQLite",
    ]

    print()
    op_idx = prompt_choice("Which step do you want to run?", operations)
    operation_name = operations[op_idx]

    print()
    print("=" * 50)

    # Map operation index to handler
    if op_idx == 0:
        success = run_download(paths)
    elif op_idx == 1:
        success = run_extract(paths)
    elif op_idx == 2:
        success = run_filter(paths)
    elif op_idx == 3:
        success = run_download_sampling(paths)
    elif op_idx == 4:
        success = run_extract_sampling(paths)
    elif op_idx == 5:
        success = run_filter_sampling(paths)
    elif op_idx == 6:
        success = run_build_db(paths, env_vars)
    elif op_idx == 7:
        success = run_generate_packs(paths, env_vars, packs_dir)
    elif op_idx == 8:
        success = run_all_steps(paths, env_vars, packs_dir, pack_version)
    elif op_idx == 9:
        success = run_upload_packs(paths, env_vars, packs_dir, pack_version)
    elif op_idx == 10:
        success = run_upload_sqlite(paths, env_vars)

    print()
    print("=" * 50)
    if success:
        print("Complete!")
    else:
        print("Failed!")

    # Send notification if configured
    ntfy_topic = env_vars.get("NTFY_NOTIFICATION_TOPIC")
    if ntfy_topic:
        status = "Complete" if success else "Failed"
        send_notification(
            topic=ntfy_topic,
            title=f"EBD Aggregator: {status}",
            message=f"{operation_name} - {month_display[month_idx]}",
            success=success,
        )

    if not success:
        sys.exit(1)
    print("=" * 50)


if __name__ == "__main__":
    main()
