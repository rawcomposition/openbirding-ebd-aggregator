#!/usr/bin/env python3
"""
Shared utility functions for EBD Aggregator.
"""

from pathlib import Path


def format_duration(seconds: float) -> str:
    """Format duration in human-readable form."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    if seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.0f}s"
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    return f"{hours}h {minutes}m"


def format_size(bytes_count: int) -> str:
    """Format bytes as human-readable size."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_count < 1024:
            return f"{bytes_count:.1f} {unit}"
        bytes_count /= 1024
    return f"{bytes_count:.1f} PB"


def taxa_file_path(filtered_file: Path) -> Path:
    """Path of the taxa file written alongside a filtered species file."""
    return filtered_file.with_suffix(".taxa.tsv")


def load_env_file(env_path: Path = None) -> dict:
    """
    Load environment variables from .env file.

    Args:
        env_path: Path to .env file. If None, uses .env in script directory.

    Returns:
        Dictionary of environment variable key-value pairs.
    """
    if env_path is None:
        env_path = Path(__file__).parent / ".env"

    env_vars = {}
    if not env_path.exists():
        return env_vars

    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                env_vars[key.strip()] = value.strip().strip('"').strip("'")

    return env_vars
