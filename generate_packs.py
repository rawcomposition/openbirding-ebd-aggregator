#!/usr/bin/env python3
"""
Generate compressed JSON pack files for each region from the targets database.

This script reads the targets SQLite database and generates gzipped JSON files
for each region defined in packs.json.
"""

import argparse
import gzip
import json
import math
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests

from utils import format_duration, format_size, load_env_file

# Get script directory for relative paths
SCRIPT_DIR = Path(__file__).parent.resolve()

# Rate limiting for eBird API
EBIRD_API_DELAY = 1.0  # 1 second between API calls


@dataclass
class PackEntry:
    """Entry from packs.json."""
    id: int
    region: str
    center_lat: Optional[float]
    center_lng: Optional[float]
    name: str
    tags: list[str]


@dataclass
class EBirdHotspot:
    """Hotspot data from eBird API."""
    location_id: str
    name: str
    lat: float
    lng: float
    total: int
    country_code: str
    subnational1_code: str
    subnational2_code: str


@dataclass
class PackMetadata:
    """Metadata for a generated pack."""
    v: str
    id: int
    region: str
    name: str
    tags: list[str]
    hotspots: int
    clusters: list
    size: int
    updated_at: str
    url: str


def get_distance_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Calculate distance between two points using Haversine formula."""
    R = 6371  # Earth radius in km
    to_rad = lambda d: d * math.pi / 180

    d_lat = to_rad(lat2 - lat1)

    # Handle longitude wrapping around the date line
    d_lng_deg = lng2 - lng1
    if d_lng_deg > 180:
        d_lng_deg -= 360
    if d_lng_deg < -180:
        d_lng_deg += 360
    d_lng = to_rad(d_lng_deg)

    a = math.sin(d_lat / 2) ** 2 + math.cos(to_rad(lat1)) * math.cos(to_rad(lat2)) * math.sin(d_lng / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def desired_clusters(hotspot_count: int, min_k: int = 3, max_k: int = 30, scale: float = 1.5) -> int:
    """Calculate desired number of clusters based on hotspot count."""
    if hotspot_count <= 0:
        return min_k
    k = math.ceil(scale * math.log2(hotspot_count))
    return max(min_k, min(max_k, k))


def k_center_clustering(
    points: list[dict],
    k: int,
    start_lat: Optional[float] = None,
    start_lng: Optional[float] = None
) -> list[dict]:
    """K-center clustering algorithm to find k well-distributed points."""
    if not points or k <= 0:
        return []
    if k >= len(points):
        return list(points)

    # Find first center
    if start_lat is not None and start_lng is not None:
        min_distance = float('inf')
        nearest_point = None
        for point in points:
            distance = get_distance_km(start_lat, start_lng, point['lat'], point['lng'])
            if distance < min_distance:
                min_distance = distance
                nearest_point = point
        first_center = nearest_point or points[0]
    else:
        first_center = points[0]

    centers = [first_center]
    min_distances = [float('inf')] * len(points)
    center_indices = set()

    # Initialize distances from first center
    for i, point in enumerate(points):
        if point is first_center:
            center_indices.add(i)
            min_distances[i] = 0
        else:
            min_distances[i] = get_distance_km(first_center['lat'], first_center['lng'], point['lat'], point['lng'])

    # Find remaining centers
    for _ in range(1, k):
        max_min_distance = -1
        next_center_idx = -1

        for j in range(len(points)):
            if j in center_indices:
                continue
            if min_distances[j] > max_min_distance:
                max_min_distance = min_distances[j]
                next_center_idx = j

        if next_center_idx == -1:
            break

        next_center = points[next_center_idx]
        centers.append(next_center)
        center_indices.add(next_center_idx)
        min_distances[next_center_idx] = 0

        # Update minimum distances
        for j in range(len(points)):
            if j in center_indices:
                continue
            distance = get_distance_km(next_center['lat'], next_center['lng'], points[j]['lat'], points[j]['lng'])
            if distance < min_distances[j]:
                min_distances[j] = distance

    return centers


def generate_clusters(
    hotspots: list[dict],
    center_lat: Optional[float] = None,
    center_lng: Optional[float] = None
) -> list[list[float]]:
    """Generate cluster centers for hotspots."""
    if not hotspots:
        return []

    k = desired_clusters(len(hotspots))
    centers = k_center_clustering(hotspots, k, center_lat, center_lng)

    return [
        [round(center['lat'] * 1000) / 1000, round(center['lng'] * 1000) / 1000]
        for center in centers
    ]


def load_packs() -> list[PackEntry]:
    """Load pack definitions from packs.json."""
    packs_path = SCRIPT_DIR / "packs.json"
    with open(packs_path) as f:
        data = json.load(f)

    return [
        PackEntry(
            id=p['id'],
            region=p['region'],
            center_lat=p.get('center_lat'),
            center_lng=p.get('center_lng'),
            name=p['name'],
            tags=p.get('tags', []),
        )
        for p in data
    ]


def fetch_regions() -> dict[str, str]:
    """Fetch region names from OpenBirding API."""
    response = requests.get("http://api.openbirding.org/api/v1/regions", timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_hotspots_for_region(region: str, api_key: str) -> list[EBirdHotspot]:
    """Fetch hotspots for a region from eBird API."""
    url = f"https://api.ebird.org/v2/ref/hotspot/{region}?fmt=json&key={api_key}"
    response = requests.get(url, timeout=60)

    if not response.ok:
        raise Exception(f"eBird API request failed: {response.status_text}")

    data = response.json()

    if isinstance(data, dict) and "errors" in data:
        raise Exception("Error fetching eBird hotspots")

    hotspots = []
    for h in data:
        name = h.get('locName', '').strip()
        hotspots.append(EBirdHotspot(
            location_id=h['locId'],
            name=name,
            lat=h['lat'],
            lng=h['lng'],
            total=h.get('numSpeciesAllTime', 0),
            country_code=h.get('countryCode', ''),
            subnational1_code=h.get('subnational1Code', ''),
            subnational2_code=h.get('subnational2Code', ''),
        ))

    return hotspots


def build_month_obs_map(rows: list[tuple]) -> dict:
    """Build month observations data structure from database rows."""
    obs_by_location = {}

    for row in rows:
        location_id, month, species_id, obs, samples = row

        if location_id not in obs_by_location:
            obs_by_location[location_id] = {
                'samples': [None] * 12,
                'species_obs': {}
            }

        location_data = obs_by_location[location_id]
        month_idx = month - 1

        # Set samples for this month
        if location_data['samples'][month_idx] is None:
            location_data['samples'][month_idx] = samples

        # Set species observations
        if species_id not in location_data['species_obs']:
            location_data['species_obs'][species_id] = [0] * 12
        location_data['species_obs'][species_id][month_idx] = obs

    return obs_by_location


def build_pack_hotspots(
    ebird_hotspots: list[EBirdHotspot],
    region_names: dict[str, str]
) -> list[dict]:
    """Build pack hotspots array from eBird hotspots."""
    pack_hotspots = []

    for h in ebird_hotspots:
        pack_hotspots.append({
            'id': h.location_id,
            'name': h.name,
            'species': h.total,
            'lat': h.lat,
            'lng': h.lng,
            'country': h.country_code,
            'state': h.subnational1_code or None,
            'county': h.subnational2_code or None,
            'countryName': region_names.get(h.country_code, h.country_code),
            'stateName': region_names.get(h.subnational1_code) if h.subnational1_code else None,
            'countyName': region_names.get(h.subnational2_code) if h.subnational2_code else None,
        })

    return pack_hotspots


def build_pack_targets(
    obs_by_location: dict,
    species_by_id: dict[int, str]
) -> list[dict]:
    """Build pack targets array from observation data."""
    pack_targets = []

    for location_id, location_data in obs_by_location.items():
        species_array = []

        for species_id, obs_array in location_data['species_obs'].items():
            species_code = species_by_id.get(species_id)
            if species_code:
                species_array.append([species_code] + obs_array)

        pack_targets.append({
            'id': location_id,
            'samples': location_data['samples'],
            'species': species_array,
        })

    return pack_targets


def generate_pack(
    pack: PackEntry,
    db_path: Path,
    output_dir: Path,
    species_by_id: dict[int, str],
    region_names: dict[str, str],
    api_key: str,
    pack_version: str,
    base_url: str,
    is_first_pack: bool,
    progress: str = ""
) -> Optional[PackMetadata]:
    """Generate a pack for a single region."""
    prefix = f"[{progress}] " if progress else ""
    print(f"\n{prefix}Generating pack for region: {pack.region}")
    start_time = time.time()

    # Rate limiting (except for first pack)
    if not is_first_pack:
        time.sleep(EBIRD_API_DELAY)

    # Fetch hotspots from eBird API
    try:
        ebird_hotspots = fetch_hotspots_for_region(pack.region, api_key)
        print(f"  Fetched {len(ebird_hotspots)} hotspots from eBird")
    except Exception as e:
        print(f"  Error fetching hotspots from eBird: {e}")
        return None

    if not ebird_hotspots:
        print("  Skipping - no hotspots")
        return None

    # Query month_obs from database
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA cache_size = -64000")  # 64MB cache

    cursor = conn.execute("""
        SELECT location_id, month, species_id, obs, samples
        FROM month_obs
        WHERE location_id IN (SELECT id FROM hotspots WHERE region_code LIKE ? || '%')
    """, (pack.region,))
    month_obs_rows = cursor.fetchall()
    conn.close()

    print(f"  Found {len(month_obs_rows)} month_obs rows in database")

    # Build observation data structure
    obs_by_location = build_month_obs_map(month_obs_rows)

    # Build pack hotspots
    pack_hotspots = build_pack_hotspots(ebird_hotspots, region_names)

    # Build pack targets
    pack_targets = build_pack_targets(obs_by_location, species_by_id)

    # Create pack data
    updated_at = datetime.utcnow().isoformat() + 'Z'
    pack_data = {
        'v': pack_version,
        'updatedAt': updated_at,
        'hotspots': pack_hotspots,
        'targets': pack_targets,
    }

    # Write gzipped JSON to versioned subdirectory
    version_dir = output_dir / pack_version
    version_dir.mkdir(parents=True, exist_ok=True)
    output_path = version_dir / f"{pack.region}.json.gz"
    pack_url = f"{base_url}{pack_version}/{pack.region}.json.gz"

    json_string = json.dumps(pack_data, separators=(',', ':'), ensure_ascii=False)
    json_bytes = json_string.encode('utf-8')

    with gzip.open(output_path, 'wb') as f:
        f.write(json_bytes)

    file_size = output_path.stat().st_size

    # Generate clusters
    hotspot_coords = [{'lat': h.lat, 'lng': h.lng} for h in ebird_hotspots]
    clusters = generate_clusters(hotspot_coords, pack.center_lat, pack.center_lng)

    elapsed = time.time() - start_time
    uncompressed_kb = len(json_bytes) // 1024
    compressed_kb = file_size // 1024
    print(f"  Generated {output_path.name} ({uncompressed_kb} KB uncompressed, {compressed_kb} KB compressed) in {elapsed:.1f}s")

    return PackMetadata(
        v=pack_version,
        id=pack.id,
        region=pack.region,
        name=pack.name,
        tags=pack.tags,
        hotspots=len(ebird_hotspots),
        clusters=clusters,
        size=file_size,
        updated_at=updated_at,
        url=pack_url,
    )


def main():
    parser = argparse.ArgumentParser(description="Generate compressed JSON pack files for each region")
    parser.add_argument("db_path", type=Path, help="Path to targets SQLite database")
    parser.add_argument("--region", type=str, help="Generate pack for a single region only")
    parser.add_argument("--output-dir", type=Path, help="Output directory for pack files")
    args = parser.parse_args()

    # Load environment variables
    env_vars = load_env_file()

    # Get eBird API key
    api_key = env_vars.get("EBIRD_API_KEY") or os.environ.get("EBIRD_API_KEY")
    if not api_key:
        print("Error: EBIRD_API_KEY not found in .env or environment")
        sys.exit(1)

    # Determine output directory
    if args.output_dir:
        output_dir = args.output_dir
    else:
        output_path = env_vars.get("OUTPUT_PATH", "")
        if output_path:
            output_dir = (SCRIPT_DIR / output_path / "output" / "packs").resolve()
        else:
            output_dir = SCRIPT_DIR / "output" / "packs"

    output_dir.mkdir(parents=True, exist_ok=True)

    # Derive pack version from database filename (e.g., "targets-dec-2025.db" -> "dec-2025")
    db_name = args.db_path.stem
    if db_name.startswith("targets-"):
        pack_version = db_name.replace("targets-", "")
    else:
        pack_version = db_name

    # Build base URL for pack files
    s3_public_url = env_vars.get("S3_PUBLIC_URL", "")
    s3_dir = env_vars.get("S3_DIR", "")
    if s3_public_url:
        # Ensure URL ends with /
        if not s3_public_url.endswith("/"):
            s3_public_url += "/"
        # Add S3_DIR if set
        if s3_dir:
            base_url = f"{s3_public_url}{s3_dir}/"
        else:
            base_url = s3_public_url
    else:
        # Fallback to relative paths if no public URL configured
        base_url = f"{s3_dir}/" if s3_dir else ""

    print("=" * 50)
    print("  Generate Packs")
    print("=" * 50)
    print(f"\nDatabase: {args.db_path}")
    print(f"Output directory: {output_dir}")
    print(f"Pack version: {pack_version}")
    if s3_public_url:
        print(f"Base URL: {base_url}")

    # Load packs
    packs = load_packs()
    print(f"Loaded {len(packs)} pack definitions")

    # Load species from database
    conn = sqlite3.connect(args.db_path)
    cursor = conn.execute("SELECT id, code FROM species")
    species_by_id = {row[0]: row[1] for row in cursor.fetchall()}
    conn.close()
    print(f"Loaded {len(species_by_id)} species")

    # Fetch region names from API
    print("Fetching region names from OpenBirding API...")
    try:
        region_names = fetch_regions()
        print(f"Loaded {len(region_names)} region names")
    except Exception as e:
        print(f"Warning: Could not fetch region names: {e}")
        region_names = {}

    start_time = time.time()
    pack_metadata_list = []

    if args.region:
        # Process single region
        pack = next((p for p in packs if p.region == args.region), None)
        if not pack:
            print(f"\nError: Pack for region '{args.region}' not found.")
            print(f"Available regions: {', '.join(p.region for p in packs)}")
            sys.exit(1)

        metadata = generate_pack(
            pack, args.db_path, output_dir, species_by_id, region_names,
            api_key, pack_version, base_url, True, "1/1"
        )
        if metadata:
            pack_metadata_list.append(metadata)
    else:
        # Process all packs
        print(f"\nProcessing {len(packs)} packs...")

        total_packs = len(packs)
        for i, pack in enumerate(packs):
            try:
                metadata = generate_pack(
                    pack, args.db_path, output_dir, species_by_id, region_names,
                    api_key, pack_version, base_url, i == 0, f"{i + 1}/{total_packs}"
                )
                if metadata:
                    pack_metadata_list.append(metadata)
            except Exception as e:
                print(f"Error processing pack {pack.region}: {e}")

    # Generate packs.json.gz index file
    if pack_metadata_list:
        packs_index = {
            'packs': [
                {
                    'v': m.v,
                    'id': m.id,
                    'region': m.region,
                    'name': m.name,
                    'tags': m.tags,
                    'hotspots': m.hotspots,
                    'clusters': m.clusters,
                    'size': m.size,
                    'updatedAt': m.updated_at,
                    'url': m.url,
                }
                for m in pack_metadata_list
            ]
        }

        packs_index_path = output_dir / "packs.json.gz"
        packs_index_json = json.dumps(
            packs_index, separators=(',', ':'), ensure_ascii=False
        )

        with gzip.open(packs_index_path, 'wb') as f:
            f.write(packs_index_json.encode('utf-8'))

        print(f"\nGenerated {packs_index_path.name} with {len(pack_metadata_list)} packs")

    total_elapsed = time.time() - start_time
    print(f"\nCompleted in {format_duration(total_elapsed)}")


if __name__ == "__main__":
    main()
