"""Converts li_input/csv/Routes2check.csv to li_input/geojson/Routes2check.geojson.

Each row becomes a GeoJSON Feature with:
- geometry: LineString [origin → destination]  (coordinates: [lon, lat])
- properties: all remaining columns
"""

import csv
import json
import re
import sys
from pathlib import Path

INPUT_CSV = Path(__file__).parents[1] / "li_input" / "csv" / "Routes2check.csv"
OUTPUT_DIR = Path(__file__).parents[1] / "li_input" / "geojson"
OUTPUT_GEOJSON = OUTPUT_DIR / "Routes2check.geojson"


def parse_point(point_str: str) -> list[float]:
    """Parses 'POINT(lon lat)' → [lon, lat]."""
    match = re.match(r"POINT\(([0-9.\-]+)\s+([0-9.\-]+)\)", point_str.strip())
    if not match:
        raise ValueError(f"Cannot parse POINT: {point_str!r}")
    return [float(match.group(1)), float(match.group(2))]


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    features = []
    with open(INPUT_CSV, encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            try:
                origin = parse_point(row["origin"])
                destination = parse_point(row["destination"])
            except ValueError as exc:
                print(f"WARNING: skipping row {row.get('route_id')}: {exc}", file=sys.stderr)
                continue

            properties = {
                k: v for k, v in row.items() if k not in ("origin", "destination")
            }

            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [origin, destination],
                },
                "properties": properties,
            }
            features.append(feature)

    geojson = {
        "type": "FeatureCollection",
        "features": features,
    }

    with open(OUTPUT_GEOJSON, "w", encoding="utf-8") as fh:
        json.dump(geojson, fh, ensure_ascii=False, indent=2)

    print(f"Saved {len(features)} features → {OUTPUT_GEOJSON}")


if __name__ == "__main__":
    main()

