"""
Generate spatial query windows for experiments.

Distributions:
  UNI  — uniformly random within the dataset bounding box
  SKE  — skewed (Zipf-distributed hotspot regions)
  GUS  — Gaussian mixture centered at trajectory density peaks
"""

from __future__ import annotations
import argparse
import csv
import os
import random
import math
from typing import Literal

Distribution = Literal["UNI", "SKE", "GUS"]

DATASETS = {
    "tdrive": {
        "lon_min": 116.10, "lon_max": 116.70,
        "lat_min": 39.75,  "lat_max": 40.18,
    },
    "cdtaxi": {
        "lon_min": 103.92, "lon_max": 104.20,
        "lat_min": 30.52,  "lat_max": 30.78,
    },
}

METRES_PER_DEGREE_LAT = 111_320.0


def metres_to_degrees_lon(metres: float, lat: float) -> float:
    return metres / (METRES_PER_DEGREE_LAT * math.cos(math.radians(lat)))


def metres_to_degrees_lat(metres: float) -> float:
    return metres / METRES_PER_DEGREE_LAT


def generate_uniform(bbox: dict, range_m: float, n: int, rng: random.Random):
    half_lon = metres_to_degrees_lon(range_m / 2, (bbox["lat_min"] + bbox["lat_max"]) / 2)
    half_lat = metres_to_degrees_lat(range_m / 2)
    queries = []
    for _ in range(n):
        cx = rng.uniform(bbox["lon_min"] + half_lon, bbox["lon_max"] - half_lon)
        cy = rng.uniform(bbox["lat_min"] + half_lat, bbox["lat_max"] - half_lat)
        queries.append((cx - half_lon, cy - half_lat, cx + half_lon, cy + half_lat))
    return queries


def generate_skewed(bbox: dict, range_m: float, n: int, rng: random.Random,
                    num_hotspots: int = 5, alpha: float = 1.5):
    """Zipf-weighted hotspot distribution."""
    half_lon = metres_to_degrees_lon(range_m / 2, (bbox["lat_min"] + bbox["lat_max"]) / 2)
    half_lat = metres_to_degrees_lat(range_m / 2)
    hotspots = [
        (rng.uniform(bbox["lon_min"], bbox["lon_max"]),
         rng.uniform(bbox["lat_min"], bbox["lat_max"]))
        for _ in range(num_hotspots)
    ]
    weights = [1.0 / (i + 1) ** alpha for i in range(num_hotspots)]
    total = sum(weights)
    weights = [w / total for w in weights]

    queries = []
    for _ in range(n):
        r = rng.random()
        cumul = 0.0
        chosen = hotspots[0]
        for hs, w in zip(hotspots, weights):
            cumul += w
            if r <= cumul:
                chosen = hs
                break
        jitter_lon = rng.gauss(0, half_lon * 0.5)
        jitter_lat = rng.gauss(0, half_lat * 0.5)
        cx = max(bbox["lon_min"] + half_lon,
                 min(bbox["lon_max"] - half_lon, chosen[0] + jitter_lon))
        cy = max(bbox["lat_min"] + half_lat,
                 min(bbox["lat_max"] - half_lat, chosen[1] + jitter_lat))
        queries.append((cx - half_lon, cy - half_lat, cx + half_lon, cy + half_lat))
    return queries


def generate_gaussian(bbox: dict, range_m: float, n: int, rng: random.Random,
                      num_components: int = 3):
    """Gaussian mixture distribution."""
    half_lon = metres_to_degrees_lon(range_m / 2, (bbox["lat_min"] + bbox["lat_max"]) / 2)
    half_lat = metres_to_degrees_lat(range_m / 2)
    lon_span = bbox["lon_max"] - bbox["lon_min"]
    lat_span = bbox["lat_max"] - bbox["lat_min"]
    centers = [
        (rng.uniform(bbox["lon_min"] + 0.2 * lon_span,
                     bbox["lon_max"] - 0.2 * lon_span),
         rng.uniform(bbox["lat_min"] + 0.2 * lat_span,
                     bbox["lat_max"] - 0.2 * lat_span))
        for _ in range(num_components)
    ]
    sigma_lon = lon_span * 0.1
    sigma_lat = lat_span * 0.1

    queries = []
    for _ in range(n):
        center = rng.choice(centers)
        cx = rng.gauss(center[0], sigma_lon)
        cy = rng.gauss(center[1], sigma_lat)
        cx = max(bbox["lon_min"] + half_lon, min(bbox["lon_max"] - half_lon, cx))
        cy = max(bbox["lat_min"] + half_lat, min(bbox["lat_max"] - half_lat, cy))
        queries.append((cx - half_lon, cy - half_lat, cx + half_lon, cy + half_lat))
    return queries


def generate_queries(dataset: str, distribution: Distribution,
                     range_m: float, n: int, seed: int = 42):
    bbox = DATASETS[dataset]
    rng  = random.Random(seed)
    if distribution == "UNI":
        return generate_uniform(bbox, range_m, n, rng)
    elif distribution == "SKE":
        return generate_skewed(bbox, range_m, n, rng)
    elif distribution == "GUS":
        return generate_gaussian(bbox, range_m, n, rng)
    else:
        raise ValueError(f"Unknown distribution: {distribution}")


def main():
    parser = argparse.ArgumentParser(description="Generate query windows")
    parser.add_argument("--dataset",      choices=list(DATASETS.keys()), required=True)
    parser.add_argument("--distribution", choices=["UNI", "SKE", "GUS"], required=True)
    parser.add_argument("--range_m",      type=float, required=True, help="Query side length (m)")
    parser.add_argument("--n",            type=int, default=100)
    parser.add_argument("--out_dir",      required=True)
    parser.add_argument("--seed",         type=int, default=42)
    args = parser.parse_args()

    queries = generate_queries(args.dataset, args.distribution, args.range_m, args.n, args.seed)
    os.makedirs(args.out_dir, exist_ok=True)
    fname = f"{args.dataset}_{args.distribution}_{int(args.range_m)}m.csv"
    fpath = os.path.join(args.out_dir, fname)
    with open(fpath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["lon_min", "lat_min", "lon_max", "lat_max"])
        writer.writerows(queries)
    print(f"Generated {len(queries)} queries → {fpath}")


if __name__ == "__main__":
    main()
