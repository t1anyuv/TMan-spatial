"""
Preprocess raw T-Drive dataset into trajectory segments for LETI experiments.

Raw T-Drive format (per file):
    taxi_id, datetime, longitude, latitude

Output format (per line, TSV):
    segment_id  taxi_id  start_lon  start_lat  end_lon  end_lat  start_time  end_time
"""

import os
import argparse
import csv
from datetime import datetime

TDRIVE_BBOX = {
    "lon_min": 116.10, "lon_max": 116.70,
    "lat_min": 39.75,  "lat_max": 40.18,
}

MAX_SPEED_MS = 55.0   # ~200 km/h — filter GPS noise
MIN_POINTS   = 2


def parse_args():
    parser = argparse.ArgumentParser(description="Preprocess T-Drive dataset")
    parser.add_argument("--raw_dir",  required=True, help="Directory with raw .txt files")
    parser.add_argument("--out_dir",  required=True, help="Output directory")
    parser.add_argument("--min_seg_len", type=int, default=2,
                        help="Minimum GPS points per segment (default: 2)")
    return parser.parse_args()


def in_bbox(lon: float, lat: float) -> bool:
    return (TDRIVE_BBOX["lon_min"] <= lon <= TDRIVE_BBOX["lon_max"] and
            TDRIVE_BBOX["lat_min"] <= lat <= TDRIVE_BBOX["lat_max"])


def haversine_m(lon1, lat1, lon2, lat2) -> float:
    """Return distance in metres between two WGS-84 points."""
    import math
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi  = math.radians(lat2 - lat1)
    dlam  = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def read_taxi_file(path: str):
    """Yield (taxi_id, timestamp, lon, lat) tuples sorted by time."""
    records = []
    taxi_id = os.path.splitext(os.path.basename(path))[0]
    with open(path, encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split(",")
            if len(parts) < 4:
                continue
            try:
                ts  = datetime.strptime(parts[1].strip(), "%Y-%m-%d %H:%M:%S")
                lon = float(parts[2])
                lat = float(parts[3])
            except (ValueError, IndexError):
                continue
            if in_bbox(lon, lat):
                records.append((ts, lon, lat))
    records.sort(key=lambda x: x[0])
    return taxi_id, records


def split_into_segments(records, max_gap_s=300, max_speed_ms=MAX_SPEED_MS):
    """Split a trajectory into continuous segments."""
    segments = []
    current = [records[0]]
    for prev, curr in zip(records, records[1:]):
        dt = (curr[0] - prev[0]).total_seconds()
        if dt <= 0:
            continue
        dist = haversine_m(prev[1], prev[2], curr[1], curr[2])
        speed = dist / dt
        if dt > max_gap_s or speed > max_speed_ms:
            if len(current) >= MIN_POINTS:
                segments.append(current)
            current = [curr]
        else:
            current.append(curr)
    if len(current) >= MIN_POINTS:
        segments.append(current)
    return segments


def main():
    args = parse_args()
    os.makedirs(args.out_dir, exist_ok=True)
    out_file = os.path.join(args.out_dir, "segments.tsv")

    seg_id = 0
    written = 0

    with open(out_file, "w", newline="", encoding="utf-8") as fout:
        writer = csv.writer(fout, delimiter="\t")
        writer.writerow(["seg_id", "taxi_id", "start_lon", "start_lat",
                         "end_lon", "end_lat", "start_time", "end_time",
                         "num_points"])

        for fname in sorted(os.listdir(args.raw_dir)):
            if not fname.endswith(".txt"):
                continue
            fpath = os.path.join(args.raw_dir, fname)
            taxi_id, records = read_taxi_file(fpath)
            if len(records) < MIN_POINTS:
                continue
            for seg in split_into_segments(records):
                if len(seg) < args.min_seg_len:
                    continue
                writer.writerow([
                    seg_id, taxi_id,
                    seg[0][1], seg[0][2],
                    seg[-1][1], seg[-1][2],
                    seg[0][0].isoformat(), seg[-1][0].isoformat(),
                    len(seg),
                ])
                seg_id += 1
                written += 1

    print(f"T-Drive preprocessing complete: {written} segments → {out_file}")


if __name__ == "__main__":
    main()
