"""
Preprocess raw CD-Taxi (Chengdu) dataset into trajectory segments.

Raw CD-Taxi format (CSV):
    order_id, driver_id, passenger_status, longitude, latitude, timestamp

Output format (TSV):
    segment_id  driver_id  start_lon  start_lat  end_lon  end_lat  start_time  end_time  num_points
"""

import os
import argparse
import csv
from datetime import datetime

CDTAXI_BBOX = {
    "lon_min": 103.92, "lon_max": 104.20,
    "lat_min": 30.52,  "lat_max": 30.78,
}

MAX_SPEED_MS = 55.0
MIN_POINTS   = 2


def parse_args():
    parser = argparse.ArgumentParser(description="Preprocess CD-Taxi dataset")
    parser.add_argument("--raw_dir",  required=True, help="Directory with raw .csv files")
    parser.add_argument("--out_dir",  required=True, help="Output directory")
    parser.add_argument("--min_seg_len", type=int, default=2,
                        help="Minimum GPS points per segment (default: 2)")
    return parser.parse_args()


def in_bbox(lon: float, lat: float) -> bool:
    return (CDTAXI_BBOX["lon_min"] <= lon <= CDTAXI_BBOX["lon_max"] and
            CDTAXI_BBOX["lat_min"] <= lat <= CDTAXI_BBOX["lat_max"])


def haversine_m(lon1, lat1, lon2, lat2) -> float:
    import math
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi  = math.radians(lat2 - lat1)
    dlam  = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def read_cdtaxi_file(path: str):
    """Return dict[order_id -> list[(ts, lon, lat)]] for one raw file."""
    orders: dict = {}
    with open(path, encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 6:
                continue
            try:
                order_id  = row[0].strip()
                driver_id = row[1].strip()
                lon       = float(row[3])
                lat       = float(row[4])
                ts        = datetime.fromtimestamp(int(row[5]))
            except (ValueError, IndexError):
                continue
            if not in_bbox(lon, lat):
                continue
            if order_id not in orders:
                orders[order_id] = {"driver_id": driver_id, "points": []}
            orders[order_id]["points"].append((ts, lon, lat))
    return orders


def split_into_segments(points, max_gap_s=300, max_speed_ms=MAX_SPEED_MS):
    points = sorted(points, key=lambda x: x[0])
    if len(points) < MIN_POINTS:
        return []
    segments = []
    current = [points[0]]
    for prev, curr in zip(points, points[1:]):
        dt = (curr[0] - prev[0]).total_seconds()
        if dt <= 0:
            continue
        dist = haversine_m(prev[1], prev[2], curr[1], curr[2])
        speed = dist / dt if dt > 0 else 0
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
        writer.writerow(["seg_id", "driver_id", "start_lon", "start_lat",
                         "end_lon", "end_lat", "start_time", "end_time",
                         "num_points"])

        for fname in sorted(os.listdir(args.raw_dir)):
            if not fname.endswith(".csv"):
                continue
            fpath = os.path.join(args.raw_dir, fname)
            orders = read_cdtaxi_file(fpath)
            for order_id, info in orders.items():
                for seg in split_into_segments(info["points"]):
                    if len(seg) < args.min_seg_len:
                        continue
                    writer.writerow([
                        seg_id, info["driver_id"],
                        seg[0][1], seg[0][2],
                        seg[-1][1], seg[-1][2],
                        seg[0][0].isoformat(), seg[-1][0].isoformat(),
                        len(seg),
                    ])
                    seg_id += 1
                    written += 1

    print(f"CD-Taxi preprocessing complete: {written} segments → {out_file}")


if __name__ == "__main__":
    main()
