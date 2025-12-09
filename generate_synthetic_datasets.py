"""
Generate two synthetic polygon datasets (A and B), each with 100 polygons.
Polygons are axis-aligned squares within a 0~15 extent, and B is slightly
shifted from A so every pair overlaps.

Output:
- dataset_a.csv
- dataset_b.csv

Schema: id, geometry (WKT)
"""

import csv
from pathlib import Path
from typing import Iterable, Tuple


def square_wkt(center: Tuple[float, float], size: float) -> str:
    """Return a WKT polygon string for an axis-aligned square."""
    cx, cy = center
    h = size / 2
    coords = [
        (cx - h, cy - h),
        (cx + h, cy - h),
        (cx + h, cy + h),
        (cx - h, cy + h),
        (cx - h, cy - h),
    ]
    ring = ", ".join(f"{x:.6f} {y:.6f}" for x, y in coords)
    return f"POLYGON (({ring}))"


def generate_centers(n: int) -> Iterable[Tuple[float, float]]:
    """Deterministic grid-like centers within 0~15 for reproducibility."""
    # 10x10 grid spaced by 1.2 units, offset to stay inside bounds.
    step = 1.2
    start = 1.5
    for i in range(n):
        gx = i % 10
        gy = i // 10
        yield (start + gx * step, start + gy * step)


def write_dataset(path: Path, rows: Iterable[Tuple[str, str]]):
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "geometry"])
        writer.writerows(rows)


def main():
    count = 100
    size_a = 0.9
    size_b = 0.9
    shift = 0.25  # small shift to keep overlaps but not identical

    centers = list(generate_centers(count))

    rows_a = []
    rows_b = []
    for idx, (cx, cy) in enumerate(centers):
        wkt_a = square_wkt((cx, cy), size_a)
        # shift B a bit diagonally so it overlaps with A
        wkt_b = square_wkt((cx + shift, cy + shift), size_b)
        rows_a.append((f"A{idx}", wkt_a))
        rows_b.append((f"B{idx}", wkt_b))

    write_dataset(Path("dataset_a_test.csv"), rows_a)
    write_dataset(Path("dataset_b_test.csv"), rows_b)
    print("✅ Generated dataset_a.csv and dataset_b.csv with 100 polygons each, all overlapping.")


if __name__ == "__main__":
    main()


