"""
Quick sampler: take the first N lines from dataset_a.csv / dataset_b.csv.
Produces sample_a_50k.csv and sample_b_50k.csv in the same directory.
"""
import itertools
from pathlib import Path


def sample(input_path: str, output_path: str, n: int = 50_000):
    src = Path(input_path)
    dst = Path(output_path)
    with src.open("r", encoding="utf-8") as fin, dst.open("w", encoding="utf-8") as fout:
        for line in itertools.islice(fin, n):
            fout.write(line)
    print(f"✅ {src.name} -> {dst.name}, sampled {n} lines")


if __name__ == "__main__":
    sample("dataset_a.csv", "sample_a_50k.csv", 50_000)
    sample("dataset_b.csv", "sample_b_50k.csv", 50_000)

