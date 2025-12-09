import csv
import os
import shutil
import pandas as pd
from shapely.wkt import loads
from shapely.errors import WKTReadingError

# 本地路径配置
UPLOAD_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "uploads"))
PARTITION_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "partitioned_data"))
GLOBAL_BOUNDS = (-120, -120, 120, 120)
NUM_DIVISIONS = 5

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(PARTITION_DIR, exist_ok=True)


def init_partition_dirs(prefix: str):
    """初始化分块目录（清空后重建）"""
    dir_path = os.path.join(PARTITION_DIR, prefix)
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)
    os.makedirs(dir_path, exist_ok=True)
    return dir_path


def get_grid_id(geom, num_divisions: int = NUM_DIVISIONS) -> str:
    """根据几何最小坐标计算网格 ID"""
    min_x, min_y, max_x, max_y = GLOBAL_BOUNDS
    cell_x = (max_x - min_x) / num_divisions
    cell_y = (max_y - min_y) / num_divisions
    gx = int((geom.bounds[0] - min_x) // cell_x)
    gy = int((geom.bounds[1] - min_y) // cell_y)
    gx = max(0, min(gx, num_divisions - 1))
    gy = max(0, min(gy, num_divisions - 1))
    return f"{gx}_{gy}"


def partition_file(input_path: str, prefix: str):
    """
    将 CSV 按 grid_id 分块到 partitioned_data/{prefix} 下。
    CSV 需包含列 id, geometry；geometry 为 WKT。
    """
    start_time = pd.Timestamp.now()
    partition_dir = init_partition_dirs(prefix)
    file_handles = {}
    total_rows = 0
    invalid_count = 0

    df = pd.read_csv(input_path)
    if "id" not in df.columns or "geometry" not in df.columns:
        raise ValueError("CSV必须包含'id'和'geometry'列")

    for _, row in df.iterrows():
        total_rows += 1
        id_val = str(row["id"]).strip()
        geom_str = str(row["geometry"]).strip().strip('"').replace('"', "")
        try:
            geom = loads(geom_str)
            if not geom.is_valid:
                geom = geom.buffer(0)
            if not geom.is_valid:
                raise ValueError("无法修复的无效几何")
        except (WKTReadingError, ValueError) as e:
            invalid_count += 1
            print(f"跳过无效几何: {id_val} | 错误: {str(e)}")
            continue

        grid_id = get_grid_id(geom)
        file_path = os.path.join(partition_dir, f"{grid_id}.csv")
        if file_path not in file_handles:
            handle = open(file_path, "w", encoding="utf-8", newline="")
            writer = csv.writer(handle)
            writer.writerow(["id", "geometry", "grid_id"])
            file_handles[file_path] = (handle, writer)
        handle, writer = file_handles[file_path]
        writer.writerow([id_val, geom_str, grid_id])

    for h, _ in file_handles.values():
        h.close()
    elapsed = (pd.Timestamp.now() - start_time).total_seconds()
    print(
        f"✅ 分块完成: {os.path.basename(input_path)} -> {prefix}, "
        f"有效行 {total_rows - invalid_count}, 耗时 {elapsed:.2f}s"
    )
    return partition_dir


if __name__ == "__main__":
    # 简单示例：使用采样后的 5 万行数据
    partition_file("sample_a_50k.csv", "data_a")
    partition_file("sample_b_50k.csv", "data_b")

