import os
import uuid
import threading
from typing import Optional, List

import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks, Form
from pydantic import BaseModel
from shapely import wkt
from shapely.errors import WKTReadingError

from parti1_local import partition_file, PARTITION_DIR, UPLOAD_DIR
from parti2_local import jaccard_local, filter_bbox

app = FastAPI(title="Local Spatial Jaccard")

# 简单内存任务表
TASKS = {}
RESULTS = {}
LOCK = threading.Lock()


class BBox(BaseModel):
    minx: float
    miny: float
    maxx: float
    maxy: float


class TaskCreate(BaseModel):
    dataset_a: str
    dataset_b: str
    bbox: Optional[BBox] = None
    grids: Optional[List[str]] = None  # 指定 grid_id 列表；为空则自动按交集网格


@app.post("/api/datasets/upload")
async def upload_dataset(prefix: str = Form(...), file: UploadFile = File(...)):
    """上传 CSV 并分块到 partitioned_data/{prefix}"""
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    filepath = os.path.join(UPLOAD_DIR, f"{prefix}_{file.filename}")
    with open(filepath, "wb") as f:
        f.write(await file.read())
    part_dir = partition_file(filepath, prefix)
    return {"prefix": prefix, "path": part_dir}


@app.post("/api/tasks")
def create_task(body: TaskCreate, background_tasks: BackgroundTasks):
    """创建计算任务"""
    task_id = str(uuid.uuid4())
    with LOCK:
        TASKS[task_id] = {"status": "PENDING", "error": None}
    background_tasks.add_task(run_task, task_id, body)
    return {"task_id": task_id, "status": "PENDING"}


def run_task(task_id: str, body: TaskCreate):
    with LOCK:
        TASKS[task_id]["status"] = "RUNNING"
    try:
        result = compute_stats_internal(body.dataset_a, body.dataset_b, body.bbox, body.grids)
        with LOCK:
            RESULTS[task_id] = result
            TASKS[task_id]["status"] = "DONE"
    except Exception as e:
        with LOCK:
            TASKS[task_id]["status"] = "FAILED"
            TASKS[task_id]["error"] = str(e)


@app.get("/api/tasks/{task_id}")
def get_task(task_id: str):
    if task_id not in TASKS:
        raise HTTPException(status_code=404, detail="task not found")
    resp = {"task_id": task_id, **TASKS[task_id]}
    if task_id in RESULTS:
        resp["result"] = RESULTS[task_id]
    return resp


@app.get("/api/results/{task_id}")
def get_result(task_id: str):
    if task_id not in RESULTS:
        raise HTTPException(status_code=404, detail="result not ready")
    return RESULTS[task_id]


def _load_polygons(dataset: str, bbox, limit: int, grids: Optional[List[str]] = None):
    """按 bbox 读取多边形，返回 GeoJSON 结构，带 area 和 id。"""
    base = os.path.join(PARTITION_DIR, dataset)
    if not os.path.isdir(base):
        raise ValueError("数据集不存在，请先上传并分块")

    if grids is None:
        # 按网格 ID 排序：先按 x 再按 y，确保 0_0 排在前面
        grid_set = {f[:-4] for f in os.listdir(base) if f.endswith(".csv")}
        grids = sorted(grid_set, key=lambda g: tuple(map(int, g.split("_"))))
    feats = []
    minx, miny, maxx, maxy = bbox
    for gid in grids:
        path = os.path.join(base, f"{gid}.csv")
        if not os.path.exists(path):
            continue
        df = pd.read_csv(path)
        if "geometry" not in df.columns or "id" not in df.columns:
            continue
        for _, row in df.iterrows():
            if len(feats) >= limit:
                return feats
            geom_str = str(row["geometry"]).strip().replace('"', "")
            try:
                geom = wkt.loads(geom_str)
            except WKTReadingError:
                continue
            bx = geom.bounds
            if bx[2] < minx or bx[0] > maxx or bx[3] < miny or bx[1] > maxy:
                continue
            feats.append(
                {
                    "id": row["id"],
                    "area": geom.area,
                    "geometry": geom.__geo_interface__,
                }
            )
    return feats


def compute_stats_internal(dataset_a: str, dataset_b: str, bbox: Optional[BBox], grids: Optional[List[str]]):
    base_a = os.path.join(PARTITION_DIR, dataset_a)
    base_b = os.path.join(PARTITION_DIR, dataset_b)
    if not os.path.isdir(base_a) or not os.path.isdir(base_b):
        raise ValueError("数据集不存在，请先上传并分块")

    grid_list = grids
    if not grid_list:
        grids_a = {f[:-4] for f in os.listdir(base_a) if f.endswith(".csv")}
        grids_b = {f[:-4] for f in os.listdir(base_b) if f.endswith(".csv")}
        # 按网格 ID 排序：先按 x 再按 y，确保 0_0 排在前面
        grid_list = sorted(list(grids_a & grids_b), key=lambda g: tuple(map(int, g.split("_"))))

    bbox_tuple = None
    if bbox:
        bbox_tuple = (bbox.minx, bbox.miny, bbox.maxx, bbox.maxy)

    # 使用集合记录已计算过的交集对，避免重复计数
    intersection_pairs = set()
    total = {"area_a": 0.0, "area_b": 0.0, "area_inter": 0.0, "intersection_count": 0}
    
    for gid in grid_list:
        pa = os.path.join(base_a, f"{gid}.csv")
        pb = os.path.join(base_b, f"{gid}.csv")
        if not (os.path.exists(pa) and os.path.exists(pb)):
            continue
        
        # 加载数据并获取 id 列
        df_a = pd.read_csv(pa)
        df_b = pd.read_csv(pb)
        if "id" not in df_a.columns or "id" not in df_b.columns:
            # 如果没有 id，回退到原来的逻辑
            res = jaccard_local(pa, pb, bbox_tuple)
            total["area_a"] += res["area_a"]
            total["area_b"] += res["area_b"]
            total["area_inter"] += res["area_inter"]
            total["intersection_count"] += res["intersection_count"]
            continue
        
        # 解析几何
        df_a["geom"] = df_a["geometry"].astype(str).str.replace('"', "").apply(wkt.loads)
        df_b["geom"] = df_b["geometry"].astype(str).str.replace('"', "").apply(wkt.loads)
        
        if bbox_tuple:
            df_a = filter_bbox(df_a, bbox_tuple)
            df_b = filter_bbox(df_b, bbox_tuple)
        
        # 计算面积
        df_a["area"] = df_a["geom"].apply(lambda g: g.area)
        df_b["area"] = df_b["geom"].apply(lambda g: g.area)
        total["area_a"] += df_a["area"].sum()
        total["area_b"] += df_b["area"].sum()
        
        # 使用 R-tree 加速查找
        from rtree import index
        idx = index.Index()
        for i, (_, row) in enumerate(df_b.iterrows()):
            idx.insert(i, row["geom"].bounds)
        
        # 计算交集，使用 (id_a, id_b) 作为唯一标识
        for _, row_a in df_a.iterrows():
            id_a = str(row_a["id"])
            geom_a = row_a["geom"]
            for j in idx.intersection(geom_a.bounds):
                row_b = df_b.iloc[j]
                id_b = str(row_b["id"])
                pair_key = (id_a, id_b)
                
                # 如果这对已经计算过，跳过
                if pair_key in intersection_pairs:
                    continue
                
                geom_b = row_b["geom"]
                if geom_a.intersects(geom_b):
                    inter = geom_a.intersection(geom_b)
                    if not inter.is_empty:
                        inter_area = inter.area
                        total["area_inter"] += inter_area
                        intersection_pairs.add(pair_key)
                        total["intersection_count"] += 1

    denom = total["area_a"] + total["area_b"] - total["area_inter"]
    jacc = total["area_inter"] / denom if denom > 0 else 0.0
    return {**total, "block_jaccard": jacc}


@app.get("/api/regions/polygons")
def get_polygons(
    dataset: str,
    minx: float,
    miny: float,
    maxx: float,
    maxy: float,
    limit: int = 5000,
    grids: Optional[str] = None,
):
    """
    拉取指定数据集在 bbox 内的多边形（用于前端渲染/缩略图），limit 默认 5000。
    grids 可传逗号分隔的 grid_id 列表，用于更快过滤。
    """
    grid_list = grids.split(",") if grids else None
    feats = _load_polygons(dataset, (minx, miny, maxx, maxy), limit, grid_list)
    return {"dataset": dataset, "count": len(feats), "features": feats}


@app.get("/api/regions/stats")
def get_region_stats(
    dataset_a: str,
    dataset_b: str,
    minx: Optional[float] = None,
    miny: Optional[float] = None,
    maxx: Optional[float] = None,
    maxy: Optional[float] = None,
    grids: Optional[str] = None,
):
    """
    计算指定 bbox（或全域）的 A/B 面积、交集面积、交集对数、Jaccard。
    grids 可传逗号分隔的 grid_id 列表；未传则自动按交集网格计算。
    """
    bbox_obj = None
    if None not in (minx, miny, maxx, maxy):
        bbox_obj = BBox(minx=minx, miny=miny, maxx=maxx, maxy=maxy)
    grid_list = grids.split(",") if grids else None
    res = compute_stats_internal(dataset_a, dataset_b, bbox_obj, grid_list)
    return {
        "dataset_a": dataset_a,
        "dataset_b": dataset_b,
        "bbox": bbox_obj.model_dump() if bbox_obj else None,
        **res,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

