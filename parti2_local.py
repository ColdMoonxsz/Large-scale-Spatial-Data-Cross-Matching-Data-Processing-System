import pandas as pd
from shapely import wkt
from rtree import index


def load_df(path: str) -> pd.DataFrame:
    """读取 CSV，去引号并解析 WKT"""
    df = pd.read_csv(path)
    df["geom"] = df["geometry"].astype(str).str.replace('"', "").apply(wkt.loads)
    df["area"] = df["geom"].apply(lambda g: g.area)
    return df


def filter_bbox(df: pd.DataFrame, bbox):
    """按 bbox 过滤几何"""
    minx, miny, maxx, maxy = bbox
    return df[
        df["geom"].apply(
            lambda g: g.bounds[2] >= minx
            and g.bounds[0] <= maxx
            and g.bounds[3] >= miny
            and g.bounds[1] <= maxy
        )
    ]


def jaccard_local(path_a: str, path_b: str, bbox=None):
    """单分区 Jaccard 计算"""
    a = load_df(path_a)
    b = load_df(path_b)
    if bbox:
        a = filter_bbox(a, bbox)
        b = filter_bbox(b, bbox)

    idx = index.Index()
    for i, g in b["geom"].items():
        idx.insert(i, g.bounds)

    inter_area = 0.0
    inter_count = 0
    for _, row in a.iterrows():
        for j in idx.intersection(row["geom"].bounds):
            g2 = b.loc[j, "geom"]
            if row["geom"].intersects(g2):
                inter = row["geom"].intersection(g2)
                if not inter.is_empty:
                    inter_area += inter.area
                    inter_count += 1

    area_a = a["area"].sum()
    area_b = b["area"].sum()
    denom = area_a + area_b - inter_area
    jacc = inter_area / denom if denom > 0 else 0
    return {
        "block_jaccard": jacc,
        "area_a": area_a,
        "area_b": area_b,
        "area_inter": inter_area,
        "intersection_count": inter_count,
    }


if __name__ == "__main__":
    # 简单示例
    res = jaccard_local("partitioned_data/data_a/0_1.csv", "partitioned_data/data_b/0_1.csv")
    print(res)

