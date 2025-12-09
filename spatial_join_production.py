import os
import sys
import math
import time
import json
import csv
from io import StringIO
from pyspark.sql import SparkSession
from typing import List, Dict, Tuple, Optional

# ==========================================
# 0. 配置参数
# ==========================================
# 空间边界 (保持不变)
GLOBAL_BOUNDS = (-115.540436, -59.913738, 114.821777, 54.987061)

# *** 针对大数据量的优化 ***
NUM_DIVISIONS = 50      # 增加网格密度到 50x50 = 2500 个网格，减少每个任务的计算压力
EPSILON = 1e-9          

# 预计算网格参数
MIN_X, MIN_Y, MAX_X, MAX_Y = GLOBAL_BOUNDS
WIDTH = MAX_X - MIN_X
HEIGHT = MAX_Y - MIN_Y
CELL_W = WIDTH / NUM_DIVISIONS
CELL_H = HEIGHT / NUM_DIVISIONS

# ==========================================
# 1. 核心函数 (保持不变，仅优化引用)
# ==========================================

def parse_csv_line(line: str) -> Optional[Tuple[str, str]]:
    """解析 CSV 行"""
    if line.startswith("id,geometry"): return None
    try:
        reader = csv.reader(StringIO(line))
        row = next(reader)
        if len(row) < 2: return None
        raw_id = row[0].strip()
        raw_geom = row[1].strip()
        if raw_geom.startswith('"') and raw_geom.endswith('"'):
            raw_geom = raw_geom[1:-1]
        return raw_id, raw_geom
    except Exception:
        return None

def map_to_grid_key(iter_rows, role: str, grid_params: Dict) -> List[Tuple[str, Tuple]]:
    """将几何体映射到网格索引 (支持多重映射)"""
    from shapely import wkt
    import math 
    
    results = []
    _MIN_X, _MIN_Y = grid_params['MIN_X'], grid_params['MIN_Y']
    _CELL_W, _CELL_H = grid_params['CELL_W'], grid_params['CELL_H']
    _NUM_DIVISIONS = grid_params['NUM_DIVISIONS']
    _EPSILON = grid_params['EPSILON']
    
    for row in iter_rows:
        if not row: continue
        obj_id, wkt_str = row
        
        try:
            geom = wkt.loads(wkt_str)
            if not geom.is_valid: geom = geom.buffer(0)
            if geom.is_empty: continue
            
            min_x, min_y, max_x, max_y = geom.bounds
            area = geom.area 
            
            start_col = max(0, math.floor((min_x - _MIN_X) / _CELL_W))
            start_row = max(0, math.floor((min_y - _MIN_Y) / _CELL_H))
            end_col = min(_NUM_DIVISIONS - 1, math.floor((max_x - _MIN_X) / _CELL_W + _EPSILON))
            end_row = min(_NUM_DIVISIONS - 1, math.floor((max_y - _MIN_Y) / _CELL_H + _EPSILON))

            for r in range(start_row, end_row + 1):
                for c in range(start_col, end_col + 1):
                    grid_key = f"{r}_{c}"
                    # 只保留必要数据以节省传输带宽
                    results.append((grid_key, (obj_id, wkt_str, area, role)))
        except Exception:
            pass
            
    return results

def calculate_intersection(pair_data) -> Optional[str]:
    """
    计算相交。
    注意：返回格式改为 CSV 字符串，方便直接写入文件
    """
    try:
        from shapely import wkt
    except ImportError:
        return None
    
    _, (item_a, item_b) = pair_data
    id_a, wkt_a, area_a, _ = item_a
    id_b, wkt_b, area_b, _ = item_b
    
    if id_a == id_b: return None

    try:
        g1 = wkt.loads(wkt_a)
        g2 = wkt.loads(wkt_b)
        if not g1.is_valid: g1 = g1.buffer(0)
        if not g2.is_valid: g2 = g2.buffer(0)
        
        if g1.intersects(g2):
            inter = g1.intersection(g2)
            if not inter.is_empty and inter.area > 1e-9:
                # 返回格式: id_a, id_b, area_a, area_b, intersection_area
                return f"{id_a},{id_b},{area_a},{area_b},{inter.area}"
    except Exception:
        pass
    return None

# ==========================================
# 3. 主程序
# ==========================================

if __name__ == "__main__":
    # 检查参数
    if len(sys.argv) < 4:
        print("Usage: spark-submit spatial_join_production.py <input_path_1> <input_path_2> <output_path>")
        sys.exit(1)

    input_path_1 = sys.argv[1]
    input_path_2 = sys.argv[2]
    output_path = sys.argv[3]

    print(f"正在处理: \nInput 1: {input_path_1}\nInput 2: {input_path_2}\nOutput: {output_path}")

    # 初始化 Spark (让 spark-submit 控制资源配置)
    spark = SparkSession.builder.appName("FullScaleSpatialJoin").getOrCreate()
    sc = spark.sparkContext

    # 广播变量
    grid_params = {
        'MIN_X': MIN_X, 'MIN_Y': MIN_Y, 
        'CELL_W': CELL_W, 'CELL_H': CELL_H, 
        'NUM_DIVISIONS': NUM_DIVISIONS, 'EPSILON': EPSILON
    }
    bc_grid = sc.broadcast(grid_params)

    start_time = time.time()

    # 1. 读取数据 (全量)
    # 增加 minPartitions 以提高并行度，建议设为 总核数 * 3 或 文件大小/128MB
    # 假设您有 100 个核心，这里设为 200-500 是合理的
    min_parts = 200 
    rdd1 = sc.textFile(input_path_1, minPartitions=min_parts).map(parse_csv_line).filter(lambda x: x is not None)
    rdd2 = sc.textFile(input_path_2, minPartitions=min_parts).map(parse_csv_line).filter(lambda x: x is not None)

    # 2. Map Phase & Repartition
    # 分区数设为网格数的 1-2 倍比较合适，这里设为 2500
    join_partitions = NUM_DIVISIONS * NUM_DIVISIONS 
    
    rdd1_mapped = rdd1.mapPartitions(lambda i: map_to_grid_key(i, 'A', bc_grid.value)).repartition(join_partitions)
    rdd2_mapped = rdd2.mapPartitions(lambda i: map_to_grid_key(i, 'B', bc_grid.value)).repartition(join_partitions)

    # 3. Join Phase
    joined = rdd1_mapped.join(rdd2_mapped)

    # 4. Reduce/Calc Phase & Save
    # 直接计算并保存，不要 collect()!
    result_rdd = joined.map(calculate_intersection).filter(lambda x: x is not None)
    
    # 去重 (如果跨网格可能会产生重复对，使用 distinct 或者 reduceByKey 去重)
    # 对于字符串输出，distinct() 最简单
    final_rdd = result_rdd.distinct()

    # 保存结果到 HDFS 或 本地路径
    final_rdd.saveAsTextFile(output_path)

    end_time = time.time()
    print(f"任务完成！耗时: {end_time - start_time:.2f} 秒")
    print(f"结果已保存至: {output_path}")
    
    spark.stop()