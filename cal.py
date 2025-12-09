import pandas as pd
from shapely import wkt
import sys
import csv
import time
import os

# ================= 配置区域 =================
# 1. 你的 Spark 运行结果 (包含交集信息)
RESULT_FILE = r"merged_spatial_join.csv"

# 2. 原始的大型数据集 (用于计算分母)
DATASET_A = r"dataset_a.csv"
DATASET_B = r"dataset_b.csv"

# 3. 性能配置
CHUNK_SIZE = 10000  # 每次读取行数
# ===========================================

# 【Windows 兼容性修复】
try:
    csv.field_size_limit(2147483647)
except Exception:
    pass

def get_intersection_sum(filepath):
    """从结果文件中计算交集总面积"""
    print(f"[-] 正在读取结果文件: {filepath} ...")
    if not os.path.exists(filepath):
        print(f"错误: 文件不存在!")
        return 0.0
    
    start = time.time()
    try:
        # 结果文件通常没有表头: id_a, id_b, area_a, area_b, intersection_area
        # 我们只需要第 5 列 (索引 4)
        df = pd.read_csv(filepath, header=None, usecols=[4], names=['inter_area'])
        total = df['inter_area'].sum()
        
        print(f"  -> 读取完成，耗时 {time.time()-start:.2f}s")
        print(f"  -> 【交集总面积】: {total:,.4f}")
        return total
    except Exception as e:
        print(f"读取结果文件失败: {e}")
        return 0.0

def get_raw_dataset_area(filepath, name):
    """分块读取原始大文件计算总面积"""
    print(f"[-] 正在计算 {name} 总面积...")
    if not os.path.exists(filepath):
        print(f"错误: 文件 {filepath} 不存在")
        return 0.0
        
    total_area = 0.0
    start = time.time()
    
    try:
        # header=0 表示第一行是表头
        reader = pd.read_csv(filepath, chunksize=CHUNK_SIZE, header=0, engine='c')
        
        for chunk in reader:
            # 自动找 geometry 列
            if 'geometry' in chunk.columns:
                wkt_col = chunk['geometry']
            elif len(chunk.columns) > 1:
                wkt_col = chunk.iloc[:, 1] # 假设在第2列
            else:
                continue

            for wkt_str in wkt_col.astype(str):
                try:
                    if len(wkt_str) < 10: continue # 跳过空或过短的
                    geom = wkt.loads(wkt_str)
                    total_area += geom.area if geom.is_valid else geom.buffer(0).area
                except:
                    pass
                    
    except Exception as e:
        print(f"处理 {name} 时出错: {e}")
        return 0.0
        
    print(f"  -> {name} 完成，耗时 {time.time()-start:.2f}s")
    print(f"  -> 面积: {total_area:,.4f}")
    return total_area

def main():
    print("="*60)
    print("        全自动 Global Jaccard 计算器        ")
    print("="*60)

    # 1. 先算交集 (最快)
    inter_area = get_intersection_sum(RESULT_FILE)
    if inter_area == 0:
        print("\n错误: 交集面积为 0 或读取失败，程序终止。")
        return

    print("-" * 30)

    # 2. 算 Dataset A
    area_a = get_raw_dataset_area(DATASET_A, "Dataset A")
    
    # 3. 算 Dataset B
    area_b = get_raw_dataset_area(DATASET_B, "Dataset B")

    # 4. 汇总计算
    union_area = area_a + area_b - inter_area
    jaccard = inter_area / union_area if union_area > 0 else 0

    print("\n" + "="*60)
    print("             最 终 结 果 报 告              ")
    print("="*60)
    print(f" Dataset A 总面积 : {area_a:,.4f}")
    print(f" Dataset B 总面积 : {area_b:,.4f}")
    print(f" 交集总面积 (I)   : {inter_area:,.4f}")
    print(f" 并集总面积 (U)   : {union_area:,.4f}")
    print("-" * 60)
    print(f" ★ Global Jaccard : {jaccard:.10f}")
    print("="*60)
    input("按回车键退出...")

if __name__ == "__main__":
    main()