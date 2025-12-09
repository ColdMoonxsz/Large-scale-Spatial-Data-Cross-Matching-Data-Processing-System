import pandas as pd

def convert(src_path: str, dst_path: str, prefix: str):
    df = pd.read_csv(src_path)
    if "geometry" not in df.columns:
        raise ValueError(f"{src_path} 缺少 geometry 列")
    df = df.reset_index(drop=True)
    df["id"] = [f"{prefix}{i}" for i in range(len(df))]
    df_out = df[["id", "geometry"]]
    df_out.to_csv(dst_path, index=False, encoding="utf-8")
    print(f"✅ {src_path} -> {dst_path}, rows: {len(df_out)}")

convert("0_1A.csv", "dataset_a_v2.csv", "A")
convert("0_1B.csv", "dataset_b_v2.csv", "B")