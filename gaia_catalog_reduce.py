import pandas as pd
import numpy as np
from astropy_healpix import HEALPix
from astropy.coordinates import ICRS
import astropy.units as u

# ---------------- 配置参数 ----------------
input_path = r'D:\a_star_catalog\gaia_dr3_merged.csv'
output_path = r'D:\a_star_catalog\gaia_dr3_final_uniform2.csv'

# NSIDE 决定了全天划分的格子数量。计算公式：像素总数 = 12 * (nside^2)
# nside=32 -> 12,288 个像素 (每个像素约 3.3 平方度)
# nside=64 -> 49,152 个像素 (每个像素约 0.8 平方度)
# # 如果你想要最终星表大约 1-2 万颗星，建议选 32 或 64
# NSIDE = 64 
# STARS_PER_PIXEL = 1  # 每个格子里保留几颗星（通常选 1 即可实现极佳的均匀度）
# ---------------- 修改后的配置参数 ----------------
# nside=128 -> 全天像素总数 = 12 * (128^2) = 196,608 个像素
NSIDE = 128 

# 每个像素保留 1 颗星，理论最大值即为 196,608 颗
STARS_PER_PIXEL = 1  
# --------------------------------------------------
# ------------------------------------------

print(f"正在读取数据 (16M rows)...")
df = pd.read_csv(input_path)

# 1. 初始化 HEALPix 对象
hp = HEALPix(nside=NSIDE, order='ring', frame=ICRS())

# 2. 为每颗星计算所在的像素索引 (Pixel ID)
print("正在计算全天像素分布...")
df['pixel_id'] = hp.lonlat_to_healpix(df['ra'].values * u.deg, 
                                      df['dec'].values * u.deg)

# 3. 定义优先级排序逻辑
# 规则：5-9等最优先(score=0)，其他15等以下次之(score=1)，其余靠后(score=2)
# 在同一个优先级内，按星等从小到大排列
print("正在执行排序过滤策略...")

def get_priority(mag):
    if mag <= 7.0:
        return 0
    elif mag < 10.0:
        return 1
    else:
        return 2

# 使用向量化操作加速
df['priority'] = 3
df.loc[(df['phot_g_mean_mag'] <= 7.0 ) & ( df['parallax'] > 5.1e-7), 'priority'] = 0
df.loc[(df['phot_g_mean_mag'] < 10.0 ) & ( df['parallax'] > 5.1e-7), 'priority'] = 1
df.loc[(df['phot_g_mean_mag'] < 14.0 ) & ( df['parallax'] > 5.1e-7), 'priority'] = 2

# 4. 在每个像素内进行分组并提取前 N 颗星
# 先按像素分组，再按优先级和星等排序，最后取每组的前 STARS_PER_PIXEL 个
df_sorted = df.sort_values(by=['pixel_id', 'priority', 'phot_g_mean_mag'])

# 核心抽稀步骤
df_uniform = df_sorted.groupby('pixel_id').head(STARS_PER_PIXEL)

# 5. 清理多余列并保存
df_final = df_uniform.drop(columns=['pixel_id', 'priority'])
df_final.to_csv(output_path, index=False)

print(f"抽稀完成！")
print(f"原始恒星数: {len(df)}")
print(f"均匀化后恒星数: {len(df_final)}")
print(f"结果已保存至: {output_path}")

