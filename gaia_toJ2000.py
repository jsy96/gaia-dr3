import pandas as pd
import astropy.units as u
from astropy.coordinates import SkyCoord, Distance # 增加导入 Distance
import numpy as np
from astropy.time import Time


# 1. 读取导航星表文件
file_path = r'D:\star_2024-1017\star_catalog\gaia_g14\gaia_g14_filled.csv'
df = pd.read_csv(file_path)

# 2. 预处理缺失值 (重要：坐标转换不允许有 NaN)
# 如果 pmra/pmdec 为空，则认为该星没有自行的观测数据，填补为 0
df['pmra'] = df['pmra'].fillna(0)
df['pmdec'] = df['pmdec'].fillna(0)
df['parallax'] = df['parallax'].fillna(0)


# 1. 先处理视差中的非正值（负值或零会导致计算距离时出错）
# 将所有小于等于 0 的视差设为一个极小的正值（如 0.01 毫角秒）
plx_values = df['parallax'].values
plx_values[plx_values <= 0] = 0.01

# 2. 修改后的 SkyCoord 部分
coords = SkyCoord(ra=df['ra'].values * u.deg, 
                  dec=df['dec'].values * u.deg, 
                  # 传入处理后的视差，并添加 allow_negative=True 参数
                  distance=Distance(parallax=plx_values * u.mas, allow_negative=True), 
                  pm_ra_cosdec=df['pmra'].values * u.mas/u.yr, 
                  pm_dec=df['pmdec'].values * u.mas/u.yr,
                  radial_velocity=np.zeros(len(df)) * u.km/u.s,
                  frame='icrs', 
                  obstime=Time(2016.0, format='jyear'))



# 4. 转换到 J2000
# apply_space_motion 会根据自行和时间差（16.0年）回溯位置
print("正在计算历元转换...")
coords_j2000 = coords.apply_space_motion(new_obstime=Time(2000.0, format='jyear'))

# 5. 【修改部分】替换原有坐标列
# 首先删除旧的 ra, dec 列
df = df.drop(columns=['ra', 'dec'])

# 5. 将结果写回 Dataframe
df['ra_j2000'] = coords_j2000.ra.deg
df['dec_j2000'] = coords_j2000.dec.deg

# 6. 保存结果
output_path = r'D:\star_2024-1017\star_catalog\gaia_g14\gaia_nav_j2000.csv'
# 调整列顺序，让 ra_j2000 和 dec_j2000 排在前面（可选）
cols = ['ra_j2000', 'dec_j2000'] + [c for c in df.columns if c not in ['ra_j2000', 'dec_j2000']]
df = df[cols]

df.to_csv(output_path, index=False)

print(f"转换完成！共处理 {len(df)} 颗恒星。")
print(f"结果已保存至: {output_path}")

