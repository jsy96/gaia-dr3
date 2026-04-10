import pandas as pd
import numpy as np
from astropy_healpix import HEALPix
from astropy.coordinates import SkyCoord
import astropy.units as u

def reduce_gaia_catalog(input_file, output_file, target_count):
    print(f"开始加载数据: {input_file}...")
    
    # 1. 加载数据：添加了 pmra, pmdec, parallax
    cols = ['ra', 'dec', 'phot_g_mean_mag', 'pmra', 'pmdec', 'parallax']
    try:
        df = pd.read_csv(input_file, usecols=cols)
    except ValueError as e:
        print(f"错误: 检查CSV列名是否完全匹配。报错信息: {e}")
        return

    print(f"原始星点数: {len(df)}")

    # 2. 设置 HEALPix 参数
    nside = 512
    print(f"正在计算 HEALPix 索引 (nside={nside})...")
    
    # 计算 HEALPix 索引
    hp_obj = HEALPix(nside=nside, order='ring', frame='icrs')
    coords = SkyCoord(ra=df['ra'].values*u.deg, dec=df['dec'].values*u.deg)
    pixels = hp_obj.skycoord_to_healpix(coords)
    df['pixel_idx'] = pixels

    # 3. 排序逻辑
    print("正在进行分桶排序...")
    # 按像素和星等排序：每个像素里最亮的星排在前面
    df = df.sort_values(by=['pixel_idx', 'phot_g_mean_mag'], ascending=[True, True])
    
    # 为每个像素内的星点编号
    df['rank_in_pixel'] = df.groupby('pixel_idx').cumcount()

    # 4. 抽取逻辑
    print("正在全局优化筛选（优先保留每个 HEALPix 像素中最亮的星）...")
    # 这里的逻辑是：先拿走所有像素的第1亮，再拿所有像素的第2亮...直到凑满 target_count
    df_reduced = df.sort_values(by=['rank_in_pixel', 'phot_g_mean_mag']).head(target_count)

    # 5. 保存结果：包含所有请求的列
    print(f"筛选完成。最终星点数: {len(df_reduced)}")
    
    # 确保输出顺序清晰
    final_cols = ['ra', 'dec', 'phot_g_mean_mag', 'pmra', 'pmdec', 'parallax']
    df_reduced = df_reduced[final_cols]
    
    df_reduced.to_csv(output_file, index=False)
    print(f"结果已保存至: {output_file}")
    

# 执行转换
input_path = r'D:\star_2024-1017\star_catalog\gaia_g14\gaia_g14_filled.csv'
output_path = r'D:\star_2024-1017\star_catalog\gaia_g14\gaia_reduced_315w.csv'

reduce_gaia_catalog(input_path, output_path, target_count=3150000)

