import os
import pyvo
import numpy as np
from astropy.table import Table

# Gaia 官方 TAP 服务
# service = pyvo.dal.TAPService("https://archives.esac.esa.int/gaia/tap")
service = pyvo.dal.TAPService("https://gea.esac.esa.int/tap-server/tap")

# 配置参数
total_count = 1811709771
step =  500000
output_dir = "D:/a_star_catalog/gaia_dr3_chunks"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

def download_chunks():
    # 从 0 开始，按步长循环
    for start_idx in range(0, total_count, step):
        end_idx = min(start_idx + step - 1, total_count - 1)
        outfile = os.path.join(output_dir, f"chunk_{start_idx}_{end_idx}.fits")

        # --- 断点续传逻辑：如果文件已存在，直接跳过 ---
        if os.path.exists(outfile):
            print(f"跳过已存在的分块: {outfile}")
            continue

        print(f"正在下载分块: {start_idx} 到 {end_idx}...")

        query = f"""
        SELECT 
            g.ra, g.dec, g.parallax, g.pmra, g.pmdec, g.radial_velocity,
            g.phot_g_mean_mag, g.source_id
        FROM gaiadr3.gaia_source_lite AS g 
        WHERE g.phot_g_mean_mag < 14
        AND g.random_index BETWEEN {start_idx} AND {end_idx}
        """

        try:
            # 使用异步查询（针对大数据集更稳定）
            job = service.run_async(query)
            tbl = job.to_table()
            
            if len(tbl) > 0:
                # 修复你在上一个问题中遇到的字符串格式问题
                for colname in tbl.colnames:
                    if tbl[colname].dtype.kind in ('O', 'S', 'U'):
                        tbl[colname] = np.array(tbl[colname], dtype=str)
                        tbl[colname].format = None
                
                tbl.write(outfile, overwrite=True)
                print(f"  保存成功: 获得 {len(tbl)} 条记录")
            else:
                print("  该块内无符合条件的源")
                # 也可以创建一个空的标记文件，防止下次重复扫这个空块
                with open(outfile + ".empty", 'w') as f: f.write("empty")

        except Exception as e:
            print(f"分块 {start_idx} 失败: {e}")
            # 如果失败，不保存文件，下次运行脚本会重新尝试这一块
            continue
        
    

if __name__ == "__main__":
    download_chunks()
    