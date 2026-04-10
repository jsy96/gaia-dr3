import os
import pyvo
import numpy as np
from astropy.table import Table

# Gaia 官方 TAP 服务
service = pyvo.dal.TAPService("https://gea.esac.esa.int/tap-server/tap")

# 配置参数
chunk_size = 500000  # 每一块处理的 OID 范围
# AllWISE OID 的大致范围是 1 到 7.5 亿左右
max_oid = 750000000 
output_dir = "allwise_download"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

def download_chunks():
    # 从 0 开始，按步长循环
    for start_id in range(0, max_oid, chunk_size):
        end_id = start_id + chunk_size - 1
        outfile = os.path.join(output_dir, f"chunk_{start_id}_{end_id}.fits")

        # --- 断点续传逻辑：如果文件已存在，直接跳过 ---
        if os.path.exists(outfile):
            print(f"跳过已存在的分块: {outfile}")
            continue

        print(f"正在下载分块: {start_id} 到 {end_id}...")

        query = f"""
        SELECT 
            g.ra, g.dec, g.parallax, g.pmra, g.pmdec, g.radial_velocity,
            a.allwise_oid, a.w1mpro, a.w2mpro
        FROM gaiadr3.allwise_best_neighbour AS h
        JOIN gaiadr3.gaia_source_lite AS g ON g.source_id = h.source_id
        JOIN gaiadr1.allwise_original_valid AS a ON a.allwise_oid = h.allwise_oid
        WHERE a.allwise_oid BETWEEN {start_id} AND {end_id}
          AND a.w1mpro < 9 
          AND a.w2mpro < 9
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
            print(f"分块 {start_id} 失败: {e}")
            # 如果失败，不保存文件，下次运行脚本会重新尝试这一块
            continue

if __name__ == "__main__":
    download_chunks()
    