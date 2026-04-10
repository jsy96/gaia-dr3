"""
merge_gaia_chunks.py

将你在 `D:/a_star_catalog/gaia_dr3_chunks` 目录下分块的 FITS 文件合并为一个文件的多种方法。

默认方法为增量写入 CSV（对内存友好）。如果你有足够内存，也可以选择将所有分块载入内存后用 astropy 写成一个 FITS。

用法示例：
    merge_chunks("D:/a_star_catalog/gaia_dr3_chunks",
                 "D:/a_star_catalog/gaia_dr3_merged.csv",
                 method='csv', deduplicate=True)

    merge_chunks("D:/a_star_catalog/gaia_dr3_chunks",
                 "D:/a_star_catalog/gaia_dr3_merged.fits",
                 method='fits_memory', deduplicate=True)

注意：
 - 如果使用 method='fits_memory' 并且数据量很大，会耗尽内存。默认推荐使用 'csv'。 
 - 脚本会跳过以 ".empty" 结尾的标记文件。
 - 合并后可选择按 source_id 去重（deduplicate=True）。

"""

import os
import glob
import numpy as np
from astropy.table import Table, vstack
import pandas as pd
import warnings


def _sanitize_table(tbl):
    """把表里可能的 bytes/object 列转换成 str，清除 format 信息（避免写入时出错）。"""
    for colname in tbl.colnames:
        dtype = tbl[colname].dtype
        if dtype.kind in ('O', 'S', 'U'):
            # astropy Table 中有时是 bytes 或 object，强制转换为 python str
            tbl[colname] = np.array(tbl[colname], dtype=str)
            try:
                tbl[colname].format = None
            except Exception:
                pass
    return tbl


def merge_chunks(input_dir,
                 output_file,
                 method='csv',
                 chunk_pattern='chunk_*.fits',
                 deduplicate=True,
                 verbose=True):
    """
    合并分块文件。

    参数：
      input_dir: 存放分块 FITS 的目录
      output_file: 要输出的合并文件路径（.csv 或 .fits），根据 method 决定扩展名
      method: 'csv'（默认，增量写入 CSV，内存友好），或 'fits_memory'（把所有分块读入内存后合并并写为 FITS）
      chunk_pattern: 匹配分块文件的 glob 模式
      deduplicate: 是否根据 source_id 去重（如果存在该列）
      verbose: 是否打印进度信息

    返回：输出文件路径（字符串）或 None（失败）
    """

    files = sorted(glob.glob(os.path.join(input_dir, chunk_pattern)))
    # 过滤标记的空块
    files = [f for f in files if not f.endswith('.empty') and os.path.exists(f)]

    if len(files) == 0:
        if verbose:
            print("未找到任何匹配的分块文件（或仅有 .empty 标记）。")
        return None

    if method == 'csv':
        # 增量写 CSV：对内存最友好
        csv_out = output_file if str(output_file).lower().endswith('.csv') else output_file + '.csv'
        first = True
        written = 0
        for fn in files:
            try:
                if verbose: print(f"读取: {fn} ...", end=' ')
                tbl = Table.read(fn)
                tbl = _sanitize_table(tbl)
                df = tbl.to_pandas()
                # 如果没有 source_id 列，也照常写入
                if first:
                    df.to_csv(csv_out, index=False, mode='w', header=True)
                    first = False
                else:
                    df.to_csv(csv_out, index=False, mode='a', header=False)
                written += len(df)
                if verbose: print(f"写入 {len(df)} 行")
            except Exception as e:
                warnings.warn(f"跳过文件 {fn}（读取/写入失败）：{e}")
                continue

        if deduplicate and os.path.exists(csv_out):
            try:
                if verbose: print("正在去重（按 source_id）...")
                # 使用 chunksize 来降低内存占用：先检测是否存在 source_id
                # 这里快速读取首行判断列名
                tmp = pd.read_csv(csv_out, nrows=0)
                if 'source_id' in tmp.columns:
                    # 读取全部并去重——对于超大文件这一步仍然可能耗内存
                    df_all = pd.read_csv(csv_out)
                    before = len(df_all)
                    df_all.drop_duplicates(subset=['source_id'], inplace=True)
                    after = len(df_all)
                    df_all.to_csv(csv_out, index=False)
                    if verbose: print(f"去重完成：{before} -> {after} 行")
                else:
                    if verbose: print("文件中无 'source_id' 列；跳过去重。")
            except Exception as e:
                warnings.warn(f"去重失败：{e}")

        if verbose: print(f"合并完成，输出: {csv_out}（大约 {written} 行）")
        return csv_out

    elif method == 'fits_memory':
        # 把所有表加载到内存，然后 vstack 写为 FITS（简单但可能耗尽内存）
        tables = []
        total = 0
        for fn in files:
            try:
                if verbose: print(f"读取: {fn} ...", end=' ')
                tbl = Table.read(fn)
                tbl = _sanitize_table(tbl)
                tables.append(tbl)
                total += len(tbl)
                if verbose: print(f"{len(tbl)} 行")
            except Exception as e:
                warnings.warn(f"跳过文件 {fn}：{e}")
                continue

        if len(tables) == 0:
            if verbose: print("没有成功读取到任何分块，终止。")
            return None

        if verbose: print("开始合并表（vstack）...")
        try:
            big = vstack(tables, metadata_conflicts='silent')
        except Exception as e:
            warnings.warn(f"vstack 失败：{e}")
            return None

        if deduplicate and 'source_id' in big.colnames:
            if verbose: print("按 source_id 去重...")
            # 保留第一次出现
            _, idx = np.unique(big['source_id'], return_index=True)
            big = big[np.sort(idx)]

        # 写入 FITS
        out_fits = output_file if str(output_file).lower().endswith('.fits') else output_file + '.fits'
        big.write(out_fits, overwrite=True)
        if verbose: print(f"合并完成，输出 FITS: {out_fits}（共 {len(big)} 行）")
        return out_fits

    else:
        raise ValueError("未知 method。可用选项：'csv' 或 'fits_memory'.")


if __name__ == '__main__':
    # 快速测试（只做示例，不会在默认运行时自动合并）
    merge_chunks("D:/a_star_catalog/gaia_dr3_chunks",
        "D:/a_star_catalog/gaia_dr3_merged.csv",
        method='csv', deduplicate=True)

