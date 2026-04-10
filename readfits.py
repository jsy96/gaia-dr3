# from astropy.table import vstack, Table
# tbl = Table.read("1.fits", format='fits')
# print(tbl['cntr'][:5])  # 显示前5个WISE ID

from astropy.table import vstack, Table
import pandas as pd
import os

def fits_to_csv(fits_file, csv_file=None):
    """
    将FITS文件转换为CSV文件
    
    参数:
        fits_file: str, FITS文件路径
        csv_file: str, 输出的CSV文件路径。如果为None，则使用与FITS文件相同的名称，扩展名改为.csv
    
    返回:
        str: 输出的CSV文件路径
    """
    # 如果未指定CSV文件名，则根据FITS文件名生成
    if csv_file is None:
        base_name = os.path.splitext(fits_file)[0]
        csv_file = base_name + '.csv'
    
    print(f"正在读取FITS文件: {fits_file}")
    # 读取FITS文件
    tbl = Table.read(fits_file, format='fits')
    
    # 将astropy Table转换为pandas DataFrame
    df = tbl.to_pandas()
    
    print(f"正在写入CSV文件: {csv_file}")
    # 写入CSV文件
    df.to_csv(csv_file, index=False)
    
    print(f"转换完成！数据行数: {len(df)}, 列数: {len(df.columns)}")
    print(f"CSV文件已保存到: {csv_file}")
    
    return csv_file

def display_fits_info(fits_file):
    """
    显示FITS文件的基本信息
    """
    print(f"\nFITS文件信息: {fits_file}")
    tbl = Table.read(fits_file, format='fits')
    print(f"数据行数: {len(tbl)}")
    print(f"列名: {list(tbl.colnames)}")
    print(f"前5行数据预览:")
    print(tbl[:5])

# if __name__ == "__main__":
#     # 指定FITS文件
#     fits_file = "1.fits"
    
#     # 显示FITS文件信息
#     display_fits_info(fits_file)
    
#     # 转换为CSV文件
#     csv_output = fits_to_csv(fits_file)
    
#     print(f"\n转换成功！CSV文件保存在: {csv_output}")

# ... existing code ...
from astropy.table import vstack, Table
import pandas as pd
import os
import glob

def merge_all_fits_to_csv(directory="allwise_download", output_csv="merged_allwise.csv"):
    """
    读取指定目录中的所有FITS文件并合并成一个大的CSV文件
    
    参数:
        directory: str, 包含FITS文件的目录路径
        output_csv: str, 输出的合并CSV文件路径
    
    返回:
        str: 输出的合并CSV文件路径
    """
    # 获取目录中所有的FITS文件
    fits_pattern = os.path.join(directory, "*.fits")
    fits_files = glob.glob(fits_pattern)
    
    if not fits_files:
        print(f"在目录 {directory} 中未找到FITS文件")
        return None
    
    print(f"找到 {len(fits_files)} 个FITS文件")
    print("文件列表:")
    for i, file in enumerate(fits_files[:10]):  # 只显示前10个文件
        print(f"  {i+1}. {file}")
    if len(fits_files) > 10:
        print(f"  ... 还有 {len(fits_files) - 10} 个文件")
    
    # 读取并合并所有FITS文件
    all_tables = []
    total_rows = 0
    
    for i, fits_file in enumerate(fits_files):
        print(f"\n正在处理文件 {i+1}/{len(fits_files)}: {fits_file}")
        
        try:
            # 读取FITS文件
            tbl = Table.read(fits_file, format='fits')
            all_tables.append(tbl)
            total_rows += len(tbl)
            print(f"  成功读取 {len(tbl)} 行数据")
            
        except Exception as e:
            print(f"  读取文件 {fits_file} 时出错: {e}")
            continue
    
    if not all_tables:
        print("没有成功读取任何FITS文件")
        return None
    
    print(f"\n开始合并 {len(all_tables)} 个表格...")
    
    # 合并所有表格
    if len(all_tables) == 1:
        merged_table = all_tables[0]
    else:
        merged_table = vstack(all_tables)
    
    print(f"合并完成！总数据行数: {len(merged_table)}")
    print(f"列名: {list(merged_table.colnames)}")
    
    # 转换为pandas DataFrame并保存为CSV
    df = merged_table.to_pandas()
    
    print(f"正在写入合并的CSV文件: {output_csv}")
    df.to_csv(output_csv, index=False)
    
    print(f"合并完成！总数据行数: {len(df)}, 列数: {len(df.columns)}")
    print(f"合并的CSV文件已保存到: {output_csv}")
    
    return output_csv

def get_fits_file_count(directory="allwise_download"):
    """
    获取指定目录中FITS文件的数量
    """
    fits_pattern = os.path.join(directory, "*.fits")
    fits_files = glob.glob(fits_pattern)
    return len(fits_files)

# ... existing code ...
if __name__ == "__main__":
    # # 检查allwise_download目录中的FITS文件数量
    # fits_count = get_fits_file_count("allwise_download")
    # print(f"allwise_download目录中有 {fits_count} 个FITS文件")
    
    # if fits_count > 0:
    #     # 合并所有FITS文件到一个CSV
    #     merged_csv = merge_all_fits_to_csv("allwise_download", "merged_allwise.csv")
        
    #     if merged_csv:
    #         print(f"\n✅ 成功合并所有FITS文件到: {merged_csv}")
    #     else:
    #         print("\n❌ 合并失败")
    # else:
    #     print("没有找到FITS文件，跳过合并操作")
    
    # print("\n" + "="*50)
    
    # 原有的单个文件转换功能（保留）
    fits_file = "../testdata/1.fits"
    if os.path.exists(fits_file):
        # 显示FITS文件信息
        display_fits_info(fits_file)
        
        # 转换为CSV文件
        csv_output = fits_to_csv(fits_file)
        print(f"\n单个文件转换成功！CSV文件保存在: {csv_output}")
    else:
        print(f"文件 {fits_file} 不存在，跳过单个文件转换")

