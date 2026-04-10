import pandas as pd

# 读取 CSV 文件
df = pd.read_csv(r'D:\star_2024-1017\star_catalog\gaia_g14\gaia_g14_filled.csv')

# 判断是否存在任意缺失值
has_missing = df.isnull().values.any()

if has_missing:
    print("CSV 文件中存在数据缺失。")
else:
    print("CSV 文件中不存在数据缺失。")

# （可选）输出每一列的缺失值数量
missing_per_column = df.isnull().sum()
print("\n各列缺失值数量：")
print(missing_per_column)
