import pandas as pd

# 读取 CSV 文件
input_path = r'D:\star_2024-1017\star_catalog\gaia_g14\gaia_g14.csv'
output_path = r'D:\star_2024-1017\star_catalog\gaia_g14\gaia_g14_filled.csv'

df = pd.read_csv(input_path)

# 填充缺失值为 0
df = df.fillna(0)

# 保存为新文件
df.to_csv(output_path, index=False)

print("缺失值已全部填充为 0，并保存为新文件：")
print(output_path)
