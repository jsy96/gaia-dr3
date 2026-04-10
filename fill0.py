import pandas as pd

input_path = r'D:\a_star_catalog\gaia_dr3_final_uniform2.csv'
output_path = r'D:\a_star_catalog\gaia_dr3_final_uniform3.csv'

# 读取
df = pd.read_csv(input_path)

# 把所有空白值（NaN）填为 0
df = df.fillna(0)

# 保存（覆盖原文件）
df.to_csv(output_path, index=False)
print("缺失值已全部填充为 0，并保存为新文件：", output_path)

