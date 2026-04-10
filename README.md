# Gaia DR3 恒星目录数据处理工具

从 Gaia DR3 数据库下载恒星数据并进行处理、抽稀、历元转换的 Python 工具集。

## 项目简介

本项目提供了一系列脚本，用于从 Gaia Data Release 3 (DR3) 下载恒星数据，并将其处理成适合天文应用的格式。主要功能包括：

- 从 Gaia 官方 TAP 服务下载数据
- 使用 HEALPix 进行全天均匀抽稀
- 坐标历元转换（J2016 → J2000）
- FITS/CSV 格式互转
- 分块数据合并

## 文件说明

### 数据下载

| 文件 | 功能 |
|------|------|
| `gaia_download.py` | 通过 TAP 服务异步下载 Gaia DR3 数据（G<14），分块保存为 CSV |
| `getGaiadr3g14.py` | 使用 pyvo 下载 G<14 的恒星数据，支持断点续传，保存为 FITS |
| `getAllWise.py` | 下载 WISE 红外天文卫星数据 |
| `getAllWise-gemini.py` | Gemini 版本的 WISE 数据下载 |
| `getHipparcos.py` | 下载 Hipparcos 恒星目录数据 |

### 数据处理

| 文件 | 功能 |
|------|------|
| `gaia_catalog_reduce.py` | 使用 HEALPix 将数据均匀分布到全天像素，优先保留亮星 |
| `merge_gaia_chunks.py` | 将多个 FITS/CSV 分块文件合并成一个文件 |
| `readfits.py` | 将 FITS 文件转换为 CSV，支持批量合并 |
| `fill0.py` | 将 CSV 中的 NaN 缺失值填充为 0 |

### 历元转换

| 文件 | 功能 |
|------|------|
| `gaia_j2016toJ2000.py` | 将 Gaia J2016.0 历元坐标自行运动到 J2000.0 历元 |
| `gaia_toJ2000.py` | 简化版历元转换 |
| `gaia_j2016.py` | J2016 历元相关处理 |
| `gaia_uniform_downsample.py` | 均匀降采样 |

## 数据处理流程

```mermaid
graph LR
    A[下载 Gaia DR3] --> B[合并分块文件]
    B --> C[HEALPix 均匀抽稀]
    C --> D[填充缺失值]
    D --> E[历元转换到 J2000]
```

## 依赖环境

```bash
pip install requests pandas numpy astropy astropy-healpix pyvo astroquery
```

## 使用示例

### 1. 下载 Gaia DR3 数据

```bash
python gaia_download.py
```

### 2. 合并分块文件

```python
from merge_gaia_chunks import merge_chunks

merge_chunks(
    input_dir="D:/a_star_catalog/gaia_dr3_chunks",
    output_file="D:/a_star_catalog/gaia_dr3_merged.csv",
    method='csv',
    deduplicate=True
)
```

### 3. HEALPix 均匀抽稀

```bash
python gaia_catalog_reduce.py
```

### 4. 历元转换

```bash
python gaia_j2016toJ2000.py
```

## 配置参数

### HEALPix 抽稀配置 (gaia_catalog_reduce.py)

- `NSIDE = 128` - 全天划分为 196,608 个像素
- `STARS_PER_PIXEL = 1` - 每个像素保留 1 颗星

### 优先级规则

- 优先级 0: G ≤ 7.0 等且有视差数据
- 优先级 1: G < 10.0 等且有视差数据
- 优先级 2: G < 14.0 等且有视差数据

## 输出数据

处理后的星表包含以下字段：

| 字段 | 说明 |
|------|------|
| `source_id` | Gaia 源 ID |
| `ra` | 赤经 (度) |
| `dec` | 赤纬 (度) |
| `parallax` | 视差 (mas) |
| `pmra` | 赤经自行 (mas/yr) |
| `pmdec` | 赤纬自行 (mas/yr) |
| `radial_velocity` | 径向速度 (km/s) |
| `phot_g_mean_mag` | G 波段平均星等 |

## 许可证

MIT License

## 作者

jsy96
