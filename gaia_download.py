import requests
import time
import pandas as pd
from pathlib import Path
from requests.adapters import HTTPAdapter, Retry

# TAP_URL = "https://mast.stsci.edu/vo-tap/api/v0.1/gaiadr3"
TAP_URL="http://dc.g-vo.org/tap"
OUTPUT_DIR = Path("D:/a_star_catalog/gaia_dr3_chunks")
OUTPUT_DIR.mkdir(exist_ok=True)

import requests
import time
from requests.adapters import HTTPAdapter, Retry

# TAP_URL = "https://mast.stsci.edu/vo-tap/api/v0.1/gaiadr3"
HEADERS = {
    # 不必显式设置 Content-Type；requests 会为 form-data 正确设置
    # "Content-Type": "application/x-www-form-urlencoded"
}

# 建立带重试策略的 Session
session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504))
session.mount("https://", HTTPAdapter(max_retries=retries))
session.mount("http://", HTTPAdapter(max_retries=retries))

def submit_job(adql, timeout=60):
    """
    提交异步 TAP 作业，返回 job_url（字符串）。
    在无法取得 Location 时，会抛出含有详细响应内容的 RuntimeError。
    """
    data = {
        "query": adql,
        "lang": "ADQL"
    }
    # 不自动跟随重定向，方便取 Location 头
    r = session.post(f"{TAP_URL}/async", data=data, headers=HEADERS,
                     timeout=timeout, allow_redirects=False)
    # 诊断输出（便于 debug）
    status = r.status_code
    # 常见成功码：201 Created 或 303 See Other（有 Location）
    if status in (201, 303, 202):
        loc = r.headers.get("Location") or r.headers.get("location") or r.headers.get("Content-Location")
        if loc:
            # 有可能返回相对路径，确保是完整 URL
            if loc.startswith("/"):
                from urllib.parse import urljoin
                loc = urljoin(TAP_URL, loc)
            return loc
        # 有些实现可能把 job id 放在 body（XML/HTML），尝试解析或返回 body 供人工检查
        raise RuntimeError(
            "Server returned success status but no Location header.\n"
            f"Status: {status}\nHeaders: {r.headers}\nBody preview: {r.text[:2000]}"
        )
    else:
        # 非成功响应：打印足够的信息以便定位问题
        raise RuntimeError(
            "Job submission failed.\n"
            f"Status: {status}\nHeaders: {r.headers}\nBody preview: {r.text[:2000]}"
        )

def debug_submit_once(adql):
    """
    调用 submit_job 时若抛错，捕获并打印更详细信息
    """
    try:
        url = submit_job(adql)
        print("Job URL:", url)
        return url
    except Exception as e:
        # 显示错误并建议下一步
        print("提交失败：", str(e))
        print("请检查：\n"
              " 1) ADQL 是否语法正确（特别是表名与条件）。\n"
              " 2) 是否使用了 /async 正确的 endpoint（末尾斜杠问题）。\n"
              " 3) 如果返回 429，请适当增加重试时间或减小提交频率。\n"
              "如需，我可以基于你粘贴的错误响应内容给出更具体修改。")
        raise


def wait_for_job(job_url, poll=10):
    while True:
        r = requests.get(f"{job_url}/phase")
        r.raise_for_status()
        phase = r.text.strip()
        if phase == "COMPLETED":
            return
        if phase in ("ERROR", "ABORTED"):
            raise RuntimeError(f"Job failed: {phase}")
        time.sleep(poll)

def download_result(job_url, out_file):
    r = requests.get(f"{job_url}/results/result", stream=True)
    r.raise_for_status()
    with open(out_file, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)

def get_last_source_id():
    files = sorted(OUTPUT_DIR.glob("chunk_*.csv"))
    if not files:
        return 0
    df = pd.read_csv(files[-1], usecols=["source_id"])
    return int(df["source_id"].max())

LIMIT = 100_000
last_source_id = get_last_source_id()
chunk_idx = len(list(OUTPUT_DIR.glob("chunk_*.csv")))

while True:
    adql = f"""
    SELECT TOP {LIMIT}
        source_id, ra, dec, parallax,
        pmra, pmdec, radial_velocity,
        phot_g_mean_mag
    FROM gaia.dr3lite
    WHERE phot_g_mean_mag < 14
      AND source_id > {last_source_id}
    ORDER BY source_id ASC
    """

    print(f"Submitting chunk {chunk_idx}, source_id > {last_source_id}")
    debug_submit_once(adql)
    job_url = submit_job(adql)
    wait_for_job(job_url)

    out_file = OUTPUT_DIR / f"chunk_{chunk_idx:05d}.csv"
    download_result(job_url, out_file)

    df = pd.read_csv(out_file)
    if df.empty:
        print("All data downloaded.")
        break

    last_source_id = int(df["source_id"].max())
    chunk_idx += 1

# 合并示例
df_all = pd.concat(
    (pd.read_csv(f) for f in OUTPUT_DIR.glob("chunk_*.csv")),
    ignore_index=True
)
