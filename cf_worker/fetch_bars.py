# -*- coding: utf-8 -*-
"""
fetch_bars.py — 云端数据采集 v2：双源分流（2026-09-15 实测定稿）
==============================================================
GitHub 美国节点实测（每源3次）:
  qt.gtimg 实时 0.71s | ifzq 日K 0.98s | push2his 日K 1.41s | sinajs 实时 0.59s
  push2 实时 302 不可用 | 新浪日K 返回异常不可用
策略(2026-09-15 用户拍板 v3): **东财 push2his 主拉全量 -> 腾讯 ifzq 备用补拉 -> 东财低并发重试**;
     源内并发 10（单IP高并发触发限速的教训）；GitHub 实测: ifzq 0.98s / push2his 1.41s / push2实时302不可用。
输出: bars.json {"generated","date","bars":{code:{"k":[[d,o,c,h,l,v]...],"name"}}}
"""
import io
import json
import os
import sys
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)

_px = {k: v for k, v in {"http": os.environ.get("HTTP_PROXY", ""), "https": os.environ.get("HTTPS_PROXY", "")}.items() if v}
# 注意: Actions 上无代理环境变量, 必须直连; 空字符串代理会让所有请求瞬间失败
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler(_px))
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
PER_SOURCE_THREADS = 10
NBARS = 60


def http_get(url, timeout=15, referer=None):
    h = {"User-Agent": UA}
    if referer:
        h["Referer"] = referer
    req = urllib.request.Request(url, headers=h)
    return OPENER.open(req, timeout=timeout).read()


def fetch_tencent(sym):
    url = ("https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=" +
           urllib.parse.quote(sym) + ",day,,," + str(NBARS) + ",")
    j = json.loads(http_get(url).decode("utf-8", "replace"))
    day = j.get("data", {}).get(sym, {}).get("day") or []
    return [[r[0], float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])] for r in day if len(r) >= 6]


def fetch_east(sym):
    secid = ("1." if sym.startswith("sh") else "0.") + sym[2:]
    url = ("https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=" + secid +
           "&klt=101&fqt=0&lmt=" + str(NBARS) + "&end=20500101&fields1=f1&fields2=f51,f52,f53,f54,f55,f56")
    j = json.loads(http_get(url, referer="https://quote.eastmoney.com/").decode("utf-8", "replace"))
    kl = (j.get("data") or {}).get("klines") or []
    return [[r.split(",")[0], float(r.split(",")[1]), float(r.split(",")[2]),
             float(r.split(",")[3]), float(r.split(",")[4]), float(r.split(",")[5])] for r in kl]


def fetch_names(codes):
    """qt.gtimg 批量名称（60码/批）"""
    names = {}
    for i in range(0, len(codes), 60):
        chunk = codes[i:i + 60]
        syms = ",".join(("sh" if c[0] == "6" else "sz") + c for c in chunk)
        try:
            raw = http_get("https://qt.gtimg.cn/q=" + syms).decode("gbk", "replace")
            import re
            for m in re.finditer(r'v_(?:sh|sz)(\d{6})="([^"]*)"', raw):
                f = m.group(2).split("~")
                if len(f) > 2:
                    names[m.group(1)] = f[1]
        except Exception:
            pass
        time.sleep(0.15)
    return names


def main():
    codes_path = sys.argv[1] if len(sys.argv) > 1 else str(Path(__file__).parent / "codes.json")
    out_path = sys.argv[2] if len(sys.argv) > 2 else "bars.json"
    codes = [c for c in json.loads(Path(codes_path).read_text(encoding="utf-8"))
             if c.startswith(("60", "00"))]
    print(f"主板待拉取: {len(codes)} 只（东财主拉 / 腾讯备用）")

    t0 = time.time()
    print("拉取名称中...")
    names = fetch_names(codes)

    results = {}
    lock = threading.Lock()
    stats = {"tencent": [0, 0], "east": [0, 0]}  # ok, fail

    def run_source(source, scodes, workers=PER_SOURCE_THREADS):
        fetcher = fetch_tencent if source == "tencent" else fetch_east
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(fetcher, ("sh" if c[0] == "6" else "sz") + c): c for c in scodes}
            for fut in as_completed(futs):
                c = futs[fut]
                try:
                    k = fut.result()
                    with lock:
                        results[c] = k
                        stats[source][0] += 1
                except Exception:
                    with lock:
                        stats[source][1] += 1

    # 第一遍: 东财主拉全量
    run_source("east", codes)
    # 第二遍: 失败的码用腾讯补拉（备用源）
    missing = [c for c in codes if c not in results]
    if missing:
        print(f"腾讯备用补拉 {len(missing)} 只")
        run_source("tencent", missing)
    # 第三遍: 仍缺的东财低并发重试
    missing2 = [c for c in codes if c not in results]
    if missing2:
        print(f"东财重试 {len(missing2)} 只")
        run_source("east", missing2)

    bars = {c: {"k": k, "name": names.get(c, "")} for c, k in results.items() if len(k) >= 30}
    last_date = max((v["k"][-1][0] for v in bars.values()), default="")
    out = {"generated": datetime.now(timezone(timedelta(hours=8))).isoformat(),
           "date": last_date, "bars": bars}
    Path(out_path).write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    print(f"BARS_OK date={last_date} stocks={len(bars)}/{len(codes)} 用时={time.time()-t0:.0f}s -> {out_path}")
    if len(bars) < len(codes) * 0.9:
        print("FETCH_QUALITY_WARN: 成功率<90%")
        sys.exit(2)


if __name__ == "__main__":
    main()
