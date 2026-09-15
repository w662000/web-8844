# -*- coding: utf-8 -*-
"""
fetch_bars.py — 云端数据采集：全市场主板最近60个交易日日线（腾讯原始不复权K线）
用法: python3 fetch_bars.py [codes.json路径] [输出路径]
输出 bars.json: {"generated":..., "date":最新交易日, "bars": {code: {"k":[[date,open,close,high,low,vol],...], "name":...}}}
仅拉主板(60/00开头)——筛选规则只认主板，请求量省 40%。
"""
import io
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

PROXIES = {"http": os.environ.get("HTTP_PROXY", ""), "https": os.environ.get("HTTPS_PROXY", "")}
# GitHub Actions 上无代理 -> ProxyHandler({}) 直连；本机走环境代理
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler(PROXIES))
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


def http_get(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    return OPENER.open(req, timeout=timeout).read()


def fetch_names(codes):
    """qt.gtimg.cn 批量行情 -> {code: name}（60码/批）"""
    names = {}
    for i in range(0, len(codes), 60):
        chunk = codes[i:i + 60]
        syms = ",".join(("sh" if c[0] == "6" else "sz") + c for c in chunk)
        try:
            raw = http_get("https://qt.gtimg.cn/q=" + syms).decode("gbk", "replace")
            for m in __import__("re").finditer(r'v_(?:sh|sz)(\d{6})="([^"]*)"', raw):
                f = m.group(2).split("~")
                if len(f) > 2:
                    names[m.group(1)] = f[1]
        except Exception:
            pass
        time.sleep(0.15)
    return names


def fetch_one(sym):
    """腾讯日K(不复权,最近60根) -> [[date,open,close,high,low,vol],...]"""
    url = ("https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=" +
           urllib.parse.quote(sym) + ",day,,,60,")
    last_err = None
    for attempt in range(2):
        try:
            j = json.loads(http_get(url).decode("utf-8", "replace"))
            data = j.get("data", {}).get(sym, {})
            day = data.get("day") or []
            out = []
            for r in day:
                # [date, open, close, high, low, volume, ...] -> 规范六元组
                if len(r) >= 6:
                    out.append([r[0], float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])])
            return out
        except Exception as e:
            last_err = str(e)[:80]
            time.sleep(1.0)
    raise RuntimeError(f"{sym}: {last_err}")


def main():
    codes_path = sys.argv[1] if len(sys.argv) > 1 else str(Path(__file__).parent / "codes.json")
    out_path = sys.argv[2] if len(sys.argv) > 2 else "bars.json"
    codes = [c for c in json.loads(Path(codes_path).read_text(encoding="utf-8"))
             if c.startswith(("60", "00"))]
    print(f"主板待拉取: {len(codes)} 只")

    print("拉取名称中...")
    names = fetch_names(codes)
    print(f"names: {len(names)}")

    bars, errs = {}, []
    done = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=20) as ex:
        futs = {ex.submit(fetch_one, ("sh" if c[0] == "6" else "sz") + c): c for c in codes}
        for fut in as_completed(futs):
            c = futs[fut]
            done += 1
            try:
                k = fut.result()
                if len(k) >= 30:
                    bars[c] = {"k": k, "name": names.get(c, "")}
            except Exception as e:
                errs.append(f"{c}: {e}")
            if done % 400 == 0:
                print(f"  进度 {done}/{len(codes)} 失败{len(errs)} 用时{time.time()-t0:.0f}s")
                sys.stdout.flush()

    last_date = max((k[-1][0] for k in bars.values()), default="")
    out = {"generated": datetime.now(timezone(timedelta(hours=8))).isoformat(),
           "date": last_date, "bars": bars}
    Path(out_path).write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    print(f"BARS_OK date={last_date} stocks={len(bars)} 失败={len(errs)} 用时={time.time()-t0:.0f}s -> {out_path}")
    for e in errs[:10]:
        print("  ERR", e)
    if len(bars) < len(codes) * 0.9:
        print("FETCH_QUALITY_WARN: 成功率<90%")
        sys.exit(2)


if __name__ == "__main__":
    main()
