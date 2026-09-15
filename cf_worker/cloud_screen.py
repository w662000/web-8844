# -*- coding: utf-8 -*-
"""
cloud_screen.py — 云端池子筛选（与 scan_realtime.hist_pool_all 逐条对齐的独立移植版）
输入: bars.json（fetch_bars.py 产出, 腾讯原始不复权日K, [[date,open,close,high,low,vol],...]）
输出: pool.json（与现有 Worker/看板 schema 完全一致）并可 POST 到 Worker /pool
规则: 仅主板(剔30/688/689/8/4/9) + 恰好2连板 + gap≤8(交易日历) + 剔双一字板 + EMA7
用法: python3 cloud_screen.py [bars.json] [pool.json输出]
环境变量: WORKER_URL / AUTH_TOKEN 存在时自动 POST 到 Worker /pool
"""
import io
import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

POOL_GAP = 8      # 底部候选池: 8个交易日内有2连板的全部主板(雷达)
BK = 0.98         # 回调破位阈值(统计 back_days 用)
EMA_N = 7         # EMA7


def cn_now():
    return datetime.now(timezone(timedelta(hours=8)))


def ema7_of(closes):
    k = 2.0 / (EMA_N + 1)
    if len(closes) < EMA_N:
        return closes[-1]
    ema = sum(closes[:EMA_N]) / EMA_N
    for c in closes[EMA_N:]:
        ema = c * k + ema * (1 - k)
    return ema


def screen_one(code, bars):
    """bars: [[date,open,close,high,low,vol],...] — 逐条对齐 scan_realtime.hist_pool_all"""
    if code.startswith(("30", "688", "689", "8", "4", "9")):
        return None
    # home 的 hist 行序是 [.., hi=r2, lo=r3, cl=r4]；腾讯行序是 [d, o, c, h, l, v]
    closes = [r[2] for r in bars]
    vols = [r[5] for r in bars]
    n = len(bars)
    if n < 30 or closes[-1] <= 0:
        return None

    is_zt = [False] * n
    for i in range(1, n):
        r = bars[i]
        hi, lo, cl = r[3], r[4], r[2]
        if lo <= 0 or hi <= 0 or cl < hi * 0.9995:
            continue
        chg = (cl - closes[i - 1]) / closes[i - 1]
        is_zt[i] = chg >= 0.095  # 主板 10cm

    i = None
    for k in range(n - 1, 0, -1):
        if is_zt[k] and (k == n - 1 or not is_zt[k + 1]):
            i = k
            break
    if i is None:
        return None
    h = 0
    k = i
    while k >= 0 and is_zt[k]:
        h += 1
        k -= 1
    if h != 2:
        return None
    gap = n - 1 - i                   # 收盘后跑: 今日K线已在 bars 内, 与家中收盘后口径一致
    if gap < 0 or gap > POOL_GAP:
        return None

    def _is_one(r):
        op, hi, lo, cl = r[1], r[3], r[4], r[2]
        if cl <= 0:
            return False
        return (cl >= hi * 0.999) and (abs(op - cl) / cl <= 0.003) and (lo >= cl * 0.997)

    if _is_one(bars[i]) and _is_one(bars[i - 1]):
        return None

    zt_close = closes[i]
    min_c = min(closes[i + 1:n]) if i + 1 < n else closes[i]
    back_days = sum(1 for j in range(i + 1, n) if closes[j] < zt_close * BK)
    ema7 = ema7_of(closes)
    return {"sym": code, "name": "", "ey": round(ema7, 3), "g": gap, "back_days": back_days,
            "zt_date": bars[i][0], "deep": round((zt_close - min_c) / zt_close * 100, 2)}


def main():
    bars_path = sys.argv[1] if len(sys.argv) > 1 else "bars.json"
    out_path = sys.argv[2] if len(sys.argv) > 2 else "pool.json"
    j = json.loads(Path(bars_path).read_text(encoding="utf-8"))
    bars = j.get("bars", {})
    entries = []
    for code in sorted(bars.keys()):
        try:
            r = screen_one(code, bars[code]["k"])
        except Exception:
            r = None
        if r:
            r["name"] = bars[code].get("name", "")
            entries.append(r)
    entries.sort(key=lambda x: (-x["g"], x["sym"]))
    out = {"date": j.get("date", ""), "time": cn_now().strftime("%H:%M:%S"),
           "ts": int(time.time() * 1000), "source": "cloud",
           "entries": entries}
    Path(out_path).write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    print(f"POOL_OK date={out['date']} entries={len(entries)} -> {out_path}")

    wurl = os.environ.get("WORKER_URL", "").rstrip("/")
    atok = os.environ.get("AUTH_TOKEN", "")
    if wurl and atok:
        try:
            req = urllib.request.Request(wurl + "/pool", data=json.dumps(out, ensure_ascii=False).encode(),
                method="POST", headers={"x-auth-token": atok, "Content-Type": "application/json",
                                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
            op = urllib.request.build_opener(urllib.request.ProxyHandler(
                {"http": os.environ.get("HTTP_PROXY", ""), "https": os.environ.get("HTTPS_PROXY", "")}))
            print("WORKER_SYNC", op.open(req, timeout=20).status)
        except Exception as e:
            print("WORKER_SYNC_FAIL", str(e)[:120])


if __name__ == "__main__":
    main()
