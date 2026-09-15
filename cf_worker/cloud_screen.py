# -*- coding: utf-8 -*-
"""
cloud_screen.py — 云端池子引擎 v4（增量更新制，彻底摆脱全量拉取限速）
======================================================================
模式:
  python3 cloud_screen.py update
      1. GET  {WORKER_URL}/bars.json        云端 60 日历史 (KV, ~6MB)
      2. 批量腾讯行情 (60码/请求, 全市场仅 ~54 请求) 取当日 OHLCV
      3. 追加/替换当日K线 (停牌 volume=0 跳过; 超60根滚动裁剪)
      4. 筛选池子 (hist_pool_all 逻辑移植, 与 scan_realtime 逐条对齐)
      5. POST {WORKER_URL}/bars.json + /pool.json 推回云端
  python3 cloud_screen.py screen <bars.json> <pool.json>
      离线: 从完整 bars 文件筛选（测试/对账用）
环境: WORKER_URL / AUTH_TOKEN（缺省时回退读 cf_worker/deploy_info.json）
"""
import io
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

POOL_GAP = 8
BK = 0.98
EMA_N = 7
PROJ = Path(__file__).parent.parent
CN_TZ = timezone(timedelta(hours=8))


UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


def cn_now():
    return datetime.now(CN_TZ)


def cfg():
    wurl = os.environ.get("WORKER_URL", "").rstrip("/")
    atok = os.environ.get("AUTH_TOKEN", "")
    if not wurl or not atok:
        di = Path(__file__).parent / "deploy_info.json"
        if di.exists():
            dij = json.loads(di.read_text(encoding="utf-8"))
            wurl = dij.get("worker_url", "").rstrip("/")
            atok = dij.get("auth_token", "")
    if not wurl or not atok:
        raise RuntimeError("缺少 WORKER_URL / AUTH_TOKEN")
    return wurl, atok


def make_opener(use_proxy):
    px = {k: v for k, v in {"http": os.environ.get("HTTP_PROXY", ""), "https": os.environ.get("HTTPS_PROXY", "")}.items() if v} if use_proxy else {}
    return urllib.request.build_opener(urllib.request.ProxyHandler(px))


CN_OPENER = make_opener(False)  # 腾讯/东财等国内源: 直连


def worker_opener():
    # workers.dev 从国内直连被 SNI 阻断: 本机走代理, Actions 无代理变量时自动直连
    px = {k: v for k, v in {"http": os.environ.get("HTTP_PROXY", ""), "https": os.environ.get("HTTPS_PROXY", "")}.items() if v}
    return urllib.request.build_opener(urllib.request.ProxyHandler(px))


def http_get(url, timeout=15, referer=None, opener=None):
    h = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    if referer:
        h["Referer"] = referer
    req = urllib.request.Request(url, headers=h)
    op = opener or CN_OPENER
    return op.open(req, timeout=timeout).read()


def http_post_json(url, obj, atok):
    req = urllib.request.Request(url, data=json.dumps(obj, ensure_ascii=False).encode("utf-8"),
        method="POST", headers={"x-auth-token": atok, "Content-Type": "application/json",
                                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
    return worker_opener().open(req, timeout=60).read().decode()


def ema7_of(closes):
    k = 2.0 / (EMA_N + 1)
    if len(closes) < EMA_N:
        return closes[-1]
    ema = sum(closes[:EMA_N]) / EMA_N
    for c in closes[EMA_N:]:
        ema = c * k + ema * (1 - k)
    return ema


def screen_one(code, bars):
    """bars: [[date,open,close,high,low,vol],...] — 与 scan_realtime.hist_pool_all 对齐"""
    if code.startswith(("30", "688", "689", "8", "4", "9")):
        return None
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
        is_zt[i] = chg >= 0.095
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
    gap = n - 1 - i
    if gap < 0 or gap > POOL_GAP:
        return None

    def _is_one(r):
        op_, hi, lo, cl = r[1], r[3], r[4], r[2]
        if cl <= 0:
            return False
        return (cl >= hi * 0.999) and (abs(op_ - cl) / cl <= 0.003) and (lo >= cl * 0.997)

    if _is_one(bars[i]) and _is_one(bars[i - 1]):
        return None
    zt_close = closes[i]
    min_c = min(closes[i + 1:n]) if i + 1 < n else closes[i]
    back_days = sum(1 for j in range(i + 1, n) if closes[j] < zt_close * BK)
    ema7 = ema7_of(closes)
    return {"sym": code, "name": "", "ey": round(ema7, 3), "g": gap, "back_days": back_days,
            "zt_date": bars[i][0], "deep": round((zt_close - min_c) / zt_close * 100, 2)}



def worker_get(url):
    """workers.dev 会按 TLS 指纹拦 Python-urllib, 必须用 curl 子进程（自动适配两环境代理）"""
    import subprocess
    r = subprocess.run(["curl", "-sS", "-m", "120", "-A", UA, url],
                       capture_output=True, timeout=150)
    return r.stdout.decode("utf-8", "replace")


def worker_post(url, obj, atok):
    import subprocess
    tmp = Path(os.environ.get("TEMP", "/tmp")) / "cf_post_body.json"
    tmp.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")
    r = subprocess.run(["curl", "-sS", "-m", "180", "-X", "POST", "-A", UA,
                        "-H", "x-auth-token: " + atok, "-H", "Content-Type: application/json",
                        "--data-binary", "@" + str(tmp), url], capture_output=True, timeout=220)
    return r.stdout.decode("utf-8", "replace")

def update():
    wurl, atok = cfg()
    WOP = worker_opener()
    # 0. 新上市股票: codes.json 里有而 bars 里没有的 -> 从 ifzq 拉 60日历史（每月几只，开销极小）
    codes_file = Path(__file__).parent / "codes.json"
    all_codes = list(json.loads(codes_file.read_text(encoding="utf-8")).keys()) if codes_file.exists() else []
    # 1. 云端 60 日历史（经代理: workers.dev 国内直连被阻断）
    raw = worker_get(wurl + "/bars.json")
    bj = json.loads(raw)
    bars = bj.get("bars", {})
    print(f"云端 bars: {len(bars)} 只, 截至 {bj.get('date')}")
    fresh = [c for c in all_codes if c.startswith(("60", "00")) and c not in bars]
    if fresh:
        print(f"新上市/缺失 {len(fresh)} 只, 从 ifzq 拉 60 日历史...")
        for c in fresh[:50]:
            sym = ("sh" if c[0] == "6" else "sz") + c
            try:
                j = json.loads(http_get("https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=" +
                    urllib.parse.quote(sym) + ",day,,,60,").decode("utf-8", "replace"))
                day = j.get("data", {}).get(sym, {}).get("day") or []
                k = [[r[0], float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])] for r in day if len(r) >= 6]
                if len(k) >= 30:
                    bars[c] = {"k": k, "name": ""}
            except Exception:
                pass
            time.sleep(0.2)
        print(f"  补齐后 bars: {len(bars)} 只")
    codes = list(bars.keys())

    # 2. 批量腾讯行情取当日 OHLCV（60码/批）
    t0 = time.time()
    today_quotes = {}
    for i in range(0, len(codes), 60):
        chunk = codes[i:i + 60]
        syms = ",".join(("sh" if c[0] == "6" else "sz") + c for c in chunk)
        try:
            rawq = http_get("https://qt.gtimg.cn/q=" + syms).decode("gbk", "replace")
            for m in re.finditer(r'v_(?:sh|sz)(\d{6})="([^"]*)"', rawq):
                f = m.group(2).split("~")
                if len(f) > 36:
                    today_quotes[m.group(1)] = {
                        "name": f[1], "date": f[30][:8].replace("/", "-"),
                        "open": float(f[5] or 0), "close": float(f[3] or 0),
                        "high": float(f[33] or 0), "low": float(f[34] or 0),
                        "vol": float(f[36] or 0),
                    }
        except Exception as e:
            print("  batch err:", str(e)[:80])
        time.sleep(0.1)
    print(f"当日行情: {len(today_quotes)}/{len(codes)} 用时 {time.time()-t0:.0f}s")

    # 3. 追加/替换当日K线
    today_cn = cn_now().strftime("%Y-%m-%d")
    added, replaced, skipped_susp, skipped_old = 0, 0, 0, 0
    for c in codes:
        tq = today_quotes.get(c)
        if not tq:
            continue
        bd8 = tq["date"] or today_cn.strftime("%Y%m%d")
        bdate = bd8[:4] + "-" + bd8[4:6] + "-" + bd8[6:8]
        if tq["vol"] == 0 and tq["close"] <= 0:
            skipped_susp += 1
            continue
        k = bars[c]["k"]
        if k and k[-1][0] == bdate:
            k[-1] = [bdate, tq["open"], tq["close"], tq["high"], tq["low"], tq["vol"]]
            replaced += 1
        elif k and k[-1][0] > bdate:
            skipped_old += 1  # 行情日期落后于已有K线(异常), 跳过
        else:
            k.append([bdate, tq["open"], tq["close"], tq["high"], tq["low"], tq["vol"]])
            if len(k) > 60:
                bars[c]["k"] = k[-60:]
            added += 1
        nm = tq.get("name", "")
        if nm:
            bars[c]["name"] = nm
    print(f"K线追加 {added} / 替换 {replaced} / 停牌跳过 {skipped_susp} / 旧日期跳过 {skipped_old}")

    # 4. 筛选
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
    last_date = max((v["k"][-1][0].replace("-", "") for v in bars.values()), default="")
    pool = {"date": last_date, "time": cn_now().strftime("%H:%M:%S"),
            "ts": int(time.time() * 1000), "source": "cloud",
            "entries": entries}
    bars_out = {"generated": cn_now().isoformat(), "date": last_date, "bars": bars}
    print(f"POOL_OK date={pool['date']} entries={len(entries)}")

    # 5. 推回云端
    r1 = worker_post(wurl + "/bars.json", bars_out, atok)
    r2 = worker_post(wurl + "/pool.json", pool, atok)
    print("WORKER_SYNC bars:", r1[:80], "| pool:", r2[:80])


def screen_offline(bars_path, out_path):
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
           "ts": int(time.time() * 1000), "source": "offline",
           "entries": entries}
    Path(out_path).write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    print(f"POOL_OK entries={len(entries)} -> {out_path}")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "update"
    if mode == "update":
        update()
    elif mode == "screen":
        screen_offline(sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else "pool.json")
    else:
        print("unknown mode"); sys.exit(1)
