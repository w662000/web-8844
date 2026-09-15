/**
 * webmon — 涨停回调盘中监控（Cloudflare Workers 独立系统，不依赖家里主机）
 * =====================================================================
 * 职责：交易时段每 2 分钟拉腾讯行情 -> 与 KV 自己的状态对比 ->
 *       BUY档/池内突破变化 -> SMTP 邮件推送到 126 邮箱（无 12h 窗口）。
 * 与家里主机的关系：完全独立（家里走微信，本 Worker 走邮件，两路并存可对比）。
 * 池子快照来源：raw.githubusercontent.com/w662000/web-8844/main/pool.json
 *   （家里 publish_site.py 每 ~5 分钟刷新；主机关机时沿用最后快照——
 *     EMA7/池子构成是日级数据，会逐渐失真，属已知降级）
 * 绑定：KV namespace -> STATE
 * 变量：MAIL_USER, MAIL_PASS, MAIL_TO, AUTH_TOKEN（secrets）
 * 触发：cron 每2分钟（UTC 1-3 点与 5-7 点，周一至五）= 北京 09:00-11:59 / 13:00-15:59
 * 手动触发：GET /run?token=<AUTH_TOKEN>   （Worker URL，一次性执行 tick 并返回摘要）
 * 推送数据源上传：POST /pool  body=pool.json  header: x-auth-token=<AUTH_TOKEN>
 */

import { connect } from "cloudflare:sockets";
import DASH from "./dashboard.html";

const POOL_URL = "https://raw.githubusercontent.com/w662000/web-8844/main/pool.json";
const SMTP_HOST = "smtp.126.com";
const SMTP_PORT = 465;
const BUY_VR_MIN = 1.0; // 量比阈值（与 scan_realtime 一致）
const SEP = "━━━━━━━━━━━";
const SEP_L = "─────";

/** 北京时间挂钟（epoch+8h 后按 UTC 字段取值即为北京墙钟） */
function cnNow() { return new Date(Date.now() + 8 * 3600e3); }

function inSession() {
  const d = cnNow();
  const wd = d.getUTCDay();
  if (wd === 0 || wd === 6) return false;
  const m = d.getUTCHours() * 60 + d.getUTCMinutes();
  return (m >= 570 && m <= 690) || (m >= 780 && m <= 900); // 9:30-11:30 / 13:00-15:00
}

function arrow(pct) {
  const p = parseFloat(pct) || 0;
  return (p < 0 ? "↓" : "↑") + Math.abs(p).toFixed(2) + "%";
}

export default {
  async scheduled(ctrl, env, ctx) {
    const r = await tick(env);
    try { await env.STATE.put("last_tick", JSON.stringify({ at: Date.now(), summary: r.summary })); } catch {}
    console.log(new Date().toISOString(), r.summary);
  },
  async fetch(req, env, ctx) {
    const url = new URL(req.url);
    if (url.pathname === "/" || url.pathname === "/dashboard")
      return new Response(DASH, { headers: { "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-store" } });
    if (url.pathname === "/pool.json") {
      if (req.method === "POST") {
        if (req.headers.get("x-auth-token") !== env.AUTH_TOKEN)
          return new Response("forbidden", { status: 403 });
        const raw = await req.text();
        let j;
        try { j = JSON.parse(raw); } catch { return Response.json({ error: "BAD_JSON" }, { status: 400 }); }
        if (!j || !Array.isArray(j.entries))
          return Response.json({ error: "BAD_POOL: entries[] required" }, { status: 400 });
        await env.STATE.put("pool", JSON.stringify(j));
        return Response.json({ ok: true, date: j.date, entries: j.entries.length });
      }
      const poolRaw = await env.STATE.get("pool");
      if (!poolRaw) return Response.json({ error: "NO_POOL" }, { status: 503 });
      const body = JSON.parse(poolRaw);
      const lt = await env.STATE.get("last_tick");
      body.last_tick = lt ? JSON.parse(lt) : null;
      body.now = Date.now();
      return new Response(JSON.stringify(body), { headers: {
        "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store",
        "Access-Control-Allow-Origin": "*" } });
    }
    if (url.pathname === "/run") {
      if (url.searchParams.get("token") !== env.AUTH_TOKEN)
        return new Response("forbidden", { status: 403 });
      // 总时长熔断 25s：超时也返回，tick 在后台继续（邮件可能稍后发出）
      const force = url.searchParams.get("force") === "1";
      const r = await Promise.race([
        tick(env, force),
        new Promise(res => setTimeout(() => res({ summary: "TICK_TIMEOUT(25s, 后台已继续)" }), 25000)),
      ]);
      return Response.json(r);
    }
    if (url.pathname === "/bars.json") {
      if (req.method === "POST") {
        if (req.headers.get("x-auth-token") !== env.AUTH_TOKEN)
          return new Response("forbidden", { status: 403 });
        const raw = await req.text();
        JSON.parse(raw);  // 校验合法 JSON
        await env.STATE.put("bars", raw);
        return Response.json({ ok: true, bytes: raw.length });
      }
      const barsRaw = await env.STATE.get("bars");
      if (!barsRaw) return Response.json({ error: "NO_BARS" }, { status: 503 });
      return new Response(barsRaw, { headers: { "Content-Type": "application/json; charset=utf-8",
        "Cache-Control": "no-store", "Access-Control-Allow-Origin": "*" } });
    }
    if (url.pathname === "/pool" && req.method === "POST") {
      if (req.headers.get("x-auth-token") !== env.AUTH_TOKEN)
        return new Response("forbidden", { status: 403 });
      const j = await req.json();
      await env.STATE.put("pool", JSON.stringify(j));
      return Response.json({ ok: true, date: j.date, entries: (j.entries || []).length });
    }
    return new Response("webmon ok");
  },
};

async function tick(env, force) {
  if (!force && !inSession()) return { summary: "OUT_OF_SESSION" };

  const poolRaw = await env.STATE.get("pool");
  if (!poolRaw) return { summary: "NO_POOL" };
  let pool;
  try { pool = JSON.parse(poolRaw); } catch { return { summary: "POOL_CORRUPT" }; }

  const ema = {}, name = {}, bd = {};
  for (const e of pool.entries || []) {
    if (!e.sym) continue;
    name[e.sym] = e.name || e.sym;
    if (e.ey) ema[e.sym] = e.ey;
    bd[e.sym] = e.back_days;
  }
  const codes = Object.keys(ema);
  if (!codes.length) return { summary: "EMPTY_POOL" };

  // 腾讯行情（GBK 解码，禁用边缘缓存）
  const syms = codes.map(c => (c[0] === "6" ? "sh" : "sz") + c);
  let text;
  try {
    const res = await fetch("https://qt.gtimg.cn/q=" + syms.join(",") + "&r=" + Date.now(),
      { headers: { "User-Agent": "Mozilla/5.0" }, signal: AbortSignal.timeout(8000),
        cf: { cacheTtl: 0, cacheEverything: false } });
    const buf = new Uint8Array(await res.arrayBuffer());
    try { text = new TextDecoder("gbk").decode(buf); }
    catch { text = new TextDecoder("utf-8", { fatal: false }).decode(buf); }
  } catch (e) {
    return { summary: "QUOTE_ERR " + String(e).slice(0, 80) };
  }
  const q = {};
  for (const m of text.matchAll(/v_(?:sh|sz)(\d{6})="([^"]*)"/g)) {
    const f = m[2].split("~");
    if (f.length > 49)
      q[m[1]] = { price: parseFloat(f[3]), pct: parseFloat(f[32]) || 0, vr: parseFloat(f[49]) || 0 };
  }
  // 行情质量守卫：缺行情的票太多时本轮跳过（防止误报大规模回撤）
  const have = codes.filter(c => q[c] && isFinite(q[c].price)).length;
  if (have < codes.length * 0.8) return { summary: `QUOTES_PARTIAL ${have}/${codes.length}` };

  let prev = { broke: [], buy: [] };
  try {
    const p = JSON.parse((await env.STATE.get("mon")) || "null");
    if (p && Array.isArray(p.broke)) prev = p;
  } catch {}

  const curBroke = [], curBuy = [];
  for (const c of codes) {
    const qq = q[c];
    if (qq && qq.price > ema[c]) {
      curBroke.push(c);
      if (qq.vr >= BUY_VR_MIN) curBuy.push(c);
    }
  }
  const addBroke = curBroke.filter(c => !prev.broke.includes(c));
  const rmBroke = prev.broke.filter(c => !curBroke.includes(c));
  const addBuy = curBuy.filter(c => !prev.buy.includes(c));
  const rmBuy = prev.buy.filter(c => !curBuy.includes(c));

  const t0 = cnNow().toISOString().slice(11, 16);
  const msgs = [];

  if (addBuy.length || rmBuy.length) {
    const L = ["📈 突破买入提醒 " + t0 + " ☁️"];
    for (const c of addBuy) {
      const qq = q[c];
      L.push(SEP, "🟢 新增", `${name[c]} ${c}`,
        `现价 ${qq.price.toFixed(2)} ${arrow(qq.pct)}`,
        `量比 ${qq.vr.toFixed(2)}${bd[c] != null ? ` ｜ 离板 ${bd[c]} 天` : ""}`);
    }
    for (const c of rmBuy) {
      const qq = q[c];
      L.push(SEP, "↩️ 回撤", `${name[c]} ${c}`);
      if (qq && isFinite(qq.price)) L.push(`现价 ${qq.price.toFixed(2)} ${arrow(qq.pct)}`);
    }
    L.push(SEP, "BUY档: " + (curBuy.map(c => name[c]).join(" / ") || "空"));
    msgs.push(L.join("\n"));
  }
  if (addBroke.length || rmBroke.length) {
    const L = ["🚀 池内突破动态 " + t0 + " ☁️"];
    for (const c of addBroke) {
      const qq = q[c];
      L.push(`✅新突破: ${name[c]}${c} ${qq.price.toFixed(2)} ${arrow(qq.pct)}`);
    }
    for (const c of rmBroke) {
      const qq = q[c];
      L.push(`↩️回撤: ${name[c]}${c}` + (qq && isFinite(qq.price) ? ` ${qq.price.toFixed(2)} ${arrow(qq.pct)}` : ""));
    }
    L.push(SEP_L, `池内已突破 ${curBroke.length}/${codes.length}`);
    msgs.push(L.join("\n"));
  }

  let summary = "NO_EVENTS";
  if (msgs.length) {
    await env.STATE.put("mon", JSON.stringify({ broke: curBroke, buy: curBuy }));
    summary = "";
    for (const m of msgs) {
      const subject = m.split("\n")[0].slice(0, 60);
      try {
        await smtpSend(env, subject, m);
        summary += "MAIL_OK;";
      } catch (e) {
        summary += "MAIL_FAIL(" + String(e).slice(-100) + ");";
      }
    }
  }
  return {
    summary,
    addBroke: addBroke.map(c => name[c]), rmBroke: rmBroke.map(c => name[c]),
    addBuy: addBuy.map(c => name[c]), rmBuy: rmBuy.map(c => name[c]),
  };
}

/* ---------------- SMTP over TCP（smtp.126.com:465 隐式 TLS） ---------------- */

function b64(s) {
  const bytes = new TextEncoder().encode(s);
  let bin = "";
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin);
}
function b64wc(s) {
  const b = b64(s);
  return b.replace(/(.{76})/g, "$1\r\n");
}

function foldB64Header(name, value) {
  const raw = b64(value);
  if (name.length + raw.length + 16 <= 78) return [`${name}: =?utf-8?B?${raw}?=`];
  // 超长: 按 52 字符(4的倍数, 保证每段独立为合法base64)切成多个 encoded-word, 空格连接
  const words = [];
  for (let i = 0; i < raw.length; i += 52)
    words.push(`=?utf-8?B?${raw.slice(i, i + 52)}?=`);
  return [`${name}: ${words[0]}`, ...words.slice(1).map(x => " " + x)];
}

async function smtpSend(env, subject, body) {
  // connect/read 全程限时：海外 IP 到 126 可能被拖住，绝不让它挂死
  const sock = connect(`${SMTP_HOST}:${SMTP_PORT}`, { secureTransport: "on", allowHalfOpen: false,
    signal: AbortSignal.timeout(60000) });
  const reader = sock.readable.getReader();
  const writer = sock.writable.getWriter();
  const enc = new TextEncoder();
  let buf = "";

  function replyEnd(s) {
    // RFC 5321: "250-" 为续行, "250 " 或独立三 位码行才是回复结束; 宽容 \n 换行
    let pos = 0;
    for (;;) {
      const nl = s.indexOf("\n", pos);
      if (nl === -1) return -1;
      const line = s.slice(pos, nl).replace(/\r$/, "");
      if (/^\d{3}( |$)/.test(line)) return nl + 1;
      pos = nl + 1;
    }
  }
  async function readReply() {
    const deadline = Date.now() + 60000;
    for (;;) {
      const idx = replyEnd(buf);
      if (idx >= 0) { const r = buf.slice(0, idx); buf = buf.slice(idx); return r; }
      if (Date.now() > deadline) throw new Error("smtp timeout, buf=" + buf.slice(-100));
      const { value, done } = await reader.read();
      if (done) throw new Error("smtp closed, buf=" + buf.slice(-100));
      buf += new TextDecoder().decode(value, { stream: true });
    }
  }
  async function cmd(line, expect) {
    await writer.write(enc.encode(line + "\r\n"));
    const r = await readReply();
    if (!r.startsWith(expect)) throw new Error(`smtp expect ${expect}: ${r.trim().slice(-140)}`);
    return r;
  }

  try {
    await readReply();                                            // 220 greeting
    await cmd("EHLO dashboard", "250");
    await cmd("AUTH LOGIN", "334");
    await cmd(b64(env.MAIL_USER), "334");
    await cmd(b64(env.MAIL_PASS), "235");
    await cmd(`MAIL FROM:<${env.MAIL_USER}>`, "250");
    await cmd(`RCPT TO:<${env.MAIL_TO}>`, "250");
    await cmd("DATA", "354");
    const msg = [
      `From: =?utf-8?B?${b64("涨停看板云端")}?= <${env.MAIL_USER}>`,
      `To: <${env.MAIL_TO}>`,
      ...foldB64Header("Subject", subject),
      "MIME-Version: 1.0",
      "Content-Type: text/plain; charset=utf-8",
      "Content-Transfer-Encoding: base64",
      "",
      b64wc(body),
      ".",
    ].join("\r\n");
    await writer.write(enc.encode(msg + "\r\n.\r\n"));
    const r = await readReply();
    if (!r.startsWith("250")) throw new Error("smtp send: " + r.trim().slice(-140));
    try { await cmd("QUIT", "221"); } catch {}
  } finally {
    try { await writer.close(); } catch {}
    try { await reader.cancel(); } catch {}
    try { sock.close(); } catch {}
  }
}
