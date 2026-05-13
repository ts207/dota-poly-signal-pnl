"""Paper trading dashboard — aiohttp server reading CSV log files.

Usage:
    python3 dashboard.py [--port 8080]

Then open http://localhost:8080 in a browser.
Runs alongside main.py; reads the same CSV files the bot writes.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))

from aiohttp import web

from config import (
    CSV_LOG_PATH,
    PAPER_TRADES_CSV_PATH,
    DOTA_EVENTS_CSV_PATH,
)

_FEED_ROWS = 25   # rows shown per feed
_EXIT_ROWS = 30   # closed positions shown


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def _read_csv(path: str | Path) -> list[dict]:
    p = Path(path)
    if not p.exists():
        return []
    try:
        with p.open(newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []


def _fnum(v) -> float | None:
    if v in (None, ""):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Data builders
# ---------------------------------------------------------------------------

def _session_data(trades: list[dict]) -> dict:
    entries = [r for r in trades if (r.get("action") or "").strip().lower() == "entry"]
    exits   = [r for r in trades if (r.get("action") or "").strip().lower() == "exit"]
    pnls    = [_fnum(r.get("pnl_usd")) for r in exits]
    pnls    = [x for x in pnls if x is not None]
    wins    = sum(1 for x in pnls if x > 0)
    total   = sum(pnls) if pnls else 0.0
    costs   = [_fnum(r.get("cost_usd")) for r in entries]
    costs   = [x for x in costs if x is not None]
    return {
        "total_entries": len(entries),
        "total_exits":   len(exits),
        "open_count":    max(len(entries) - len(exits), 0),
        "total_pnl":     round(total, 4),
        "win_rate":      round(wins / len(pnls) * 100, 1) if pnls else None,
        "wins":          wins,
        "losses":        len(pnls) - wins,
        "notional_usd":  round(sum(costs), 2) if costs else 0.0,
    }


def _open_positions(trades: list[dict]) -> list[dict]:
    """Reconstruct open positions from the entry/exit log (FIFO per token)."""
    open_by_token: dict[str, list[dict]] = {}
    for row in trades:
        action   = (row.get("action") or "").strip().lower()
        token_id = str(row.get("token_id") or "")
        if not token_id:
            continue
        if action == "entry":
            open_by_token.setdefault(token_id, []).append(row)
        elif action == "exit":
            bucket = open_by_token.get(token_id)
            if bucket:
                bucket.pop(0)
                if not bucket:
                    del open_by_token[token_id]
    return [row for bucket in open_by_token.values() for row in bucket]


def _closed_positions(trades: list[dict], n: int) -> list[dict]:
    exits = [r for r in trades if (r.get("action") or "").strip().lower() == "exit"]
    return list(reversed(exits[-n:]))


# ---------------------------------------------------------------------------
# API endpoint
# ---------------------------------------------------------------------------

async def _api_data(_request: web.Request) -> web.Response:
    trades  = _read_csv(PAPER_TRADES_CSV_PATH)
    signals = list(reversed(_read_csv(CSV_LOG_PATH)[-_FEED_ROWS:]))
    events  = list(reversed(_read_csv(DOTA_EVENTS_CSV_PATH)[-_FEED_ROWS:]))

    payload = {
        "ts":               datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "stats":            _session_data(trades),
        "open_positions":   _open_positions(trades),
        "closed_positions": _closed_positions(trades, _EXIT_ROWS),
        "signals":          signals,
        "events":           events,
    }
    return web.Response(
        text=json.dumps(payload, default=str),
        content_type="application/json",
        headers={"Access-Control-Allow-Origin": "*"},
    )


# ---------------------------------------------------------------------------
# HTML dashboard
# ---------------------------------------------------------------------------

_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>PAPERBOT · DOTA/POLY</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;700&family=Rajdhani:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
:root {
  --bg:        #07080a;
  --sf:        #0c0d10;
  --sf2:       #11121a;
  --bd:        #191b24;
  --bd2:       #252838;
  --gold:      #c9952a;
  --gold-lo:   rgba(201,149,42,0.12);
  --gold-dim:  #6a4e15;
  --green:     #22c55e;
  --green-lo:  rgba(34,197,94,0.10);
  --red:       #ef4444;
  --red-lo:    rgba(239,68,68,0.10);
  --blue:      #60a5fa;
  --text:      #cdd0dc;
  --mid:       #696b80;
  --dim:       #363848;
  --mono:      'JetBrains Mono', monospace;
  --sans:      'Rajdhani', sans-serif;
}
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

body {
  background: var(--bg);
  color: var(--text);
  font-family: var(--mono);
  font-size: 11.5px;
  line-height: 1.45;
  min-height: 100vh;
  -webkit-font-smoothing: antialiased;
}

/* ── HEADER ─────────────────────────────────────────────────── */
#hdr {
  position: sticky; top: 0; z-index: 200;
  display: flex; align-items: center; justify-content: space-between;
  padding: 0 24px;
  height: 44px;
  background: var(--sf);
  border-bottom: 1px solid var(--bd);
}
.brand {
  font-family: var(--sans);
  font-size: 17px; font-weight: 700;
  letter-spacing: .2em;
  text-transform: uppercase;
  color: var(--gold);
}
.brand em { color: var(--mid); font-style: normal; font-weight: 400; }

.hdr-r { display: flex; align-items: center; gap: 20px; }

.live-pill {
  display: flex; align-items: center; gap: 5px;
  font-size: 10px; letter-spacing: .15em; text-transform: uppercase;
  color: var(--mid);
}
.live-dot {
  width: 5px; height: 5px; border-radius: 50%;
  background: var(--green);
  box-shadow: 0 0 6px var(--green);
  animation: blink 2.2s ease-in-out infinite;
}
@keyframes blink { 0%,100%{opacity:1} 55%{opacity:.25} }

#clock { font-size: 11px; color: var(--mid); letter-spacing: .05em; }
#upd   { font-size: 10px; color: var(--dim); }

/* ── LAYOUT ─────────────────────────────────────────────────── */
#wrap {
  max-width: 1560px; margin: 0 auto;
  padding: 18px 24px;
  display: flex; flex-direction: column; gap: 14px;
}

/* ── KPI STRIP ──────────────────────────────────────────────── */
.kpi-row {
  display: grid;
  grid-template-columns: 2.2fr 1fr 1fr 1fr 1fr;
  gap: 10px;
}
.kpi {
  background: var(--sf);
  border: 1px solid var(--bd);
  padding: 14px 18px 12px;
  position: relative;
  overflow: hidden;
}
.kpi::after {
  content: '';
  position: absolute; left: 0; top: 0; bottom: 0;
  width: 2px;
  background: var(--bd2);
}
.kpi.hero::after { background: var(--gold-dim); }
.kpi.pos-accent::after { background: var(--green); opacity: .6; }
.kpi.neg-accent::after { background: var(--red); opacity: .6; }

.kpi-lbl {
  font-family: var(--sans);
  font-size: 10px; font-weight: 600;
  letter-spacing: .2em; text-transform: uppercase;
  color: var(--mid);
  margin-bottom: 6px;
}
.kpi-val {
  font-family: var(--sans);
  font-size: 30px; font-weight: 700; line-height: 1;
  color: var(--text);
}
.kpi-val.g  { color: var(--green); }
.kpi-val.r  { color: var(--red); }
.kpi-val.au { color: var(--gold); }
.kpi-sub {
  margin-top: 5px;
  font-size: 10px; color: var(--dim);
}

/* ── SECTION LABEL ──────────────────────────────────────────── */
.sec-hdr {
  display: flex; align-items: center; gap: 8px;
  margin-bottom: 6px;
}
.sec-lbl {
  font-family: var(--sans);
  font-size: 10px; font-weight: 600;
  letter-spacing: .22em; text-transform: uppercase;
  color: var(--mid);
}
.sec-cnt {
  font-size: 10px; color: var(--dim);
  background: var(--sf2);
  border: 1px solid var(--bd);
  padding: 1px 6px;
}

/* ── TABLES ─────────────────────────────────────────────────── */
.tbl-wrap {
  background: var(--sf);
  border: 1px solid var(--bd);
  overflow: hidden;
}
table { width: 100%; border-collapse: collapse; }

th {
  padding: 7px 12px;
  font-family: var(--sans);
  font-size: 9.5px; font-weight: 600;
  letter-spacing: .18em; text-transform: uppercase;
  color: var(--mid);
  text-align: left; white-space: nowrap;
  background: var(--sf2);
  border-bottom: 1px solid var(--bd);
}
td {
  padding: 6px 12px;
  border-bottom: 1px solid var(--bd);
  font-size: 11px; white-space: nowrap;
}
tr:last-child td { border-bottom: none; }
tr:hover td { background: rgba(255,255,255,.018); }

.empty-row td {
  padding: 28px; text-align: center;
  color: var(--dim); font-size: 11px;
  border-bottom: none;
}

/* ── FEEDS ──────────────────────────────────────────────────── */
.feeds { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
.feed { background: var(--sf); border: 1px solid var(--bd); overflow: hidden; }

.feed-hdr {
  padding: 7px 12px;
  background: var(--sf2); border-bottom: 1px solid var(--bd);
  font-family: var(--sans);
  font-size: 10px; font-weight: 600;
  letter-spacing: .2em; text-transform: uppercase;
  color: var(--mid);
}
.feed-body { max-height: 380px; overflow-y: auto; }
.feed-body::-webkit-scrollbar { width: 3px; }
.feed-body::-webkit-scrollbar-track { background: transparent; }
.feed-body::-webkit-scrollbar-thumb { background: var(--bd2); }

.fi {
  padding: 7px 12px;
  border-bottom: 1px solid var(--bd);
  display: grid; gap: 2px;
}
.fi:last-child { border-bottom: none; }
.fi-top { display: flex; align-items: center; justify-content: space-between; gap: 8px; }
.fi-main { font-size: 11px; color: var(--text); display: flex; align-items: center; gap: 5px; flex-wrap: wrap; }
.fi-time { font-size: 10px; color: var(--dim); flex-shrink: 0; }
.fi-detail { font-size: 10px; color: var(--mid); }
.feed-empty { padding: 24px; text-align: center; color: var(--dim); font-size: 11px; }

/* ── UTILITY CLASSES ────────────────────────────────────────── */
.g   { color: var(--green); }
.r   { color: var(--red); }
.au  { color: var(--gold); }
.mid { color: var(--mid); }
.dim { color: var(--dim); }

.tag {
  display: inline-block;
  padding: 1px 5px;
  font-size: 9px; font-weight: 700; letter-spacing: .08em; text-transform: uppercase;
}
.tag-buy  { background: var(--green-lo); color: var(--green); }
.tag-skip { background: var(--sf2);      color: var(--dim); }
.tag-hi   { background: var(--gold-lo);  color: var(--gold); }
.tag-med  { background: var(--sf2);      color: var(--mid); }

/* ── SCROLLABLE EXITS ───────────────────────────────────────── */
.exits-wrap { max-height: 320px; overflow-y: auto; }
.exits-wrap::-webkit-scrollbar { width: 3px; }
.exits-wrap::-webkit-scrollbar-track { background: transparent; }
.exits-wrap::-webkit-scrollbar-thumb { background: var(--bd2); }
</style>
</head>
<body>

<div id="hdr">
  <div class="brand">PAPER<em>BOT</em> · <em>DOTA/POLY</em></div>
  <div class="hdr-r">
    <div class="live-pill"><div class="live-dot"></div>LIVE</div>
    <div id="clock">--:--:-- UTC</div>
    <div id="upd">—</div>
  </div>
</div>

<div id="wrap">

  <!-- KPIs -->
  <div class="kpi-row">
    <div class="kpi hero" id="kpi-pnl-card">
      <div class="kpi-lbl">Session P&amp;L</div>
      <div class="kpi-val" id="kpi-pnl">$0.00</div>
      <div class="kpi-sub" id="kpi-pnl-sub">no closed trades</div>
    </div>
    <div class="kpi" id="kpi-wr-card">
      <div class="kpi-lbl">Win Rate</div>
      <div class="kpi-val" id="kpi-wr">—</div>
      <div class="kpi-sub" id="kpi-wr-sub">0W / 0L</div>
    </div>
    <div class="kpi">
      <div class="kpi-lbl">Closed</div>
      <div class="kpi-val au" id="kpi-exits">0</div>
      <div class="kpi-sub" id="kpi-exits-sub">trades</div>
    </div>
    <div class="kpi">
      <div class="kpi-lbl">Deployed</div>
      <div class="kpi-val" id="kpi-notional">$0</div>
      <div class="kpi-sub">total entries</div>
    </div>
    <div class="kpi">
      <div class="kpi-lbl">Open</div>
      <div class="kpi-val" id="kpi-open">0</div>
      <div class="kpi-sub" id="kpi-open-sub">positions</div>
    </div>
  </div>

  <!-- Open positions -->
  <div>
    <div class="sec-hdr">
      <div class="sec-lbl">Open Positions</div>
      <div class="sec-cnt" id="open-cnt">0</div>
    </div>
    <div class="tbl-wrap">
      <table>
        <thead><tr>
          <th>Market</th><th>Side</th><th>Event</th>
          <th>Entry</th><th>Lag</th><th>Exp Move</th>
          <th>Cost</th><th>Age</th><th>Game T</th>
        </tr></thead>
        <tbody id="open-body"><tr class="empty-row"><td colspan="9">no open positions</td></tr></tbody>
      </table>
    </div>
  </div>

  <!-- Feeds -->
  <div class="feeds">
    <div class="feed">
      <div class="feed-hdr">Signal Feed</div>
      <div class="feed-body" id="sig-feed"><div class="feed-empty">no signals yet</div></div>
    </div>
    <div class="feed">
      <div class="feed-hdr">Dota Event Feed</div>
      <div class="feed-body" id="evt-feed"><div class="feed-empty">no events detected</div></div>
    </div>
  </div>

  <!-- Exits -->
  <div>
    <div class="sec-hdr">
      <div class="sec-lbl">Recent Exits</div>
      <div class="sec-cnt" id="exits-cnt">0</div>
    </div>
    <div class="tbl-wrap">
      <div class="exits-wrap">
        <table>
          <thead><tr>
            <th>Market</th><th>Side</th><th>Event</th><th>Reason</th>
            <th>Entry</th><th>Exit</th><th>P&amp;L</th><th>ROI</th><th>Hold</th>
          </tr></thead>
          <tbody id="exits-body"><tr class="empty-row"><td colspan="9">no closed trades</td></tr></tbody>
        </table>
      </div>
    </div>
  </div>

</div><!-- /wrap -->

<script>
// ── helpers ──────────────────────────────────────────────────
const $ = id => document.getElementById(id);

function fmtTimeUTC(iso) {
  if (!iso) return '—';
  try { return new Date(iso).toISOString().slice(11,19); } catch { return '—'; }
}

function ago(iso) {
  if (!iso) return '—';
  try {
    const s = (Date.now() - new Date(iso)) / 1000;
    if (s < 60)   return s.toFixed(0) + 's';
    if (s < 3600) return (s/60).toFixed(0) + 'm';
    return (s/3600).toFixed(1) + 'h';
  } catch { return '—'; }
}

function fmtHold(sec) {
  if (sec == null || sec === '') return '—';
  const s = parseFloat(sec);
  if (s < 60)   return s.toFixed(0) + 's';
  if (s < 3600) return (s/60).toFixed(1) + 'm';
  return (s/3600).toFixed(1) + 'h';
}

function fmtGT(sec) {
  if (sec == null || sec === '') return '—';
  const s = parseInt(sec);
  return Math.floor(s/60) + ':' + String(s%60).padStart(2,'0');
}

function fmtPrice(v) {
  if (v == null || v === '') return '—';
  return parseFloat(v).toFixed(4);
}

function pnlHtml(v) {
  if (v == null || v === '') return '<span class="dim">—</span>';
  const n = parseFloat(v);
  const cls = n >= 0 ? 'g' : 'r';
  const sgn = n >= 0 ? '+' : '';
  return `<span class="${cls}">${sgn}$${Math.abs(n).toFixed(2)}</span>`;
}

function roiHtml(v) {
  if (v == null || v === '') return '<span class="dim">—</span>';
  const n = parseFloat(v) * 100;
  const cls = n >= 0 ? 'g' : 'r';
  const sgn = n >= 0 ? '+' : '';
  return `<span class="${cls}">${sgn}${n.toFixed(1)}%</span>`;
}

function shortMarket(name) {
  if (!name) return '—';
  return name
    .replace(/^Dota\s*2:\s*/i, '')
    .replace(/\s*-\s*Game\s*\d+\s*Winner\s*$/i, '');
}

const EVT_COLOR = {
  SECOND_T4_TOWER_FALL:      '#f97316',
  FIRST_T4_TOWER_FALL:       '#fb923c',
  ULTRA_LATE_WIPE:           '#dc2626',
  LATE_GAME_WIPE:            '#ef4444',
  STOMP_THROW:               '#d946ef',
  MULTIPLE_T3_TOWERS_DOWN:   '#c084fc',
  T3_TOWER_FALL:             '#a78bfa',
  MAJOR_COMEBACK:            '#f43f5e',
  COMEBACK:                  '#fb7185',
  LEAD_SWING_60S:            '#60a5fa',
  LEAD_SWING_30S:            '#93c5fd',
  KILL_CONFIRMED_LEAD_SWING: '#a3e635',
  KILL_BURST_30S:            '#cbd5e1',
  T2_TOWER_FALL:             '#c9952a',
};
const EVT_SHORT = {
  SECOND_T4_TOWER_FALL:      'T4·2',
  FIRST_T4_TOWER_FALL:       'T4·1',
  ULTRA_LATE_WIPE:           'ULTRA WIPE',
  LATE_GAME_WIPE:            'LATE WIPE',
  STOMP_THROW:               'THROW',
  MULTIPLE_T3_TOWERS_DOWN:   'MULTI T3',
  T3_TOWER_FALL:             'T3',
  MAJOR_COMEBACK:            'MAJOR CB',
  COMEBACK:                  'COMEBACK',
  LEAD_SWING_60S:            'LS·60',
  LEAD_SWING_30S:            'LS·30',
  KILL_CONFIRMED_LEAD_SWING: 'KC·LEAD',
  KILL_BURST_30S:            'KB·30',
  T2_TOWER_FALL:             'T2',
};
function evtTag(t) {
  if (!t) return '';
  const c = EVT_COLOR[t] || '#363848';
  const s = EVT_SHORT[t] || t;
  return `<span style="color:${c};font-size:9.5px;font-weight:700;letter-spacing:.06em">${s}</span>`;
}

function dirTag(d) {
  if (!d) return '';
  return `<span class="${d==='radiant'?'g':'r'}" style="font-size:9px">${d.toUpperCase()}</span>`;
}

// ── clock ─────────────────────────────────────────────────────
function tick() {
  const n = new Date();
  $('clock').textContent =
    String(n.getUTCHours()).padStart(2,'0') + ':' +
    String(n.getUTCMinutes()).padStart(2,'0') + ':' +
    String(n.getUTCSeconds()).padStart(2,'0') + ' UTC';
}
setInterval(tick, 1000); tick();

// ── refresh ───────────────────────────────────────────────────
async function refresh() {
  let d;
  try {
    const r = await fetch('/api/data');
    if (!r.ok) throw new Error('HTTP ' + r.status);
    d = await r.json();
  } catch (e) {
    $('upd').textContent = 'error: ' + e.message;
    return;
  }

  $('upd').textContent = 'upd ' + fmtTimeUTC(d.ts);

  // ── KPIs ──────────────────────────────────────────────────
  const pnl = d.stats.total_pnl || 0;
  const pnlEl = $('kpi-pnl');
  const sgn = pnl >= 0 ? '+' : '';
  pnlEl.textContent = sgn + '$' + Math.abs(pnl).toFixed(2);
  pnlEl.className   = 'kpi-val' + (pnl > 0 ? ' g' : pnl < 0 ? ' r' : '');
  $('kpi-pnl-card').className = 'kpi hero' + (pnl > 0 ? ' pos-accent' : pnl < 0 ? ' neg-accent' : '');

  const ex = d.stats.total_exits || 0;
  $('kpi-pnl-sub').textContent = ex ? ex + ' closed trade' + (ex !== 1 ? 's' : '') : 'no closed trades';

  const wr = d.stats.win_rate;
  const wrEl = $('kpi-wr');
  wrEl.textContent = wr != null ? wr.toFixed(0) + '%' : '—';
  wrEl.className   = 'kpi-val' + (wr != null ? (wr >= 50 ? ' g' : ' r') : '');
  $('kpi-wr-card').className = 'kpi' + (wr != null ? (wr >= 50 ? ' pos-accent' : ' neg-accent') : '');
  $('kpi-wr-sub').textContent = (d.stats.wins||0) + 'W / ' + (d.stats.losses||0) + 'L';

  $('kpi-exits').textContent    = d.stats.total_exits || 0;
  $('kpi-exits-sub').textContent = 'of ' + (d.stats.total_entries||0) + ' entries';

  const not = d.stats.notional_usd || 0;
  $('kpi-notional').textContent = '$' + not.toFixed(0);

  const oc = d.stats.open_count || 0;
  $('kpi-open').textContent    = oc;
  $('kpi-open-sub').textContent = oc ? oc + ' active' : 'none active';

  // ── Open positions ────────────────────────────────────────
  $('open-cnt').textContent = d.open_positions.length;
  if (!d.open_positions.length) {
    $('open-body').innerHTML = '<tr class="empty-row"><td colspan="9">no open positions</td></tr>';
  } else {
    $('open-body').innerHTML = d.open_positions.map(p => `<tr>
      <td title="${p.market_name||''}">${shortMarket(p.market_name)}</td>
      <td><span class="${p.side==='YES'?'g':'au'}">${p.side||'—'}</span></td>
      <td>${evtTag(p.event_type)}</td>
      <td class="mid">${fmtPrice(p.entry_price)}</td>
      <td class="au">${p.lag ? parseFloat(p.lag).toFixed(3) : '—'}</td>
      <td class="dim">${p.expected_move ? parseFloat(p.expected_move).toFixed(3) : '—'}</td>
      <td class="mid">$${parseFloat(p.cost_usd||0).toFixed(2)}</td>
      <td class="dim">${ago(p.timestamp_utc)}</td>
      <td class="dim">${fmtGT(p.entry_game_time_sec)}</td>
    </tr>`).join('');
  }

  // ── Signal feed ───────────────────────────────────────────
  if (!d.signals.length) {
    $('sig-feed').innerHTML = '<div class="feed-empty">no signals yet</div>';
  } else {
    $('sig-feed').innerHTML = d.signals.map(s => {
      const buy = s.decision === 'paper_buy_yes';
      const badge = buy
        ? '<span class="tag tag-buy">BUY</span>'
        : `<span class="tag tag-skip">${(s.skip_reason||'skip').slice(0,16)}</span>`;
      const detail = buy
        ? `lag=${parseFloat(s.lag||0).toFixed(3)} · ask=${parseFloat(s.ask||0).toFixed(4)}` +
          (s.market_move_recent != null ? ` · mv=${parseFloat(s.market_move_recent).toFixed(3)}` : '') +
          (s.executable_edge != null ? ` · edge=${parseFloat(s.executable_edge).toFixed(3)}` : '')
        : `gt=${fmtGT(s.game_time_sec)}` +
          (s.steam_age_ms ? ` · steam=${s.steam_age_ms}ms` : '');
      return `<div class="fi">
        <div class="fi-top">
          <div class="fi-main">${badge} ${evtTag(s.event_type)} ${dirTag(s.event_direction)}</div>
          <div class="fi-time">${fmtTimeUTC(s.timestamp_utc)}</div>
        </div>
        <div class="fi-detail">${detail}</div>
      </div>`;
    }).join('');
  }

  // ── Event feed ────────────────────────────────────────────
  if (!d.events.length) {
    $('evt-feed').innerHTML = '<div class="feed-empty">no events detected</div>';
  } else {
    $('evt-feed').innerHTML = d.events.map(e => {
      const sev = (e.severity||'').toLowerCase();
      const sevTag = sev === 'high'
        ? '<span class="tag tag-hi">HIGH</span>'
        : '<span class="tag tag-med">MED</span>';
      const teams = [e.radiant_team, e.dire_team].filter(Boolean).join(' vs ');
      const delta = e.delta ? ` Δ${parseInt(e.delta).toLocaleString()}` : '';
      return `<div class="fi">
        <div class="fi-top">
          <div class="fi-main">${evtTag(e.event_type)} ${dirTag(e.direction)} ${sevTag}</div>
          <div class="fi-time">${fmtTimeUTC(e.timestamp_utc)}</div>
        </div>
        <div class="fi-detail">${teams}${delta} · gt=${fmtGT(e.game_time_sec)}</div>
      </div>`;
    }).join('');
  }

  // ── Exits ─────────────────────────────────────────────────
  $('exits-cnt').textContent = d.closed_positions.length;
  if (!d.closed_positions.length) {
    $('exits-body').innerHTML = '<tr class="empty-row"><td colspan="9">no closed trades</td></tr>';
  } else {
    $('exits-body').innerHTML = d.closed_positions.map(p => {
      const reason = (p.exit_reason||'').replace(/_/g,' ');
      return `<tr>
        <td title="${p.market_name||''}">${shortMarket(p.market_name)}</td>
        <td><span class="${p.side==='YES'?'g':'au'}">${p.side||'—'}</span></td>
        <td>${evtTag(p.event_type)}</td>
        <td class="dim" style="font-size:10px">${reason}</td>
        <td class="mid">${fmtPrice(p.entry_price)}</td>
        <td class="mid">${fmtPrice(p.exit_price)}</td>
        <td>${pnlHtml(p.pnl_usd)}</td>
        <td>${roiHtml(p.roi)}</td>
        <td class="dim">${fmtHold(p.hold_sec)}</td>
      </tr>`;
    }).join('');
  }
}

refresh();
setInterval(refresh, 3000);
</script>
</body>
</html>
"""


async def _index(_request: web.Request) -> web.Response:
    return web.Response(text=_HTML, content_type="text/html")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Paper trading dashboard")
    parser.add_argument("--port", type=int, default=int(os.getenv("DASHBOARD_PORT", "8080")))
    parser.add_argument("--host", default="0.0.0.0")
    args = parser.parse_args()

    app = web.Application()
    app.router.add_get("/", _index)
    app.router.add_get("/api/data", _api_data)

    print(f"Dashboard → http://localhost:{args.port}")
    web.run_app(app, host=args.host, port=args.port, print=None)


if __name__ == "__main__":
    main()
