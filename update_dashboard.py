import sys

with open("dota-poly-signal-pnl/dashboard.py", "r") as f:
    code = f.read()

# Replace the subagent JS with correct JS
old_js = """
  // ── MATCH_WINNER Research ───────────────────────────────────
  $('mw-cnt').textContent = (d.match_winner||[]).length;
  if (!(d.match_winner||[]).length) {
    $('mw-body').innerHTML = '<tr class="empty-row"><td colspan="8">no match winner signals</td></tr>';
  } else {
    $('mw-body').innerHTML = d.match_winner.map(s => {
      const ts = fmtTimeUTC(s.timestamp_utc);
      const mkt = shortMarket(s.market_name) || '—';
      const evt = evtTag(s.event_type);
      const mapFairDelta = s.map_fair_delta != null ? parseFloat(s.map_fair_delta).toFixed(4) : '—';
      const seriesFairDelta = s.series_fair_delta != null ? parseFloat(s.series_fair_delta).toFixed(4) : '—';
      const neutral = s.neutral_series_fair != null ? parseFloat(s.neutral_series_fair).toFixed(4) : '—';
      const ask = s.match_ask != null ? parseFloat(s.match_ask).toFixed(4) : '—';
      const edge = s.executable_edge != null ? parseFloat(s.executable_edge).toFixed(4) : '—';
      return `<tr>
        <td class="dim">${ts}</td>
        <td title="${s.market_name||''}">${mkt}</td>
        <td>${evt}</td>
        <td class="mid">${mapFairDelta}</td>
        <td class="mid">${seriesFairDelta}</td>
        <td class="dim">${neutral}</td>
        <td class="au">${ask}</td>
        <td class="g">${edge}</td>
      </tr>`;
    }).join('');
  }
"""

new_js = """
  // ── MATCH_WINNER Research ───────────────────────────────────
  $('mw-cnt').textContent = (d.match_winner||[]).length;
  if (!(d.match_winner||[]).length) {
    $('mw-body').innerHTML = '<tr class="empty-row"><td colspan="8">no match winner signals</td></tr>';
  } else {
    $('mw-body').innerHTML = d.match_winner.map(s => {
      const ts = fmtTimeUTC(s.timestamp_utc);
      const evt = evtTag(s.event_type);
      let mapFairDelta = '—';
      if (s.current_map_p_after && s.current_map_p_before) {
         mapFairDelta = (parseFloat(s.current_map_p_after) - parseFloat(s.current_map_p_before)).toFixed(4);
      }
      const seriesFairDelta = s.match_fair_delta != null && s.match_fair_delta !== '' ? parseFloat(s.match_fair_delta).toFixed(4) : '—';
      const ask = s.match_ask != null && s.match_ask !== '' ? parseFloat(s.match_ask).toFixed(4) : '—';
      const edge = s.match_edge != null && s.match_edge !== '' ? parseFloat(s.match_edge).toFixed(4) : '—';
      return `<tr>
        <td class="dim">${ts}</td>
        <td title="${s.match_id||''}">${s.match_id||''}</td>
        <td>${evt}</td>
        <td class="mid">${mapFairDelta}</td>
        <td class="mid">${seriesFairDelta}</td>
        <td class="au">${ask}</td>
        <td class="g">${edge}</td>
      </tr>`;
    }).join('');
  }
"""

if old_js in code:
    code = code.replace(old_js, new_js)
else:
    print("WARNING: Old JS not found!")

# Add HTML section
html_section = """
  <!-- MATCH_WINNER Research -->
  <div>
    <div class="sec-hdr">
      <div class="sec-lbl">MATCH_WINNER Research</div>
      <div class="sec-cnt" id="mw-cnt">0</div>
    </div>
    <div class="tbl-wrap">
      <table>
        <thead><tr>
          <th>Time</th><th>Match ID</th><th>Event</th>
          <th>Map Fair &Delta;</th><th>Series Fair &Delta;</th>
          <th>Match Ask</th><th>Edge</th>
        </tr></thead>
        <tbody id="mw-body"><tr class="empty-row"><td colspan="7">no match winner signals</td></tr></tbody>
      </table>
    </div>
  </div>
"""

# Insert HTML after Rescue Log
if '<!-- Live Prices -->' in code and 'id="mw-cnt"' not in code:
    code = code.replace('<!-- Live Prices -->', html_section + '\n  <!-- Live Prices -->')

with open("dota-poly-signal-pnl/dashboard.py", "w") as f:
    f.write(code)

