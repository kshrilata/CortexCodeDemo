import streamlit as st
import streamlit.components.v1 as components
import json
from snowflake.snowpark.context import get_active_session

st.set_page_config(layout="wide")
st.title("Interactive ERD Explorer")

session = get_active_session()


@st.cache_data
def get_databases():
    return session.sql("SHOW DATABASES").collect()


@st.cache_data
def get_schemas(db):
    return session.sql(f"SHOW SCHEMAS IN DATABASE \"{db}\"").collect()


@st.cache_data
def get_tables(db, schema):
    return session.sql(
        f"SELECT TABLE_NAME FROM \"{db}\".INFORMATION_SCHEMA.TABLES "
        f"WHERE TABLE_SCHEMA = '{schema}' AND TABLE_TYPE IN ('BASE TABLE','VIEW') "
        f"ORDER BY TABLE_NAME"
    ).collect()


@st.cache_data
def get_columns(db, schema):
    return session.sql(
        f"SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION "
        f"FROM \"{db}\".INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA = '{schema}' ORDER BY TABLE_NAME, ORDINAL_POSITION"
    ).collect()


@st.cache_data
def get_primary_keys(db, schema):
    rows = session.sql(f"SHOW PRIMARY KEYS IN SCHEMA \"{db}\".\"{schema}\"").collect()
    pk_map = {}
    for r in rows:
        tbl = r["table_name"]
        col = r["column_name"]
        pk_map.setdefault(tbl, []).append(col)
    return pk_map


@st.cache_data
def get_foreign_keys(db, schema):
    rows = session.sql(f"SHOW IMPORTED KEYS IN SCHEMA \"{db}\".\"{schema}\"").collect()
    fks = []
    for r in rows:
        fks.append({
            "fk_table": r["fk_table_name"],
            "fk_column": r["fk_column_name"],
            "pk_table": r["pk_table_name"],
            "pk_column": r["pk_column_name"],
        })
    return fks


@st.cache_data
def check_uniqueness(db, schema, table, column):
    row = session.sql(
        f'SELECT COUNT(DISTINCT "{column}") AS dist, COUNT(*) AS tot, '
        f'COUNT(*) - COUNT("{column}") AS nulls '
        f'FROM (SELECT "{column}" FROM "{db}"."{schema}"."{table}" LIMIT 1000)'
    ).collect()[0]
    return int(row["DIST"]), int(row["TOT"]), int(row["NULLS"])


@st.cache_data
def check_referential_match(db, schema, fk_table, fk_col, pk_table, pk_col):
    row = session.sql(
        f'SELECT COUNT(*) AS matches FROM '
        f'(SELECT "{fk_col}" AS v FROM "{db}"."{schema}"."{fk_table}" LIMIT 500) a '
        f'WHERE a.v IN (SELECT "{pk_col}" FROM "{db}"."{schema}"."{pk_table}")'
    ).collect()[0]
    return int(row["MATCHES"])


db_rows = get_databases()
db_names = sorted([r["name"] for r in db_rows])

with st.sidebar:
    st.header("Configuration")
    selected_db = st.selectbox("Database", db_names, index=0)
    schema_rows = get_schemas(selected_db)
    schema_names = sorted([r["name"] for r in schema_rows if r["name"] != "INFORMATION_SCHEMA"])
    selected_schema = st.selectbox("Schema", schema_names, index=0 if schema_names else None)

if not selected_db or not selected_schema:
    st.warning("Select a database and schema.")
    st.stop()

with st.spinner("Loading metadata..."):
    table_rows = get_tables(selected_db, selected_schema)
    all_tables = [r["TABLE_NAME"] for r in table_rows]
    col_rows = get_columns(selected_db, selected_schema)
    pk_map = get_primary_keys(selected_db, selected_schema)
    explicit_fks = get_foreign_keys(selected_db, selected_schema)

table_columns = {}
for r in col_rows:
    tbl = r["TABLE_NAME"]
    table_columns.setdefault(tbl, []).append({
        "name": r["COLUMN_NAME"],
        "type": r["DATA_TYPE"],
        "nullable": r["IS_NULLABLE"],
    })

with st.sidebar:
    exclude_tables = st.multiselect("Exclude Tables", all_tables, default=[])

visible_tables = [t for t in all_tables if t not in exclude_tables]

inferred_pks = {}
with st.spinner("Inferring primary keys..."):
    for tbl in visible_tables:
        if tbl in pk_map:
            continue
        cols = table_columns.get(tbl, [])
        for c in cols:
            cname = c["name"]
            if cname.upper().endswith(("_ID", "_KEY", "ID")):
                try:
                    dist, tot, nulls = check_uniqueness(selected_db, selected_schema, tbl, cname)
                    if tot > 0 and dist == tot and nulls == 0:
                        inferred_pks.setdefault(tbl, []).append(cname)
                except Exception:
                    pass

all_pk_cols = {}
for tbl in visible_tables:
    for col in pk_map.get(tbl, []):
        all_pk_cols.setdefault(col.upper(), []).append(tbl)
    for col in inferred_pks.get(tbl, []):
        all_pk_cols.setdefault(col.upper(), []).append(tbl)

relationships = []
fk_set = set()
for fk in explicit_fks:
    if fk["fk_table"] in visible_tables and fk["pk_table"] in visible_tables:
        key = (fk["fk_table"], fk["fk_column"], fk["pk_table"], fk["pk_column"])
        if key not in fk_set:
            fk_set.add(key)
            relationships.append({
                "source": fk["fk_table"], "source_col": fk["fk_column"],
                "target": fk["pk_table"], "target_col": fk["pk_column"],
                "type": "explicit", "match_pct": 100,
            })

with st.spinner("Sniffing relationships..."):
    for tbl in visible_tables:
        cols = table_columns.get(tbl, [])
        for c in cols:
            cname = c["name"].upper()
            if cname in all_pk_cols:
                for pk_tbl in all_pk_cols[cname]:
                    if pk_tbl == tbl:
                        continue
                    key = (tbl, c["name"], pk_tbl, c["name"])
                    rev_key = (pk_tbl, c["name"], tbl, c["name"])
                    if key in fk_set or rev_key in fk_set:
                        continue
                    if tbl in pk_map and c["name"] in pk_map[tbl]:
                        continue
                    if tbl in inferred_pks and c["name"] in inferred_pks[tbl]:
                        continue
                    try:
                        matches = check_referential_match(
                            selected_db, selected_schema,
                            tbl, c["name"], pk_tbl, c["name"]
                        )
                        pct = round(matches / 5.0 * 100) if matches > 0 else 0
                        pct = min(pct, 100)
                        if pct >= 40:
                            rtype = "confirmed" if pct >= 80 else "probable"
                            fk_set.add(key)
                            relationships.append({
                                "source": tbl, "source_col": c["name"],
                                "target": pk_tbl, "target_col": c["name"],
                                "type": rtype, "match_pct": pct,
                            })
                    except Exception:
                        pass

with st.sidebar:
    rel_labels = [f"{r['source']}.{r['source_col']} → {r['target']}.{r['target_col']} ({r['type']})" for r in relationships]
    hide_rels = st.multiselect("Hide False Positives", rel_labels, default=[])

filtered_rels = [r for i, r in enumerate(relationships) if rel_labels[i] not in hide_rels]

nodes = []
for i, tbl in enumerate(visible_tables):
    cols = table_columns.get(tbl, [])
    pk_cols = set(pk_map.get(tbl, []) + inferred_pks.get(tbl, []))
    col_list = []
    for c in cols:
        col_list.append({
            "name": c["name"],
            "type": c["type"],
            "pk": c["name"] in pk_cols,
            "inferred_pk": c["name"] in inferred_pks.get(tbl, []),
        })
    nodes.append({
        "id": tbl,
        "columns": col_list,
        "x": 80 + (i % 4) * 320,
        "y": 60 + (i // 4) * 300,
    })

edges = []
for r in filtered_rels:
    color = "#22c55e" if r["type"] == "explicit" else "#3b82f6" if r["type"] == "confirmed" else "#f97316"
    dash = "" if r["type"] != "probable" else "6,4"
    edges.append({
        "source": r["source"], "source_col": r["source_col"],
        "target": r["target"], "target_col": r["target_col"],
        "color": color, "dash": dash, "type": r["type"],
        "match_pct": r["match_pct"],
    })

graph_data = json.dumps({"nodes": nodes, "edges": edges})

d3_html = f"""
<!DOCTYPE html>
<html><head><style>
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ background:#1e1e2e; overflow:hidden; font-family:'Segoe UI',sans-serif; }}
.toolbar {{ position:fixed; top:10px; left:10px; z-index:100; display:flex; gap:6px; }}
.toolbar button {{ background:#313244; color:#cdd6f4; border:1px solid #585b70; padding:6px 14px;
  border-radius:6px; cursor:pointer; font-size:13px; }}
.toolbar button:hover {{ background:#45475a; }}
.legend {{ position:fixed; bottom:10px; left:10px; z-index:100; background:#313244; padding:10px 14px;
  border-radius:8px; border:1px solid #585b70; color:#cdd6f4; font-size:12px; }}
.legend div {{ display:flex; align-items:center; gap:8px; margin:3px 0; }}
.legend span.line {{ display:inline-block; width:30px; height:3px; }}
.tooltip {{ position:absolute; background:#313244; color:#cdd6f4; padding:6px 10px; border-radius:6px;
  font-size:12px; pointer-events:none; border:1px solid #585b70; display:none; z-index:200; }}
</style></head><body>
<div class="toolbar">
  <button onclick="fitToScreen()">Fit to Screen</button>
  <button onclick="resetView()">Reset</button>
</div>
<div class="legend">
  <div><span class="line" style="background:#22c55e"></span> Explicit FK</div>
  <div><span class="line" style="background:#3b82f6"></span> Confirmed (&ge;80%)</div>
  <div><span class="line" style="background:#f97316;border-top:2px dashed #f97316;height:0"></span> Probable (40-79%)</div>
</div>
<div class="tooltip" id="tooltip"></div>
<svg id="canvas" width="100%" height="100%"></svg>
<script>
var DATA = {graph_data};
var svg = document.getElementById('canvas');
var NS = 'http://www.w3.org/2000/svg';
var W = window.innerWidth, H = window.innerHeight;
svg.setAttribute('viewBox', '0 0 ' + W + ' ' + H);

var viewState = {{ tx: 0, ty: 0, scale: 1 }};
var mainG = document.createElementNS(NS, 'g');
svg.appendChild(mainG);

var edgeG = document.createElementNS(NS, 'g');
mainG.appendChild(edgeG);
var nodeG = document.createElementNS(NS, 'g');
mainG.appendChild(nodeG);

var tooltip = document.getElementById('tooltip');
var COL_H = 22, HEADER_H = 32, PAD = 10, TBL_W = 260;

function tableHeight(n) {{ return HEADER_H + n.columns.length * COL_H + PAD; }}

function colY(n, colName) {{
  var idx = 0;
  for (var i = 0; i < n.columns.length; i++) {{
    if (n.columns[i].name === colName) {{ idx = i; break; }}
  }}
  return n.y + HEADER_H + idx * COL_H + COL_H / 2;
}}

function drawTable(n) {{
  var g = document.createElementNS(NS, 'g');
  g.setAttribute('class', 'table-node');
  g.setAttribute('data-id', n.id);

  var h = tableHeight(n);
  var rect = document.createElementNS(NS, 'rect');
  rect.setAttribute('x', n.x); rect.setAttribute('y', n.y);
  rect.setAttribute('width', TBL_W); rect.setAttribute('height', h);
  rect.setAttribute('rx', 8); rect.setAttribute('fill', '#1e1e2e');
  rect.setAttribute('stroke', '#585b70'); rect.setAttribute('stroke-width', 1.5);
  g.appendChild(rect);

  var header = document.createElementNS(NS, 'rect');
  header.setAttribute('x', n.x); header.setAttribute('y', n.y);
  header.setAttribute('width', TBL_W); header.setAttribute('height', HEADER_H);
  header.setAttribute('rx', 8); header.setAttribute('fill', '#45475a');
  g.appendChild(header);

  var clip = document.createElementNS(NS, 'rect');
  clip.setAttribute('x', n.x); clip.setAttribute('y', n.y + HEADER_H - 8);
  clip.setAttribute('width', TBL_W); clip.setAttribute('height', 8);
  clip.setAttribute('fill', '#45475a');
  g.appendChild(clip);

  var title = document.createElementNS(NS, 'text');
  title.setAttribute('x', n.x + TBL_W / 2); title.setAttribute('y', n.y + 21);
  title.setAttribute('text-anchor', 'middle'); title.setAttribute('fill', '#cdd6f4');
  title.setAttribute('font-size', '13'); title.setAttribute('font-weight', 'bold');
  title.textContent = n.id;
  g.appendChild(title);

  for (var i = 0; i < n.columns.length; i++) {{
    var c = n.columns[i];
    var cy = n.y + HEADER_H + i * COL_H;

    if (i % 2 === 0) {{
      var stripe = document.createElementNS(NS, 'rect');
      stripe.setAttribute('x', n.x + 1); stripe.setAttribute('y', cy);
      stripe.setAttribute('width', TBL_W - 2); stripe.setAttribute('height', COL_H);
      stripe.setAttribute('fill', 'rgba(69,71,90,0.3)');
      if (i === n.columns.length - 1) {{ stripe.setAttribute('rx', 6); }}
      g.appendChild(stripe);
    }}

    var colText = document.createElementNS(NS, 'text');
    var prefix = c.pk ? (c.inferred_pk ? '🔑 ' : '🗝️ ') : '   ';
    colText.setAttribute('x', n.x + 12); colText.setAttribute('y', cy + 15);
    colText.setAttribute('fill', c.pk ? '#f9e2af' : '#bac2de');
    colText.setAttribute('font-size', '12');
    colText.setAttribute('data-col', c.name);
    colText.textContent = prefix + c.name;
    g.appendChild(colText);

    var typeText = document.createElementNS(NS, 'text');
    typeText.setAttribute('x', n.x + TBL_W - 10); typeText.setAttribute('y', cy + 15);
    typeText.setAttribute('text-anchor', 'end'); typeText.setAttribute('fill', '#6c7086');
    typeText.setAttribute('font-size', '11');
    typeText.textContent = c.type;
    g.appendChild(typeText);
  }}

  var drag = {{ active: false, ox: 0, oy: 0 }};
  g.addEventListener('mousedown', function(e) {{
    if (e.button !== 0) return;
    e.stopPropagation();
    drag.active = true;
    drag.ox = (e.clientX - viewState.tx) / viewState.scale - n.x;
    drag.oy = (e.clientY - viewState.ty) / viewState.scale - n.y;
    document.addEventListener('mousemove', onDrag);
    document.addEventListener('mouseup', onUp);
  }});
  function onDrag(e) {{
    if (!drag.active) return;
    n.x = (e.clientX - viewState.tx) / viewState.scale - drag.ox;
    n.y = (e.clientY - viewState.ty) / viewState.scale - drag.oy;
    redraw();
  }}
  function onUp() {{ drag.active = false; document.removeEventListener('mousemove', onDrag); document.removeEventListener('mouseup', onUp); }}

  nodeG.appendChild(g);
}}

function drawEdge(e) {{
  var sn = DATA.nodes.find(function(n) {{ return n.id === e.source; }});
  var tn = DATA.nodes.find(function(n) {{ return n.id === e.target; }});
  if (!sn || !tn) return;

  var sy = colY(sn, e.source_col);
  var ty = colY(tn, e.target_col);
  var sx, tx, cx1, cx2;

  if (sn.x + TBL_W < tn.x) {{
    sx = sn.x + TBL_W; tx = tn.x;
    cx1 = sx + 60; cx2 = tx - 60;
  }} else if (tn.x + TBL_W < sn.x) {{
    sx = sn.x; tx = tn.x + TBL_W;
    cx1 = sx - 60; cx2 = tx + 60;
  }} else {{
    sx = sn.x + TBL_W; tx = tn.x + TBL_W;
    cx1 = Math.max(sx, tx) + 80; cx2 = cx1;
  }}

  var path = document.createElementNS(NS, 'path');
  path.setAttribute('d', 'M' + sx + ',' + sy + ' C' + cx1 + ',' + sy + ' ' + cx2 + ',' + ty + ' ' + tx + ',' + ty);
  path.setAttribute('fill', 'none'); path.setAttribute('stroke', e.color);
  path.setAttribute('stroke-width', 2); path.setAttribute('class', 'edge-path');
  if (e.dash) path.setAttribute('stroke-dasharray', e.dash);
  path.setAttribute('data-source', e.source); path.setAttribute('data-target', e.target);
  path.setAttribute('data-scol', e.source_col); path.setAttribute('data-tcol', e.target_col);
  path.setAttribute('marker-end', 'url(#arrow-' + e.color.replace('#','') + ')');

  var hitArea = document.createElementNS(NS, 'path');
  hitArea.setAttribute('d', path.getAttribute('d'));
  hitArea.setAttribute('fill', 'none'); hitArea.setAttribute('stroke', 'transparent');
  hitArea.setAttribute('stroke-width', 14); hitArea.setAttribute('cursor', 'pointer');

  hitArea.addEventListener('mouseenter', function(ev) {{
    path.setAttribute('stroke', '#C8F135');
    path.setAttribute('stroke-width', 5);
    path.style.filter = 'drop-shadow(0 0 6px #C8F135)';
    highlightCols(e.source, e.source_col, e.target, e.target_col, true);
    tooltip.style.display = 'block';
    tooltip.innerHTML = e.source + '.' + e.source_col + ' → ' + e.target + '.' + e.target_col +
      '<br>Type: ' + e.type + ' | Match: ' + e.match_pct + '%';
  }});
  hitArea.addEventListener('mousemove', function(ev) {{
    tooltip.style.left = (ev.clientX + 12) + 'px';
    tooltip.style.top = (ev.clientY - 10) + 'px';
  }});
  hitArea.addEventListener('mouseleave', function() {{
    path.setAttribute('stroke', e.color);
    path.setAttribute('stroke-width', 2);
    path.style.filter = '';
    highlightCols(e.source, e.source_col, e.target, e.target_col, false);
    tooltip.style.display = 'none';
  }});

  edgeG.appendChild(path);
  edgeG.appendChild(hitArea);
}}

function highlightCols(st, sc, tt, tc, on) {{
  var nodes = nodeG.querySelectorAll('.table-node');
  nodes.forEach(function(g) {{
    var id = g.getAttribute('data-id');
    if (id === st || id === tt) {{
      g.querySelectorAll('text[data-col]').forEach(function(t) {{
        var cn = t.getAttribute('data-col');
        if ((id === st && cn === sc) || (id === tt && cn === tc)) {{
          t.setAttribute('fill', on ? '#C8F135' : (t.textContent.indexOf('🔑') >= 0 || t.textContent.indexOf('🗝') >= 0 ? '#f9e2af' : '#bac2de'));
          t.setAttribute('font-weight', on ? 'bold' : 'normal');
        }}
      }});
    }}
  }});
}}

function createArrowMarker(color) {{
  var defs = svg.querySelector('defs') || (function() {{ var d = document.createElementNS(NS, 'defs'); svg.insertBefore(d, svg.firstChild); return d; }})();
  var m = document.createElementNS(NS, 'marker');
  m.setAttribute('id', 'arrow-' + color.replace('#',''));
  m.setAttribute('viewBox', '0 0 10 10'); m.setAttribute('refX', 10); m.setAttribute('refY', 5);
  m.setAttribute('markerWidth', 8); m.setAttribute('markerHeight', 8);
  m.setAttribute('orient', 'auto-start-reverse');
  var p = document.createElementNS(NS, 'path');
  p.setAttribute('d', 'M 0 0 L 10 5 L 0 10 z'); p.setAttribute('fill', color);
  m.appendChild(p); defs.appendChild(m);
}}

createArrowMarker('#22c55e');
createArrowMarker('#3b82f6');
createArrowMarker('#f97316');
createArrowMarker('#C8F135');

function redraw() {{
  while (edgeG.firstChild) edgeG.removeChild(edgeG.firstChild);
  while (nodeG.firstChild) nodeG.removeChild(nodeG.firstChild);
  DATA.nodes.forEach(drawTable);
  DATA.edges.forEach(drawEdge);
}}

redraw();

var isPanning = false, panStart = {{ x: 0, y: 0 }};
svg.addEventListener('mousedown', function(e) {{
  if (e.target === svg || e.target === mainG) {{
    isPanning = true; panStart.x = e.clientX - viewState.tx; panStart.y = e.clientY - viewState.ty;
  }}
}});
document.addEventListener('mousemove', function(e) {{
  if (isPanning) {{
    viewState.tx = e.clientX - panStart.x; viewState.ty = e.clientY - panStart.y;
    mainG.setAttribute('transform', 'translate(' + viewState.tx + ',' + viewState.ty + ') scale(' + viewState.scale + ')');
  }}
}});
document.addEventListener('mouseup', function() {{ isPanning = false; }});

svg.addEventListener('wheel', function(e) {{
  e.preventDefault();
  var factor = e.deltaY < 0 ? 1.1 : 0.9;
  var newScale = viewState.scale * factor;
  if (newScale < 0.1 || newScale > 5) return;
  var rect = svg.getBoundingClientRect();
  var mx = e.clientX - rect.left, my = e.clientY - rect.top;
  viewState.tx = mx - (mx - viewState.tx) * factor;
  viewState.ty = my - (my - viewState.ty) * factor;
  viewState.scale = newScale;
  mainG.setAttribute('transform', 'translate(' + viewState.tx + ',' + viewState.ty + ') scale(' + viewState.scale + ')');
}}, {{ passive: false }});

function fitToScreen() {{
  if (DATA.nodes.length === 0) return;
  var minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  DATA.nodes.forEach(function(n) {{
    if (n.x < minX) minX = n.x;
    if (n.y < minY) minY = n.y;
    if (n.x + TBL_W > maxX) maxX = n.x + TBL_W;
    var h = HEADER_H + n.columns.length * COL_H + PAD;
    if (n.y + h > maxY) maxY = n.y + h;
  }});
  var cw = maxX - minX + 80, ch = maxY - minY + 80;
  var scale = Math.min(W / cw, H / ch, 1.5);
  viewState.scale = scale;
  viewState.tx = (W - cw * scale) / 2 - minX * scale + 40 * scale;
  viewState.ty = (H - ch * scale) / 2 - minY * scale + 40 * scale;
  mainG.setAttribute('transform', 'translate(' + viewState.tx + ',' + viewState.ty + ') scale(' + viewState.scale + ')');
}}

function resetView() {{
  viewState = {{ tx: 0, ty: 0, scale: 1 }};
  mainG.setAttribute('transform', '');
}}

fitToScreen();
</script></body></html>
"""

canvas_height = max(650, len(visible_tables) * 80)
components.html(d3_html, height=canvas_height, scrolling=False)

st.markdown("---")
tab1, tab2 = st.tabs(["Data Quality Alerts", "Orphan Finder"])

with tab1:
    st.subheader("Potential Key Columns with Low Referential Integrity")
    alerts = []
    for r in relationships:
        if r["type"] in ("confirmed", "probable") and r["match_pct"] < 40:
            alerts.append(r)
    low_quality = [r for r in relationships if r["type"] == "probable"]
    if low_quality:
        for r in low_quality:
            st.warning(
                f"**{r['source']}.{r['source_col']}** → **{r['target']}.{r['target_col']}** — "
                f"Only {r['match_pct']}% referential match"
            )
    else:
        st.success("No low-quality inferred relationships detected.")

with tab2:
    st.subheader("Tables With No Detected Connections")
    connected = set()
    for r in filtered_rels:
        connected.add(r["source"])
        connected.add(r["target"])
    orphans = [t for t in visible_tables if t not in connected]
    if orphans:
        for o in orphans:
            st.info(f"🔌 **{o}** — No relationships found")
    else:
        st.success("All visible tables have at least one relationship.")
