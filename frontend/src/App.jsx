import { useState, useEffect, useCallback } from "react";

const API_BASE = window.location.hostname === "localhost"
  ? "http://localhost:8000"
  : "https://usetapscom-production.up.railway.app";

const $ = (n) => n >= 1e6 ? "$" + (n / 1e6).toFixed(2) + "M" : n >= 1e3 ? "$" + (n / 1e3).toFixed(1) + "K" : "$" + n.toFixed(0);
const N = (n) => n >= 1e6 ? (n / 1e6).toFixed(1) + "M" : n >= 1e3 ? (n / 1e3).toFixed(1) + "K" : n.toLocaleString();
const pc = (n) => n.toFixed(1) + "%";

const TABS = [
  { n: "Command Center", i: "◉" }, { n: "Inventory Health", i: "⚡" },
  { n: "Deep Dive", i: "◈" }, { n: "Power Rankings", i: "♛" },
  { n: "Purchase Orders", i: "⬡" },
];

const CANNABIS_CATS = ["Flower", "Pre Rolls", "Concentrates", "Carts", "Disposables", "Edibles", "Infused Flower", "Capsules", "Tinctures", "Topicals"];

export default function TAPSApp() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [tab, setTab] = useState(0);
  const [filters, setFilters] = useState({ s: "All", c: "All", cl: "All", b: "All", q: "" });
  const [sortStack, setSortStack] = useState([]);
  const [wos, setWos] = useState(2.5);
  const [storeView, setStoreView] = useState("");
  const [brandView, setBrandView] = useState("");
  const [healthView, setHealthView] = useState("stockouts"); // stockouts | overstock | dead
  const [diveView, setDiveView] = useState("stores"); // stores | categories | brands
  const [error, setError] = useState(null);
  const [poStore, setPoStore] = useState("");
  const [poBrand, setPoBrand] = useState("");
  const [poEdits, setPoEdits] = useState({}); // { product_key: qty_override }
  const [poExporting, setPoExporting] = useState(false);
  const [poWos, setPoWos] = useState(null); // null = use global wos

  const fetchTaps = useCallback(async (refreshInv = false) => {
    try {
      const url = `${API_BASE}/api/taps?wos=${wos}&refresh_inventory=${refreshInv}`;
      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      setData(json);
      setError(null);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, [wos]);

  useEffect(() => { fetchTaps(); }, [fetchTaps]);

  const refreshInventory = () => { setRefreshing(true); fetchTaps(true); };
  const handleSort = (ci) => {
    setSortStack((prev) => {
      const existing = prev.findIndex((s) => s.ci === ci);
      if (existing !== -1) {
        if (existing === prev.length - 1) {
          if (prev[existing].asc) {
            const next = [...prev];
            next[existing] = { ci, asc: false };
            return next;
          } else {
            return prev.filter((s) => s.ci !== ci);
          }
        } else {
          return [...prev.filter((s) => s.ci !== ci), { ci, asc: true }];
        }
      }
      return [...prev, { ci, asc: true }];
    });
  };

  const switchTab = (i) => { setTab(i); setSortStack([]); setFilters({ s: "All", c: "All", cl: "All", b: "All", q: "" }); };

  if (loading) return (
    <div style={{ background: "#0a0a0a", color: "#22c55e", height: "100vh", display: "flex", alignItems: "center", justifyContent: "center", fontFamily: "'JetBrains Mono', monospace" }}>
      <div style={{ textAlign: "center" }}>
        <div style={{ fontSize: 28, fontWeight: 700, letterSpacing: 4 }}>T A P S</div>
        <div style={{ fontSize: 12, color: "#666", marginTop: 8 }}>Loading...</div>
      </div>
    </div>
  );

  if (error && !data) return (
    <div style={{ background: "#0a0a0a", color: "#ef4444", height: "100vh", display: "flex", alignItems: "center", justifyContent: "center", fontFamily: "'JetBrains Mono', monospace" }}>
      <div style={{ textAlign: "center" }}>
        <div style={{ fontSize: 18 }}>Connection Error</div>
        <div style={{ fontSize: 12, color: "#666", marginTop: 8 }}>{error}</div>
        <button onClick={() => { setLoading(true); fetchTaps(); }} style={{ marginTop: 16, background: "#22c55e22", color: "#22c55e", border: "1px solid #22c55e", padding: "8px 24px", borderRadius: 4, cursor: "pointer", fontFamily: "inherit" }}>Retry</button>
      </div>
    </div>
  );

  const S = data.st;
  const SS = data.ss;
  const P = data.pd;

  const products = P.map((p) => {
    const par = Math.max(Math.round(p.wv * wos), 0);
    return { ...p, par, oq: Math.max(par - p.oh, 0), profit: Math.round((p.nr - p.cogs) * 100) / 100 };
  });

  // Cannabis-only products for all formulas/scores — accessories visible but not weighted
  const cp = products.filter((p) => CANNABIS_CATS.includes(p.cat));

  const getFiltered = (extra) => {
    let d = [...products];
    if (filters.s !== "All") d = d.filter((p) => p.s === filters.s);
    if (filters.c !== "All") d = d.filter((p) => p.cat === filters.c);
    if (filters.cl !== "All") d = d.filter((p) => p.cls === filters.cl);
    if (filters.b !== "All") d = d.filter((p) => p.b === filters.b);
    if (filters.q) d = d.filter((p) => (p.p + p.b).toLowerCase().includes(filters.q.toLowerCase()));
    if (extra) d = d.filter(extra);
    return d;
  };

  const multiSort = (rows, cols) => {
    if (!sortStack.length) return rows;
    return [...rows].sort((a, b) => {
      for (const s of sortStack) {
        const col = cols[s.ci];
        if (!col?.k) continue;
        let av = a[col.k], bv = b[col.k];
        if (av == null) av = s.asc ? Infinity : -Infinity;
        if (bv == null) bv = s.asc ? Infinity : -Infinity;
        let cmp = typeof av === "string" ? av.localeCompare(bv) : av > bv ? 1 : av < bv ? -1 : 0;
        if (!s.asc) cmp = -cmp;
        if (cmp !== 0) return cmp;
      }
      return 0;
    });
  };

  const stores = ["All", ...[...new Set(products.map((p) => p.s))].sort()];
  const cats = ["All", ...[...new Set(products.map((p) => p.cat).filter(Boolean))].sort()];
  const brands = ["All", ...[...new Set(products.map((p) => p.b).filter(Boolean))].sort()];

  // ── COMPONENTS ──
  const KPI = ({ label, value, sub, color }) => (
    <div style={{ background: "#111", border: "1px solid #222", borderRadius: 6, padding: "12px 14px" }}>
      <div style={{ fontSize: 9, color: "#666", fontFamily: "'JetBrains Mono', monospace", textTransform: "uppercase", letterSpacing: 1 }}>{label}</div>
      <div style={{ fontSize: 24, fontWeight: 700, fontFamily: "'JetBrains Mono', monospace", marginTop: 2, color: color || "#e5e5e5" }}>{value}</div>
      {sub && <div style={{ fontSize: 9, color: "#666", fontFamily: "'JetBrains Mono', monospace", marginTop: 1 }}>{sub}</div>}
    </div>
  );

  const filterBar = (extra) => (
    <div style={{ display: "flex", gap: 8, marginBottom: 12, flexWrap: "wrap", alignItems: "flex-end" }}>
      <div><label style={lbl}>Store</label><select style={sel} value={filters.s} onChange={(e) => setFilters((f) => ({ ...f, s: e.target.value }))}>{stores.map((s) => <option key={s}>{s}</option>)}</select></div>
      <div><label style={lbl}>Category</label><select style={sel} value={filters.c} onChange={(e) => setFilters((f) => ({ ...f, c: e.target.value }))}>{cats.map((c) => <option key={c}>{c}</option>)}</select></div>
      <div><label style={lbl}>Brand</label><select style={sel} value={filters.b} onChange={(e) => setFilters((f) => ({ ...f, b: e.target.value }))}>{brands.map((b) => <option key={b}>{b}</option>)}</select></div>
      <div><label style={lbl}>Class</label><select style={sel} value={filters.cl} onChange={(e) => setFilters((f) => ({ ...f, cl: e.target.value }))}>{["All", "A", "B", "C", "D"].map((c) => <option key={c}>{c}</option>)}</select></div>
      <div><label style={lbl}>Search</label><input id="taps-search" style={{ ...sel, width: 200 }} placeholder="Product or brand..." value={filters.q} onChange={(e) => setFilters((f) => ({ ...f, q: e.target.value }))} /></div>
      {extra}
    </div>
  );

  const Table = ({ rows, cols, maxRows = 500 }) => {
    const sorted = multiSort(rows, cols);
    return (
      <div style={{ overflowX: "auto", fontSize: 10, fontFamily: "'JetBrains Mono', monospace" }}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead><tr>{cols.map((c, i) => {
            const si = sortStack.findIndex((s) => s.ci === i);
            const ar = si !== -1 ? (sortStack[si].asc ? " ▲" : " ▼") : "";
            const badge = si !== -1 && sortStack.length > 1 ? si + 1 : "";
            return (
              <th key={i} onClick={() => handleSort(i)} style={{ ...th, textAlign: c.nm ? "right" : "left" }}>
                {c.l}{ar}{badge ? <sup style={{ color: "#f59e0b", fontSize: 8 }}>{badge}</sup> : ""}
              </th>
            );
          })}</tr>
          </thead>
          <tbody>{sorted.slice(0, maxRows).map((r, ri) => (
            <tr key={ri} style={{ background: ri % 2 === 0 ? "transparent" : "#0d0d0d" }}>
              {cols.map((c, ci) => (
                <td key={ci} style={{ ...td, textAlign: c.nm ? "right" : "left", ...(c.c ? c.c(r) : {}) }}>{c.g(r)}</td>
              ))}
            </tr>
          ))}</tbody>
        </table>
      </div>
    );
  };

  const Summary = ({ items }) => (
    <div style={{ display: "flex", gap: 14, marginBottom: 10, flexWrap: "wrap" }}>
      {items.map((item, i) => (
        <div key={i} style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: "#666" }}>
          {item.label}: <span style={{ fontWeight: 600, color: item.color || "#666" }}>{item.value}</span>
        </div>
      ))}
    </div>
  );

  const Trend = (r) => {
    const t = r.tr || 0;
    if (t === 0 || r.wv === 0) return <span style={{ color: "#444" }}>—</span>;
    const arrow = t > 0 ? "▲" : "▼";
    const color = t >= 20 ? "#22c55e" : t > 0 ? "#4ade80" : t > -20 ? "#f97316" : "#ef4444";
    return <span style={{ color, fontWeight: 600, fontSize: 9 }}>{arrow} {Math.abs(t)}%</span>;
  };

  // Sub-tab toggle button
  const SubTab = ({ label, active, onClick, color }) => (
    <button onClick={onClick} style={{
      background: active ? (color || "#22c55e") + "22" : "#111",
      color: active ? (color || "#22c55e") : "#666",
      border: `1px solid ${active ? (color || "#22c55e") + "66" : "#222"}`,
      padding: "6px 16px", borderRadius: 4, cursor: "pointer",
      fontFamily: "'JetBrains Mono', monospace", fontSize: 10, fontWeight: active ? 700 : 400,
    }}>{label}</button>
  );

  // ── COLUMN DEFS ──

  // ── COMMAND CENTER ──
  const renderCC = () => {
    const dead = cp.filter((p) => p.wv === 0);
    const deadC = dead.reduce((a, p) => a + p.ic, 0);
    const over = cp.filter((p) => p.wos && p.wos > 8 && p.wv > 0);
    const overC = over.reduce((a, p) => a + p.ic, 0);
    const cpInvCost = cp.reduce((a, p) => a + p.ic, 0);
    const at = cpInvCost > 0 ? (S.cogs * 365 / 31) / cpInvCost : 0;

    // ── Store Efficiency Scores ──
    const storeScores = SS.map((s) => {
      const sp = cp.filter((p) => p.s === s.s);
      const totalIC = sp.reduce((a, p) => a + p.ic, 0) || 1;
      const totalUnits = sp.reduce((a, p) => a + p.oh, 0) || 1;

      // Dead Weight (40%): cost ratio of zero-velocity inventory
      const deadItems = sp.filter((p) => p.wv === 0);
      const deadCost = deadItems.reduce((a, p) => a + p.ic, 0);
      const deadRatio = deadCost / totalIC;
      const deadScore = Math.max(0, 100 - deadRatio * 250); // 0% dead = 100, 40% dead = 0

      // Overstock (30%): excess units beyond par as cost ratio
      const overItems = sp.filter((p) => p.wos && p.wos > 8 && p.wv > 0);
      const excessCost = overItems.reduce((a, p) => a + Math.max(p.ic - (p.par * p.uc), 0), 0);
      const overRatio = excessCost / totalIC;
      const overScore = Math.max(0, 100 - overRatio * 300); // 0% excess = 100, 33% = 0

      // WOS Balance (30%): % of active SKUs within healthy 1-6 WOS range
      const active = sp.filter((p) => p.wv > 0);
      const healthy = active.filter((p) => p.wos && p.wos >= 1 && p.wos <= 6);
      const balanceScore = active.length > 0 ? (healthy.length / active.length) * 100 : 50;

      const raw = deadScore * 0.40 + overScore * 0.30 + balanceScore * 0.30;
      const score = Math.round(Math.max(0, Math.min(100, raw)));

      // Grade
      let grade, gradeColor;
      if (score >= 85)      { grade = "A"; gradeColor = "#22c55e"; }
      else if (score >= 70) { grade = "B"; gradeColor = "#3b82f6"; }
      else if (score >= 55) { grade = "C"; gradeColor = "#f59e0b"; }
      else                  { grade = "D"; gradeColor = "#ef4444"; }

      // Action items
      const actions = [];
      if (deadCost > 1000) actions.push({ text: `${$(deadCost)} dead weight`, color: "#ef4444", type: "dead" });
      if (excessCost > 1000) actions.push({ text: `${$(excessCost)} overstock`, color: "#f97316", type: "over" });
      const stockouts = sp.filter((p) => p.wos != null && p.wos < 1 && p.wv >= 3);
      if (stockouts.length > 0) actions.push({ text: `${stockouts.length} stockout risk`, color: "#ef4444", type: "stockout" });

      return {
        name: s.s, score, grade, gradeColor, deadCost, deadScore: Math.round(deadScore),
        excessCost, overScore: Math.round(overScore), balanceScore: Math.round(balanceScore),
        deadItems: deadItems.length, overItems: overItems.length,
        healthy: healthy.length, active: active.length, total: sp.length,
        inv: totalIC, rev: s.rev, actions,
      };
    }).sort((a, b) => b.score - a.score);

    const avgScore = storeScores.length > 0 ? Math.round(storeScores.reduce((a, s) => a + s.score, 0) / storeScores.length) : 0;
    const totalLiability = storeScores.reduce((a, s) => a + s.deadCost + s.excessCost, 0);

    return (<>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(190px, 1fr))", gap: 10, marginBottom: 16 }}>
        <KPI label="Net Revenue" value={$(S.net_revenue)} sub="Post-discount · Validated ✓" color="#22c55e" />
        <KPI label="Gross Profit" value={$(S.gross_profit)} sub={pc(S.margin) + " margin"} color="#22c55e" />
        <KPI label="Discounts" value={$(S.discounts)} sub={pc(S.discount_rate) + " of gross"} color="#f59e0b" />
        <KPI label="Units Sold" value={N(S.units_sold)} sub={Math.round(S.units_sold / 31) + "/day"} color="#3b82f6" />
        <KPI label="Inventory" value={$(S.total_inv_cost)} sub={N(S.total_inv_units) + " units · " + N(S.total_products) + " SKUs"} />
        <KPI label="Ann. Turns" value={at.toFixed(1) + "x"} sub="COGS/inventory" color="#3b82f6" />
        <KPI label="Dead Weight" value={$(deadC)} sub={dead.length + " SKUs · 0 sales"} color="#ef4444" />
        <KPI label="Overstock >8wk" value={$(overC)} sub={over.length + " SKUs"} color="#f97316" />
      </div>

      {/* ── INVENTORY EFFICIENCY VISUALIZER ── */}
      <div style={{ background: "#111", border: "1px solid #222", borderRadius: 8, padding: "16px 18px", marginBottom: 16 }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 14 }}>
          <div>
            <span style={{ fontSize: 13, fontWeight: 700, color: "#e5e5e5", fontFamily: "'JetBrains Mono', monospace" }}>
              INVENTORY EFFICIENCY
            </span>
            <span style={{ fontSize: 9, color: "#555", fontFamily: "'JetBrains Mono', monospace", marginLeft: 10 }}>
              Dead Weight (40%) · Overstock (30%) · WOS Balance (30%)
            </span>
          </div>
          <div style={{ display: "flex", gap: 12, alignItems: "center" }}>
            <span style={{ fontSize: 10, color: "#666", fontFamily: "'JetBrains Mono', monospace" }}>
              Fleet Avg: <span style={{ color: avgScore >= 70 ? "#22c55e" : avgScore >= 55 ? "#f59e0b" : "#ef4444", fontWeight: 700, fontSize: 14 }}>{avgScore}</span>
            </span>
            <span style={{ fontSize: 10, color: "#666", fontFamily: "'JetBrains Mono', monospace" }}>
              Total Liability: <span style={{ color: "#ef4444", fontWeight: 700 }}>{$(totalLiability)}</span>
            </span>
          </div>
        </div>

        {storeScores.map((s, i) => (
          <div key={s.name} style={{
            padding: "10px 0", borderBottom: i < storeScores.length - 1 ? "1px solid #1a1a1a" : "none",
            display: "grid", gridTemplateColumns: "120px 50px 1fr 200px", gap: 12, alignItems: "center",
          }}>
            {/* Store name */}
            <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 12, fontWeight: 600, color: "#e5e5e5" }}>
              {s.name}
            </div>

            {/* Grade badge */}
            <div>
              <span style={{
                background: s.gradeColor + "22", color: s.gradeColor,
                padding: "3px 10px", borderRadius: 4, fontWeight: 800, fontSize: 13,
                fontFamily: "'JetBrains Mono', monospace",
              }}>{s.grade}</span>
            </div>

            {/* Score bar */}
            <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
              {/* Main efficiency bar */}
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <div style={{ flex: 1, height: 16, background: "#1a1a1a", borderRadius: 4, overflow: "hidden", position: "relative" }}>
                  {/* Score fill */}
                  <div style={{
                    width: `${s.score}%`, height: "100%", borderRadius: 4,
                    background: `linear-gradient(90deg, ${s.gradeColor}88, ${s.gradeColor})`,
                    transition: "width 0.5s ease",
                  }} />
                  {/* 100 target line */}
                  <div style={{
                    position: "absolute", right: 0, top: 0, bottom: 0, width: 2,
                    background: "#22c55e44",
                  }} />
                  {/* Score label inside bar */}
                  <div style={{
                    position: "absolute", top: 0, left: 0, right: 0, bottom: 0,
                    display: "flex", alignItems: "center", justifyContent: "center",
                    fontSize: 10, fontWeight: 700, fontFamily: "'JetBrains Mono', monospace",
                    color: s.score > 50 ? "#fff" : "#888",
                  }}>{s.score}</div>
                </div>
              </div>

              {/* Component breakdown mini bars */}
              <div style={{ display: "flex", gap: 6, fontSize: 8, fontFamily: "'JetBrains Mono', monospace" }}>
                <div style={{ display: "flex", alignItems: "center", gap: 3 }}>
                  <div style={{ width: 30, height: 3, background: "#1a1a1a", borderRadius: 2, overflow: "hidden" }}>
                    <div style={{ width: `${s.deadScore}%`, height: "100%", background: s.deadScore >= 70 ? "#22c55e" : s.deadScore >= 40 ? "#f59e0b" : "#ef4444", borderRadius: 2 }} />
                  </div>
                  <span style={{ color: "#555" }}>dead</span>
                </div>
                <div style={{ display: "flex", alignItems: "center", gap: 3 }}>
                  <div style={{ width: 30, height: 3, background: "#1a1a1a", borderRadius: 2, overflow: "hidden" }}>
                    <div style={{ width: `${s.overScore}%`, height: "100%", background: s.overScore >= 70 ? "#22c55e" : s.overScore >= 40 ? "#f59e0b" : "#ef4444", borderRadius: 2 }} />
                  </div>
                  <span style={{ color: "#555" }}>over</span>
                </div>
                <div style={{ display: "flex", alignItems: "center", gap: 3 }}>
                  <div style={{ width: 30, height: 3, background: "#1a1a1a", borderRadius: 2, overflow: "hidden" }}>
                    <div style={{ width: `${s.balanceScore}%`, height: "100%", background: s.balanceScore >= 70 ? "#22c55e" : s.balanceScore >= 40 ? "#f59e0b" : "#ef4444", borderRadius: 2 }} />
                  </div>
                  <span style={{ color: "#555" }}>bal</span>
                </div>
              </div>
            </div>

            {/* Action items / what's dragging them down */}
            <div style={{ display: "flex", flexDirection: "column", gap: 2, fontSize: 9, fontFamily: "'JetBrains Mono', monospace" }}>
              {s.actions.length > 0 ? s.actions.map((a, ai) => (
                <span key={ai} style={{ color: a.color }}>→ {a.text}</span>
              )) : (
                <span style={{ color: "#22c55e" }}>✓ running clean</span>
              )}
            </div>
          </div>
        ))}
      </div>

      {/* Original store cards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))", gap: 10 }}>
        {SS.map((s) => (
          <div key={s.s} style={{ background: "#111", border: "1px solid #222", borderRadius: 6, padding: "12px 14px" }}>
            <div style={{ fontSize: 13, fontWeight: 600, color: "#fff", marginBottom: 6, fontFamily: "'JetBrains Mono', monospace" }}>{s.s}</div>
            {[
              ["Net Rev", $(s.rev), "#22c55e"], ["Margin", pc(s.margin), s.margin >= 52 ? "#22c55e" : "#f59e0b"],
              ["Discounts", $(s.disc), "#f59e0b"], ["COGS", $(s.cogs)],
              ["Inventory", $(s.inv_cost)], ["Dead", `${$(s.dead_cost)} (${pc(s.dead_pct)})`, "#ef4444"],
            ].map(([l, v, c]) => (
              <div key={l} style={{ display: "flex", justifyContent: "space-between", fontSize: 10, fontFamily: "'JetBrains Mono', monospace", padding: "2px 0", borderBottom: "1px solid #1a1a1a" }}>
                <span style={{ color: "#666" }}>{l}</span><span style={{ fontWeight: 500, color: c || "#e5e5e5" }}>{v}</span>
              </div>
            ))}
          </div>
        ))}
      </div>
    </>);
  };

  // ── POWER RANKINGS ──
  const renderPowerRankings = () => {
    const brandData = {};
    products.forEach((p) => {
      if (!p.b) return;
      if (!CANNABIS_CATS.includes(p.cat)) return;
      if (!brandData[p.b]) brandData[p.b] = { rev: 0, cogs: 0, profit: 0, inv: 0, vel: 0, units: 0, skus: 0, dead: 0, w1: 0, w2: 0, w3: 0, w4: 0 };
      const d = brandData[p.b];
      d.rev += p.nr; d.cogs += p.cogs; d.profit += p.profit; d.inv += p.ic;
      d.vel += p.wv; d.units += p.oh; d.skus++; d.w1 += (p.w1 || 0); d.w2 += (p.w2 || 0); d.w3 += (p.w3 || 0); d.w4 += (p.w4 || 0);
      if (p.wv === 0) d.dead++;
    });

    const totalRev = Object.values(brandData).reduce((a, d) => a + d.rev, 0);
    const totalProfit = Object.values(brandData).reduce((a, d) => a + d.profit, 0);

    // Power Score: 0-100 composite
    // Revenue contribution (35%) — log-scaled share of total revenue
    // Profit contribution (30%) — share of total profit (can be negative)
    // Margin quality (15%) — margin % vs 50% benchmark
    // Momentum (10%) — WoW velocity trend
    // Efficiency penalty (-10%) — dead SKU ratio drags score down
    const rawScores = Object.entries(brandData).map(([name, d]) => {
      const margin = d.rev > 0 ? (d.rev - d.cogs) / d.rev * 100 : 0;
      const revShare = totalRev > 0 ? d.rev / totalRev * 100 : 0;
      const profitShare = totalProfit > 0 ? d.profit / totalProfit * 100 : 0;

      // Revenue: log-scaled so #1 brand doesn't dominate
      const revScore = revShare > 0 ? Math.min(Math.log10(revShare + 1) / Math.log10(101) * 100, 100) : 0;

      // Profit: allow negatives to penalize money-losers
      const profitScore = Math.max(Math.min(profitShare * 5, 100), -50);

      // Margin: 50% = perfect, scale linearly
      const marginScore = Math.max(Math.min(margin / 50 * 100, 100), 0);

      // Momentum: WoW trend clamped
      const priorAvg = (d.w2 + d.w3 + d.w4) / 3;
      const momentum = priorAvg > 0 ? ((d.w1 - priorAvg) / priorAvg * 100) : (d.w1 > 0 ? 25 : -25);
      const momentumScore = Math.max(Math.min((momentum + 50), 100), 0);

      // Dead penalty: ratio of dead SKUs to total
      const deadRatio = d.skus > 0 ? d.dead / d.skus : 0;
      const deadPenalty = deadRatio * 100; // 0 = no dead, 100 = all dead

      const rawScore =
        revScore * 0.35 +
        profitScore * 0.30 +
        marginScore * 0.15 +
        momentumScore * 0.10 -
        deadPenalty * 0.10;

      return { name, d, margin, revShare, profitShare, momentum: Math.round(momentum), rawScore };
    }).filter((b) => b.d.rev > 0 || b.d.inv > 0);

    // Normalize to 0-100 using min/max
    const scores = rawScores.map((b) => b.rawScore);
    const minScore = Math.min(...scores);
    const maxScore = Math.max(...scores);
    const range = maxScore - minScore || 1;

    const ranked = rawScores.map((b) => {
      const powerScore = Math.round(((b.rawScore - minScore) / range) * 100);
      return {
        name: b.name, rev: b.d.rev, profit: b.d.profit, margin: b.margin,
        inv: b.d.inv, vel: b.d.vel, skus: b.d.skus, dead: b.d.dead,
        revShare: b.revShare, profitShare: b.profitShare, momentum: b.momentum,
        powerScore, w1: b.d.w1,
      };
    }).sort((a, b) => b.powerScore - a.powerScore);

    // Percentile-based grading: S=top 10%, A=next 20%, B=next 30%, C=next 25%, D=bottom 15%
    const total = ranked.length;
    ranked.forEach((b, i) => {
      const pct = i / total;
      if (pct < 0.10)      { b.grade = "S"; b.gradeColor = "#22c55e"; }
      else if (pct < 0.30) { b.grade = "A"; b.gradeColor = "#4ade80"; }
      else if (pct < 0.60) { b.grade = "B"; b.gradeColor = "#3b82f6"; }
      else if (pct < 0.85) { b.grade = "C"; b.gradeColor = "#f59e0b"; }
      else                 { b.grade = "D"; b.gradeColor = "#ef4444"; }
    });

    const maxPower = ranked[0]?.powerScore || 1;

    return (<>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(190px, 1fr))", gap: 10, marginBottom: 16 }}>
        <KPI label="Total Brands" value={ranked.length} color="#3b82f6" />
        <KPI label="S-Tier" value={ranked.filter((b) => b.grade === "S").length} sub="Elite performers" color="#22c55e" />
        <KPI label="D-Tier" value={ranked.filter((b) => b.grade === "D").length} sub="Underperformers" color="#ef4444" />
        <KPI label="Dead SKU Brands" value={ranked.filter((b) => b.dead > 0).length} sub="Have zero-velocity SKUs" color="#f97316" />
      </div>

      <div style={{ fontSize: 9, color: "#555", fontFamily: "'JetBrains Mono', monospace", marginBottom: 8 }}>
        Score = Revenue (35%) + Profit (30%) + Margin Quality (15%) + Momentum (10%) − Dead SKU Penalty (10%) · Graded by percentile
      </div>

      <div style={{ overflowX: "auto", fontSize: 10, fontFamily: "'JetBrains Mono', monospace" }}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead><tr>
            {["#", "Grade", "Brand", "Power Score", "Revenue", "Profit", "Margin", "Rev Share", "Velocity", "Momentum", "Inventory", "SKUs", "Dead"].map((h, i) => (
              <th key={h} style={{ ...th, textAlign: i <= 2 ? "left" : "right", cursor: "default" }}>{h}</th>
            ))}
          </tr></thead>
          <tbody>
            {ranked.map((b, i) => (
              <tr key={b.name} style={{ background: i % 2 === 0 ? "transparent" : "#0d0d0d" }}>
                <td style={{ ...td, color: "#555", fontWeight: 700 }}>{i + 1}</td>
                <td style={{ ...td }}>
                  <span style={{ background: b.gradeColor + "22", color: b.gradeColor, padding: "2px 8px", borderRadius: 3, fontWeight: 800, fontSize: 11 }}>{b.grade}</span>
                </td>
                <td style={{ ...td, color: "#e5e5e5", fontWeight: 600, maxWidth: 200 }}>
                  <span onClick={() => { setTab(2); setDiveView("brands"); setBrandView(b.name); setSortStack([]); }}
                    style={{ cursor: "pointer", borderBottom: "1px dashed #555", transition: "all .15s" }}
                    onMouseEnter={(e) => { e.target.style.color = "#22c55e"; e.target.style.borderColor = "#22c55e"; }}
                    onMouseLeave={(e) => { e.target.style.color = "#e5e5e5"; e.target.style.borderColor = "#555"; }}
                  >{b.name}</span>
                </td>
                <td style={{ ...td, textAlign: "right" }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 6, justifyContent: "flex-end" }}>
                    <div style={{ width: 60, height: 6, background: "#1a1a1a", borderRadius: 3, overflow: "hidden" }}>
                      <div style={{ width: `${(b.powerScore / maxPower) * 100}%`, height: "100%", background: b.gradeColor, borderRadius: 3 }} />
                    </div>
                    <span style={{ color: b.gradeColor, fontWeight: 700 }}>{b.powerScore}</span>
                  </div>
                </td>
                <td style={{ ...td, textAlign: "right", color: "#22c55e" }}>{$(b.rev)}</td>
                <td style={{ ...td, textAlign: "right", color: b.profit > 0 ? "#4ade80" : "#ef4444" }}>{$(b.profit)}</td>
                <td style={{ ...td, textAlign: "right", color: b.margin >= 55 ? "#22c55e" : b.margin >= 45 ? "#f59e0b" : "#ef4444" }}>{pc(b.margin)}</td>
                <td style={{ ...td, textAlign: "right", color: "#888" }}>{pc(b.revShare)}</td>
                <td style={{ ...td, textAlign: "right", color: "#3b82f6" }}>{b.vel.toFixed(1)}/wk</td>
                <td style={{ ...td, textAlign: "right" }}>
                  {b.momentum !== 0 ? <span style={{ color: b.momentum > 0 ? "#22c55e" : "#ef4444", fontWeight: 600 }}>{b.momentum > 0 ? "▲" : "▼"} {Math.abs(b.momentum)}%</span> : <span style={{ color: "#444" }}>—</span>}
                </td>
                <td style={{ ...td, textAlign: "right" }}>{$(b.inv)}</td>
                <td style={{ ...td, textAlign: "right", color: "#888" }}>{b.skus}</td>
                <td style={{ ...td, textAlign: "right", color: b.dead > 0 ? "#ef4444" : "#444" }}>{b.dead || "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>);
  };

  // ── DEEP DIVE: CATEGORIES ──
  const renderCategoryDive = () => {
    const catData = {};
    products.forEach((p) => {
      if (!p.cat) return;
      if (!catData[p.cat]) catData[p.cat] = { rev: 0, cogs: 0, profit: 0, inv: 0, vel: 0, units: 0, skus: 0, dead: 0, w1: 0, w2: 0, w3: 0, w4: 0 };
      const d = catData[p.cat];
      d.rev += p.nr; d.cogs += p.cogs; d.profit += p.profit; d.inv += p.ic;
      d.vel += p.wv; d.units += p.oh; d.skus++; d.w1 += (p.w1 || 0); d.w2 += (p.w2 || 0); d.w3 += (p.w3 || 0); d.w4 += (p.w4 || 0);
      if (p.wv === 0) d.dead++;
    });
    const totalRev = Object.values(catData).reduce((a, d) => a + d.rev, 0);
    const catList = Object.entries(catData).sort((a, b) => b[1].rev - a[1].rev);

    return (<>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))", gap: 10, marginBottom: 16 }}>
        {catList.map(([cat, d]) => {
          const margin = d.rev > 0 ? (d.rev - d.cogs) / d.rev * 100 : 0;
          const share = totalRev > 0 ? d.rev / totalRev * 100 : 0;
          const pa = (d.w2 + d.w3 + d.w4) / 3;
          const trend = pa > 0 ? ((d.w1 - pa) / pa * 100) : 0;
          return (
            <div key={cat} style={{ background: "#111", border: "1px solid #222", borderRadius: 6, padding: "12px 14px" }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
                <div style={{ fontSize: 13, fontWeight: 700, color: "#e5e5e5" }}>{cat}</div>
                <div style={{ fontSize: 10, fontWeight: 600, color: trend >= 0 ? "#22c55e" : "#ef4444", fontFamily: "'JetBrains Mono', monospace" }}>
                  {trend !== 0 ? `${trend > 0 ? "▲" : "▼"} ${Math.abs(Math.round(trend))}%` : "—"}
                </div>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "4px 16px", fontSize: 10, fontFamily: "'JetBrains Mono', monospace" }}>
                <div style={{ color: "#666" }}>Revenue</div><div style={{ textAlign: "right", color: "#22c55e" }}>{$(d.rev)}</div>
                <div style={{ color: "#666" }}>Profit</div><div style={{ textAlign: "right", color: "#4ade80" }}>{$(d.profit)}</div>
                <div style={{ color: "#666" }}>Margin</div><div style={{ textAlign: "right", color: margin >= 52 ? "#22c55e" : "#f59e0b" }}>{pc(margin)}</div>
                <div style={{ color: "#666" }}>Velocity</div><div style={{ textAlign: "right", color: "#3b82f6" }}>{d.vel.toFixed(1)}/wk</div>
                <div style={{ color: "#666" }}>Rev Share</div><div style={{ textAlign: "right" }}>{pc(share)}</div>
                <div style={{ color: "#666" }}>Inventory</div><div style={{ textAlign: "right" }}>{$(d.inv)} ({N(d.units)}u)</div>
                <div style={{ color: "#666" }}>SKUs</div><div style={{ textAlign: "right" }}>{d.skus} <span style={{ color: d.dead > 0 ? "#ef4444" : "#444" }}>({d.dead} dead)</span></div>
              </div>
            </div>
          );
        })}
      </div>
    </>);
  };

  // ── DEEP DIVE: STORES ──
  const renderStoreDive = () => {
    const sv = storeView || SS[0]?.s;
    const d = products.filter((p) => p.s === sv).sort((a, b) => b.nr - a.nr);
    const ss = SS.find((s) => s.s === sv) || {};
    const selling = d.filter((p) => p.wv > 0), dead = d.filter((p) => p.wv === 0);
    return (<>
      <div style={{ marginBottom: 12 }}><label style={lbl}>Store</label>
        <select style={sel} value={sv} onChange={(e) => setStoreView(e.target.value)}>
          {SS.map((s) => <option key={s.s}>{s.s}</option>)}
        </select>
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(190px, 1fr))", gap: 10, marginBottom: 16 }}>
        <KPI label="Net Revenue" value={$(ss.rev || 0)} color="#22c55e" />
        <KPI label="Margin" value={pc(ss.margin || 0)} color={ss.margin >= 52 ? "#22c55e" : "#f59e0b"} />
        <KPI label="Inventory" value={$(ss.inv_cost || 0)} sub={N(ss.inv_units || 0) + " units"} />
        <KPI label="Dead" value={$(ss.dead_cost || 0)} sub={pc(ss.dead_pct || 0)} color="#ef4444" />
        <KPI label="Products" value={d.length} sub={selling.length + " sell / " + dead.length + " dead"} color="#3b82f6" />
        <KPI label="Discounts" value={$(ss.disc || 0)} color="#f59e0b" />
      </div>
      <Table rows={d} cols={[
        { l: "Product", g: (r) => r.p, k: "p" }, { l: "Brand", g: (r) => r.b, k: "b" }, { l: "Cat", g: (r) => r.cat, k: "cat" },
        { l: "Cls", g: (r) => r.cls, k: "cls" }, { l: "Vel/Wk", g: (r) => r.wv.toFixed(1), nm: 1, k: "wv" },
        { l: "Trend", g: (r) => Trend(r), nm: 1, k: "tr" },
        { l: "On Hand", g: (r) => N(r.oh), nm: 1, k: "oh" },
        { l: "WOS", g: (r) => r.wos ? r.wos.toFixed(1) : "—", nm: 1, k: "wos", c: (r) => !r.wos ? {} : r.wos < 1 ? { color: "#ef4444" } : r.wos > 8 ? { color: "#f97316" } : {} },
        { l: "Net Rev", g: (r) => r.nr > 0 ? $(r.nr) : "—", nm: 1, k: "nr", c: (r) => r.nr > 0 ? { color: "#22c55e" } : {} },
        { l: "Profit", g: (r) => r.profit > 0 ? $(r.profit) : "—", nm: 1, k: "profit", c: (r) => r.profit > 0 ? { color: "#4ade80" } : {} },
        { l: "Margin", g: (r) => r.mgn > 0 ? pc(r.mgn) : "—", nm: 1, k: "mgn" },
        { l: "Inv Cost", g: (r) => $(r.ic), nm: 1, k: "ic" },
      ]} />
    </>);
  };

  // ── DEEP DIVE: BRANDS (reuse existing brand deep dive) ──
  const renderBrandDive = () => {
    const brandList = [...new Set(products.filter((p) => CANNABIS_CATS.includes(p.cat)).map((p) => p.b).filter(Boolean))].sort();
    const bv = brandView || brandList[0] || "";
    const bdStore = filters._bdStore || "All";
    const bpAll = products.filter((p) => p.b === bv && CANNABIS_CATS.includes(p.cat));
    const bp = bdStore === "All" ? bpAll : bpAll.filter((p) => p.s === bdStore);
    const brandStoreList = ["All", ...[...new Set(bpAll.map((p) => p.s))].sort()];

    const bRev = bp.reduce((a, p) => a + p.nr, 0);
    const bCogs = bp.reduce((a, p) => a + p.cogs, 0);
    const bProfit = bp.reduce((a, p) => a + p.profit, 0);
    const bInvCost = bp.reduce((a, p) => a + p.ic, 0);
    const bInvUnits = bp.reduce((a, p) => a + p.oh, 0);
    const bMargin = bRev > 0 ? (bRev - bCogs) / bRev * 100 : 0;
    const bVel = bp.reduce((a, p) => a + p.wv, 0);
    const selling = bp.filter((p) => p.wv > 0);
    const dead = bp.filter((p) => p.wv === 0);
    const overstock = bp.filter((p) => p.wos && p.wos > 8 && p.wv > 0);
    const stockout = bp.filter((p) => p.wos != null && p.wos < 2 && p.wv >= 1);
    const needsOrder = bp.filter((p) => p.oq > 0);
    const orderVal = needsOrder.reduce((a, p) => a + p.oq * p.uc, 0);
    const deadCost = dead.reduce((a, p) => a + p.ic, 0);
    const overCost = overstock.reduce((a, p) => a + p.ic, 0);

    const bW1 = bp.reduce((a, p) => a + (p.w1 || 0), 0);
    const bW2 = bp.reduce((a, p) => a + (p.w2 || 0), 0);
    const bW3 = bp.reduce((a, p) => a + (p.w3 || 0), 0);
    const bW4 = bp.reduce((a, p) => a + (p.w4 || 0), 0);
    const bPriorAvg = (bW2 + bW3 + bW4) / 3;
    const bTrendPct = bPriorAvg > 0 ? ((bW1 - bPriorAvg) / bPriorAvg * 100) : bW1 > 0 ? 100 : 0;

    const allBrandRev = {};
    products.forEach((p) => { if (p.b) allBrandRev[p.b] = (allBrandRev[p.b] || 0) + p.nr; });
    const brandRanked = Object.entries(allBrandRev).sort((a, b) => b[1] - a[1]);
    const brandRank = brandRanked.findIndex(([b]) => b === bv) + 1;

    const avgWos = bVel > 0 ? bInvUnits / bVel : null;

    // Store breakdown
    const storeBreak = {};
    bpAll.forEach((p) => {
      if (!storeBreak[p.s]) storeBreak[p.s] = { rev: 0, vel: 0, inv: 0, units: 0, count: 0, w1: 0, w2: 0, w3: 0, w4: 0 };
      const d = storeBreak[p.s];
      d.rev += p.nr; d.vel += p.wv; d.inv += p.ic; d.units += p.oh; d.count++;
      d.w1 += (p.w1 || 0); d.w2 += (p.w2 || 0); d.w3 += (p.w3 || 0); d.w4 += (p.w4 || 0);
    });

    // Category breakdown
    const catBreak = {};
    bp.forEach((p) => {
      if (!catBreak[p.cat]) catBreak[p.cat] = { rev: 0, vel: 0, inv: 0, count: 0 };
      catBreak[p.cat].rev += p.nr; catBreak[p.cat].vel += p.wv; catBreak[p.cat].inv += p.ic; catBreak[p.cat].count++;
    });

    const bar = { background: "#1a1a1a", borderRadius: 4, overflow: "hidden", height: 6, marginTop: 3 };
    const barFill = (pct, color) => ({ width: Math.max(pct, 2) + "%", height: "100%", background: color, borderRadius: 4 });
    const miniLabel = { fontSize: 8, color: "#555", fontFamily: "'JetBrains Mono', monospace", textTransform: "uppercase", letterSpacing: 0.5 };

    // Velocity spark
    const weeks = [bW4, bW3, bW2, bW1];
    const maxW = Math.max(...weeks, 1);
    const sparkW = 120, sparkH = 32;

    return (<>
      <div style={{ marginBottom: 16, display: "flex", alignItems: "center", gap: 12 }}>
        <div><label style={lbl}>Brand</label>
          <select style={{ ...sel, fontSize: 12, padding: "7px 12px" }} value={bv} onChange={(e) => { setBrandView(e.target.value); setFilters((f) => ({ ...f, _bdStore: "All" })); }}>
            {brandList.map((b) => <option key={b}>{b}</option>)}
          </select>
        </div>
        <div><label style={lbl}>Store</label>
          <select style={{ ...sel, fontSize: 12, padding: "7px 12px" }} value={bdStore} onChange={(e) => setFilters((f) => ({ ...f, _bdStore: e.target.value }))}>
            {brandStoreList.map((s) => <option key={s}>{s}</option>)}
          </select>
        </div>
        <div style={{ marginLeft: "auto", fontFamily: "'JetBrains Mono', monospace" }}>
          {bdStore !== "All" && <span style={{ fontSize: 9, color: "#f59e0b", marginRight: 8 }}>Viewing: {bdStore}</span>}
          <span style={{ fontSize: 9, color: "#555" }}>RANK </span>
          <span style={{ fontSize: 18, fontWeight: 800, color: brandRank <= 3 ? "#22c55e" : brandRank <= 10 ? "#3b82f6" : brandRank <= 25 ? "#f59e0b" : "#666" }}>#{brandRank}</span>
          <span style={{ fontSize: 9, color: "#555" }}> / {brandRanked.length}</span>
        </div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))", gap: 10, marginBottom: 16 }}>
        <KPI label="Net Revenue" value={$(bRev)} color="#22c55e" />
        <KPI label="Profit" value={$(bProfit)} color="#4ade80" />
        <KPI label="Margin" value={pc(bMargin)} color={bMargin >= 52 ? "#22c55e" : bMargin >= 40 ? "#f59e0b" : "#ef4444"} />
        <KPI label="Velocity" value={bVel.toFixed(1) + "/wk"} color="#3b82f6" />
        <KPI label="WoW Trend" value={(bTrendPct >= 0 ? "▲ " : "▼ ") + Math.abs(Math.round(bTrendPct)) + "%"} color={bTrendPct >= 0 ? "#22c55e" : "#ef4444"} />
        <KPI label="Avg WOS" value={avgWos ? avgWos.toFixed(1) + "w" : "—"} color={!avgWos ? "#666" : avgWos < 2 ? "#ef4444" : avgWos > 6 ? "#f97316" : "#22c55e"} />
        <KPI label="Inventory" value={$(bInvCost)} sub={N(bInvUnits) + " units"} />
        <KPI label="Risk" value={$(deadCost + overCost)} sub={dead.length + " dead · " + overstock.length + " over"} color="#f97316" />
      </div>

      {/* Velocity Spark + Health */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 10, marginBottom: 16 }}>
        <div style={{ background: "#111", border: "1px solid #222", borderRadius: 6, padding: "10px 14px" }}>
          <div style={{ ...miniLabel, marginBottom: 8 }}>Weekly Units</div>
          <svg width={sparkW} height={sparkH} viewBox={`0 0 ${sparkW} ${sparkH}`}>
            {weeks.map((w, i) => {
              const bw = sparkW / 4 - 4;
              const h = (w / maxW) * sparkH;
              return <rect key={i} x={i * (sparkW / 4) + 2} y={sparkH - h} width={bw} height={h} rx={2}
                fill={i === 3 ? (bTrendPct >= 0 ? "#22c55e" : "#ef4444") : "#333"} />;
            })}
          </svg>
          <div style={{ display: "flex", justifyContent: "space-between", fontSize: 8, color: "#555", fontFamily: "'JetBrains Mono', monospace", marginTop: 4 }}>
            <span>4w ago</span><span>3w</span><span>2w</span><span>Now</span>
          </div>
        </div>
        <div style={{ background: "#111", border: "1px solid #222", borderRadius: 6, padding: "10px 14px" }}>
          <div style={{ ...miniLabel, marginBottom: 8 }}>Portfolio</div>
          <div style={{ display: "flex", gap: 4, marginBottom: 6 }}>
            <div style={{ flex: selling.length, height: 6, background: "#22c55e", borderRadius: 3 }} />
            <div style={{ flex: dead.length || 0.01, height: 6, background: "#ef4444", borderRadius: 3 }} />
          </div>
          <div style={{ fontSize: 10, color: "#888", fontFamily: "'JetBrains Mono', monospace" }}>
            <span style={{ color: "#22c55e" }}>{selling.length} selling</span> · <span style={{ color: "#ef4444" }}>{dead.length} dead</span>
          </div>
        </div>
        <div style={{ background: "#111", border: "1px solid #222", borderRadius: 6, padding: "10px 14px" }}>
          <div style={{ ...miniLabel, marginBottom: 8 }}>Actions</div>
          <div style={{ fontSize: 10, color: "#888", fontFamily: "'JetBrains Mono', monospace" }}>
            <div><span style={{ color: "#ef4444" }}>{stockout.length}</span> stockout risk</div>
            <div><span style={{ color: "#f97316" }}>{needsOrder.length}</span> need reorder ({$(orderVal)})</div>
          </div>
          {needsOrder.length > 0 && (
            <button onClick={() => {
              setTab(4); setPoStore(bdStore !== "All" ? bdStore : ""); setPoBrand(bv); setPoEdits({});
            }} style={{
              marginTop: 8, width: "100%", padding: "5px 10px", borderRadius: 4, fontSize: 10,
              fontFamily: "'JetBrains Mono', monospace", cursor: "pointer",
              background: "#f9731622", color: "#f97316", border: "1px solid #f97316",
            }}>⬡ Generate PO</button>
          )}
        </div>
      </div>

      {/* Store Performance */}
      <div style={{ ...miniLabel, marginBottom: 6, fontSize: 9, color: "#666" }}>Store Performance</div>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))", gap: 8, marginBottom: 16 }}>
        {Object.entries(storeBreak).sort((a, b) => b[1].rev - a[1].rev).map(([s, d]) => {
          const pa = (d.w2 + d.w3 + d.w4) / 3;
          const tr = pa > 0 ? ((d.w1 - pa) / pa * 100) : 0;
          return (
            <div key={s} style={{ background: "#111", border: "1px solid #222", borderRadius: 6, padding: "8px 12px" }}>
              <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}>
                <span style={{ fontSize: 11, fontWeight: 700 }}>{s}</span>
                <span style={{ fontSize: 10, color: tr >= 0 ? "#22c55e" : "#ef4444", fontWeight: 600, fontFamily: "'JetBrains Mono', monospace" }}>
                  {tr !== 0 ? `${tr > 0 ? "▲" : "▼"} ${Math.abs(Math.round(tr))}%` : "—"}
                </span>
              </div>
              <div style={{ fontSize: 10, fontFamily: "'JetBrains Mono', monospace", color: "#888" }}>
                <span style={{ color: "#22c55e" }}>{$(d.rev)}</span> · {d.vel.toFixed(1)}/wk · {$(d.inv)} inv · {d.count} SKUs
              </div>
            </div>
          );
        })}
      </div>

      {/* Category Breakdown */}
      {Object.keys(catBreak).length > 1 && <>
        <div style={{ ...miniLabel, marginBottom: 6, fontSize: 9, color: "#666" }}>Category Breakdown</div>
        <div style={{ background: "#111", border: "1px solid #222", borderRadius: 6, padding: 12, marginBottom: 16 }}>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead><tr>
              {["Category", "Revenue", "Vel/Wk", "SKUs", "Inventory", "Rev Share"].map((h, i) => (
                <th key={h} style={{ ...th, cursor: "default", textAlign: i === 0 ? "left" : "center" }}>{h}</th>
              ))}
            </tr></thead>
            <tbody>
              {Object.entries(catBreak).sort((a, b) => b[1].rev - a[1].rev).map(([cat, cd]) => (
                <tr key={cat}>
                  <td style={{ ...td, color: "#ccc" }}>{cat}</td>
                  <td style={{ ...td, textAlign: "center", color: "#22c55e" }}>{$(cd.rev)}</td>
                  <td style={{ ...td, textAlign: "center", color: "#3b82f6" }}>{cd.vel.toFixed(1)}</td>
                  <td style={{ ...td, textAlign: "center", color: "#666" }}>{cd.count}</td>
                  <td style={{ ...td, textAlign: "center" }}>{$(cd.inv)}</td>
                  <td style={{ ...td, textAlign: "center" }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 4, justifyContent: "center" }}>
                      <span>{bRev > 0 ? pc(cd.rev / bRev * 100) : "—"}</span>
                      <div style={{ ...bar, width: 40, display: "inline-block" }}><div style={barFill(bRev > 0 ? cd.rev / bRev * 100 : 0, "#f59e0b")} /></div>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </>}

      {/* Product Table */}
      <div style={{ ...miniLabel, marginBottom: 6, fontSize: 9, color: "#666" }}>All {bv} Products ({bp.length})</div>
      <Table rows={[...bp].sort((a, b) => b.nr - a.nr)} cols={[
        { l: "Store", g: (r) => r.s, k: "s" }, { l: "Product", g: (r) => r.p, k: "p" },
        { l: "Cat", g: (r) => r.cat, k: "cat" },
        { l: "Vel/Wk", g: (r) => r.wv.toFixed(1), nm: 1, k: "wv" },
        { l: "Trend", g: (r) => Trend(r), nm: 1, k: "tr" },
        { l: "On Hand", g: (r) => N(r.oh), nm: 1, k: "oh" },
        { l: "WOS", g: (r) => r.wos ? r.wos.toFixed(1) : "—", nm: 1, k: "wos", c: (r) => !r.wos ? {} : r.wos < 1 ? { color: "#ef4444" } : r.wos > 8 ? { color: "#f97316" } : {} },
        { l: "Net Rev", g: (r) => r.nr > 0 ? $(r.nr) : "—", nm: 1, k: "nr", c: (r) => r.nr > 0 ? { color: "#22c55e" } : {} },
        { l: "Profit", g: (r) => r.profit > 0 ? $(r.profit) : "—", nm: 1, k: "profit", c: (r) => r.profit > 0 ? { color: "#4ade80" } : {} },
        { l: "Margin", g: (r) => r.mgn > 0 ? pc(r.mgn) : "—", nm: 1, k: "mgn" },
        { l: "Inv Cost", g: (r) => $(r.ic), nm: 1, k: "ic" },
      ]} />
    </>);
  };

  // ── RENDER ──
  return (
    <div style={{ background: "#0a0a0a", color: "#e5e5e5", fontFamily: "'Space Grotesk', sans-serif", minHeight: "100vh" }}>
      {/* Header */}
      <div style={{ padding: "16px 24px 10px", borderBottom: "1px solid #222", display: "flex", alignItems: "center", gap: 12 }}>
        <h1 style={{ fontSize: 22, fontWeight: 700, color: "#22c55e", fontFamily: "'JetBrains Mono', monospace", letterSpacing: 2, margin: 0 }}>TAPS</h1>
        <span style={{ background: "#22c55e22", color: "#22c55e", padding: "2px 8px", borderRadius: 3, fontSize: 10, fontFamily: "'JetBrains Mono', monospace", animation: "pulse 2s infinite" }}>● LIVE</span>
        {S.period && <span style={{ fontSize: 11, color: "#888", fontFamily: "'JetBrains Mono', monospace", border: "1px solid #333", padding: "4px 10px", borderRadius: 4, background: "#111" }}>
          📅 {S.period}
        </span>}
        <div style={{ marginLeft: "auto", display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap", justifyContent: "flex-end" }}>
          <button onClick={refreshInventory} disabled={refreshing} style={{ ...btnStyle, whiteSpace: "nowrap" }}>
            {refreshing ? "Refreshing..." : "↻ Inventory"}
          </button>
          <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-end", gap: 2 }}>
            {S.inventory_ts && <span style={{ fontSize: 9, color: "#555", fontFamily: "'JetBrains Mono', monospace" }}>
              inv {new Date(S.inventory_ts).toLocaleString([], {month:'short', day:'numeric', hour:'numeric', minute:'2-digit'})}
            </span>}
            {S.sales_ts && <span style={{ fontSize: 9, color: "#555", fontFamily: "'JetBrains Mono', monospace" }}>
              sales {new Date(S.sales_ts).toLocaleString([], {month:'short', day:'numeric', hour:'numeric', minute:'2-digit'})}
            </span>}
          </div>
        </div>
      </div>

      {/* Tabs */}
      <div style={{ display: "flex", padding: "0 24px", borderBottom: "1px solid #222", background: "#0d0d0d", overflowX: "auto" }}>
        {TABS.map((t, i) => (
          <div key={i} onClick={() => switchTab(i)} style={{
            padding: "10px 16px", fontSize: 11, fontFamily: "'JetBrains Mono', monospace",
            color: i === tab ? "#22c55e" : "#666", cursor: "pointer",
            borderBottom: `2px solid ${i === tab ? "#22c55e" : "transparent"}`,
            whiteSpace: "nowrap", transition: "all .15s",
          }}>
            {t.i} {t.n}
          </div>
        ))}
      </div>

      {/* Content */}
      <div style={{ padding: "20px 24px", maxHeight: "calc(100vh - 110px)", overflowY: "auto" }}>
        {tab === 0 && renderCC()}

        {/* Inventory Health — consolidated */}
        {tab === 1 && (() => {
          return (<>
            <div style={{ display: "flex", gap: 8, marginBottom: 16 }}>
              <SubTab label="⚠ Stockouts" active={healthView === "stockouts"} onClick={() => { setHealthView("stockouts"); setSortStack([]); }} color="#ef4444" />
              <SubTab label="▲ Overstock" active={healthView === "overstock"} onClick={() => { setHealthView("overstock"); setSortStack([]); }} color="#f97316" />
              <SubTab label="✕ Dead Weight" active={healthView === "dead"} onClick={() => { setHealthView("dead"); setSortStack([]); }} color="#ef4444" />
            </div>

            {healthView === "stockouts" && (() => {
              let d = getFiltered((p) => CANNABIS_CATS.includes(p.cat) && p.wos != null && p.wos < 2 && p.wv >= 1);
              d.sort((a, b) => (a.wos || 0) - (b.wos || 0));
              return (<>{filterBar()}<Summary items={[{ label: "At Risk", value: d.length + " products", color: "#ef4444" }]} />
              <Table rows={d} cols={[
                { l: "Store", g: (r) => r.s, k: "s" }, { l: "Product", g: (r) => r.p, k: "p" }, { l: "Cat", g: (r) => r.cat, k: "cat" },
                { l: "Cls", g: (r) => r.cls, k: "cls" }, { l: "Vel/Wk", g: (r) => r.wv.toFixed(1), nm: 1, k: "wv" },
                { l: "On Hand", g: (r) => r.oh, nm: 1, k: "oh", c: (r) => r.oh <= 2 ? { color: "#ef4444", fontWeight: 700 } : {} },
                { l: "WOS", g: (r) => r.wos?.toFixed(1) || "—", nm: 1, k: "wos", c: () => ({ color: "#ef4444", fontWeight: 700 }) },
                { l: "Par", g: (r) => r.par, nm: 1, k: "par" },
                { l: "Order Qty", g: (r) => r.oq, nm: 1, k: "oq", c: () => ({ color: "#f97316", fontWeight: 700 }) },
                { l: "Net Rev", g: (r) => $(r.nr), nm: 1, k: "nr" },
              ]} /></>);
            })()}

            {healthView === "overstock" && (() => {
              let d = getFiltered((p) => CANNABIS_CATS.includes(p.cat) && p.wos && p.wos > 8 && p.wv > 0);
              if (!sortStack.length) d.sort((a, b) => b.ic - a.ic);
              const tc = d.reduce((a, p) => a + p.ic, 0), eu = d.reduce((a, p) => a + Math.max(p.oh - p.par, 0), 0);
              return (<>{filterBar()}<Summary items={[
                { label: "Overstocked", value: d.length, color: "#f97316" }, { label: "Inv Cost", value: $(tc), color: "#f97316" },
                { label: "Excess Units", value: N(eu) },
              ]} /><Table rows={d} cols={[
                { l: "Store", g: (r) => r.s, k: "s" }, { l: "Product", g: (r) => r.p, k: "p" }, { l: "Cat", g: (r) => r.cat, k: "cat" },
                { l: "Vel/Wk", g: (r) => r.wv.toFixed(1), nm: 1, k: "wv" }, { l: "On Hand", g: (r) => N(r.oh), nm: 1, k: "oh" },
                { l: "WOS", g: (r) => r.wos?.toFixed(1) || "—", nm: 1, k: "wos", c: () => ({ color: "#f97316", fontWeight: 700 }) },
                { l: "Par", g: (r) => r.par, nm: 1, k: "par" },
                { l: "Excess", g: (r) => N(Math.max(r.oh - r.par, 0)), nm: 1, k: "oh", c: () => ({ color: "#f97316" }) },
                { l: "Inv Cost", g: (r) => $(r.ic), nm: 1, k: "ic" },
              ]} /></>);
            })()}

            {healthView === "dead" && (() => {
              let d = getFiltered((p) => p.wv === 0 && CANNABIS_CATS.includes(p.cat));
              d.sort((a, b) => b.ic - a.ic);
              const tc = d.reduce((a, p) => a + p.ic, 0);
              return (<>{filterBar()}<Summary items={[
                { label: "Dead Products", value: d.length, color: "#ef4444" },
                { label: "Trapped Capital", value: $(tc), color: "#ef4444" },
                { label: "Action", value: "liquidate · transfer · deep discount" },
              ]} /><Table rows={d} cols={[
                { l: "Store", g: (r) => r.s, k: "s" }, { l: "Product", g: (r) => r.p, k: "p" },
                { l: "Brand", g: (r) => r.b, k: "b" }, { l: "Cat", g: (r) => r.cat, k: "cat" },
                { l: "Qty", g: (r) => N(r.oh), nm: 1, k: "oh" },
                { l: "Inv Cost", g: (r) => $(r.ic), nm: 1, k: "ic", c: () => ({ color: "#ef4444", fontWeight: 600 }) },
                { l: "Unit Cost", g: (r) => "$" + r.uc.toFixed(2), nm: 1, k: "uc" },
                { l: "Supplier", g: (r) => r.sup, k: "sup" },
              ]} /></>);
            })()}
          </>);
        })()}

        {/* Deep Dive */}
        {tab === 2 && (<>
          <div style={{ display: "flex", gap: 8, marginBottom: 16 }}>
            <SubTab label="◫ Stores" active={diveView === "stores"} onClick={() => { setDiveView("stores"); setSortStack([]); }} />
            <SubTab label="◉ Categories" active={diveView === "categories"} onClick={() => { setDiveView("categories"); setSortStack([]); }} />
            <SubTab label="◈ Brands" active={diveView === "brands"} onClick={() => { setDiveView("brands"); setSortStack([]); }} />
          </div>
          {diveView === "stores" && renderStoreDive()}
          {diveView === "categories" && renderCategoryDive()}
          {diveView === "brands" && renderBrandDive()}
        </>)}

        {/* Power Rankings */}
        {tab === 3 && renderPowerRankings()}

        {/* Purchase Orders */}
        {tab === 4 && (() => {
          // PO-specific WOS (falls back to global)
          const effectiveWos = poWos !== null ? poWos : wos;

          // Holiday awareness — hardcoded calendar
          const BUSY_WINDOWS = [
            { label: "4/20 Week", start: [4, 14], end: [4, 21], wos: 4.5 },
            { label: "Memorial Day", start: [5, 20], end: [5, 27], wos: 3.5 },
            { label: "Independence Day", start: [6, 28], end: [7, 5], wos: 3.5 },
            { label: "Labor Day", start: [8, 28], end: [9, 4], wos: 3.5 },
            { label: "Halloween Week", start: [10, 25], end: [11, 1], wos: 3.5 },
            { label: "Thanksgiving", start: [11, 20], end: [11, 28], wos: 4.0 },
            { label: "Holiday / NYE", start: [12, 15], end: [1, 3], wos: 4.5 },
          ];

          const now = new Date();
          const m = now.getMonth() + 1, d = now.getDate();
          const lookAhead = new Date(now.getTime() + 14 * 86400000);
          const la_m = lookAhead.getMonth() + 1, la_d = lookAhead.getDate();

          const inWindow = (month, day, s, e) => {
            const v = month * 100 + day;
            const sv = s[0] * 100 + s[1];
            const ev = e[0] * 100 + e[1];
            if (sv <= ev) return v >= sv && v <= ev;
            return v >= sv || v <= ev; // wraps around year (NYE)
          };

          const activeHoliday = BUSY_WINDOWS.find((w) =>
            inWindow(m, d, w.start, w.end) || inWindow(la_m, la_d, w.start, w.end)
          );

          // Recalculate par & order qty with PO-specific WOS — cannabis only
          const poProducts = cp.map((p) => {
            const par = Math.max(Math.round(p.wv * effectiveWos), 0);
            return { ...p, par, oq: Math.max(par - p.oh, 0) };
          });

          // All products that need ordering
          let need = poProducts.filter((p) => p.oq > 0).map((p) => ({ ...p, sup: p.sup || "Unknown Supplier", b: p.b || "Unknown Brand" }));
          const allStores = [...new Set(need.map((p) => p.s))].sort();
          const activeStore = poStore || (allStores.length > 0 ? allStores[0] : "");

          // Filter to selected store
          const storeNeed = activeStore ? need.filter((p) => p.s === activeStore) : need;

          // Group by brand
          const brandGroups = {};
          storeNeed.forEach((p) => {
            if (!brandGroups[p.b]) brandGroups[p.b] = { items: [], total: 0, units: 0, suppliers: new Set() };
            brandGroups[p.b].items.push(p);
            brandGroups[p.b].total += p.oq * p.uc;
            brandGroups[p.b].units += p.oq;
            brandGroups[p.b].suppliers.add(p.sup);
          });
          const brandList = Object.entries(brandGroups)
            .map(([name, g]) => ({ name, ...g, sup: [...g.suppliers].filter((s) => s !== "Unknown Supplier").join(", ") || "Unknown Supplier" }))
            .sort((a, b) => b.total - a.total);

          const totalVal = storeNeed.reduce((a, p) => a + p.oq * p.uc, 0);
          const totalUnits = storeNeed.reduce((a, p) => a + p.oq, 0);

          // Selected brand's items
          const brandItems = poBrand ? (brandGroups[poBrand]?.items || []).sort((a, b) => (b.oq * b.uc) - (a.oq * a.uc)) : [];
          const brandSup = poBrand ? (brandGroups[poBrand]?.suppliers ? [...brandGroups[poBrand].suppliers].filter((s) => s !== "Unknown Supplier").join(", ") : "") : "";

          // Get effective qty (with edits)
          const getQty = (p) => {
            const key = `${p.s}|${p.p}`;
            return poEdits[key] !== undefined ? poEdits[key] : p.oq;
          };

          // Export PO
          const exportPO = async () => {
            if (!activeStore || !poBrand || brandItems.length === 0) return;
            setPoExporting(true);
            try {
              const items = brandItems.map((p) => ({
                description: p.p,
                qty: getQty(p),
                unit_price: p.uc,
              })).filter((i) => i.qty > 0);
              const res = await fetch(`${API_BASE}/api/generate-po`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ store: activeStore, supplier: brandSup || poBrand, brand: poBrand, items }),
              });
              if (!res.ok) throw new Error("Export failed");
              const blob = await res.blob();
              const url = URL.createObjectURL(blob);
              const a = document.createElement("a");
              a.href = url;
              a.download = res.headers.get("content-disposition")?.match(/filename="(.+)"/)?.[1] || `PO_${activeStore}_${poBrand}.xlsx`;
              a.click();
              URL.revokeObjectURL(url);
            } catch (e) {
              alert("PO export failed: " + e.message);
            } finally {
              setPoExporting(false);
            }
          };

          return (<>
            {/* Holiday Alert */}
            {activeHoliday && poWos === null && (
              <div style={{
                background: "#f9731611", border: "1px solid #f9731644", borderRadius: 6,
                padding: "10px 14px", marginBottom: 12, display: "flex", alignItems: "center",
                justifyContent: "space-between", fontFamily: "'JetBrains Mono', monospace",
              }}>
                <div>
                  <span style={{ fontSize: 11, color: "#f97316", fontWeight: 700 }}>🔥 {activeHoliday.label}</span>
                  <span style={{ fontSize: 10, color: "#888", marginLeft: 8 }}>
                    approaching — consider stocking up to {activeHoliday.wos} WOS
                  </span>
                </div>
                <button onClick={() => { setPoWos(activeHoliday.wos); setPoEdits({}); }}
                  style={{
                    background: "#f9731622", color: "#f97316", border: "1px solid #f97316",
                    padding: "5px 12px", borderRadius: 4, cursor: "pointer",
                    fontFamily: "'JetBrains Mono', monospace", fontSize: 10, fontWeight: 700,
                  }}>Apply {activeHoliday.wos} WOS</button>
              </div>
            )}

            {/* KPIs */}
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(190px, 1fr))", gap: 10, marginBottom: 16 }}>
              <KPI label="Total PO Value" value={$(totalVal)} sub={N(totalUnits) + " units"} color="#f97316" />
              <KPI label="Brands" value={brandList.length} sub={activeStore || "All Stores"} color="#3b82f6" />
              <KPI label="Line Items" value={storeNeed.length} color="#8b5cf6" />
              <div style={{ background: "#111", border: "1px solid #222", borderRadius: 6, padding: "12px 14px" }}>
                <div style={{ fontSize: 9, color: "#666", fontFamily: "'JetBrains Mono', monospace", textTransform: "uppercase", letterSpacing: 1 }}>WOS TARGET</div>
                <div style={{ display: "flex", gap: 4, marginTop: 6 }}>
                  {[
                    { v: null, l: `${wos} std` },
                    { v: 3.0, l: "3.0" },
                    { v: 3.5, l: "3.5" },
                    { v: 4.0, l: "4.0" },
                    { v: 4.5, l: "4.5" },
                    { v: 5.0, l: "5.0" },
                  ].map((opt) => {
                    const active = opt.v === poWos;
                    return (
                      <button key={opt.l} onClick={() => { setPoWos(opt.v); setPoEdits({}); }}
                        style={{
                          padding: "4px 8px", borderRadius: 3, fontSize: 10,
                          fontFamily: "'JetBrains Mono', monospace", fontWeight: active ? 700 : 400,
                          cursor: "pointer", transition: "all .15s",
                          background: active ? "#f9731622" : "transparent",
                          color: active ? "#f97316" : "#666",
                          border: `1px solid ${active ? "#f97316" : "#333"}`,
                        }}>{opt.l}</button>
                    );
                  })}
                </div>
                <div style={{ fontSize: 20, fontWeight: 700, fontFamily: "'JetBrains Mono', monospace", marginTop: 4, color: poWos !== null ? "#f97316" : "#e5e5e5" }}>
                  {effectiveWos}{poWos !== null && <span style={{ fontSize: 9, color: "#888", marginLeft: 6 }}>override</span>}
                </div>
              </div>
            </div>

            {/* Store Selector */}
            <div style={{ display: "flex", gap: 8, marginBottom: 16, alignItems: "center", flexWrap: "wrap" }}>
              <label style={{ ...lbl, marginBottom: 0, marginRight: 4 }}>STORE</label>
              {allStores.map((s) => (
                <button key={s} onClick={() => { setPoStore(s); setPoBrand(""); setPoEdits({}); }}
                  style={{
                    padding: "6px 14px", borderRadius: 4, fontSize: 11, fontFamily: "'JetBrains Mono', monospace",
                    cursor: "pointer", transition: "all .15s",
                    background: s === activeStore ? "#f9731622" : "#111",
                    color: s === activeStore ? "#f97316" : "#666",
                    border: `1px solid ${s === activeStore ? "#f97316" : "#222"}`,
                  }}>{s}</button>
              ))}
            </div>

            {/* Two-panel layout: Brand list | PO Builder */}
            <div style={{ display: "grid", gridTemplateColumns: poBrand ? "320px 1fr" : "1fr", gap: 16 }}>
              {/* Left: Brand Cards */}
              <div style={{ display: "flex", flexDirection: "column", gap: 6, maxHeight: "calc(100vh - 340px)", overflowY: "auto" }}>
                {brandList.map((b) => (
                  <div key={b.name} onClick={() => { setPoBrand(b.name); setPoEdits({}); }}
                    style={{
                      background: b.name === poBrand ? "#f9731611" : "#111",
                      border: `1px solid ${b.name === poBrand ? "#f97316" : "#222"}`,
                      borderRadius: 6, padding: "10px 12px", cursor: "pointer", transition: "all .15s",
                    }}>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                      <span style={{
                        fontSize: 11, fontFamily: "'JetBrains Mono', monospace", fontWeight: 600, color: "#e5e5e5",
                      }}>{b.name}</span>
                      <span style={{ fontSize: 13, fontFamily: "'JetBrains Mono', monospace", fontWeight: 700, color: "#f97316" }}>
                        {$(b.total)}
                      </span>
                    </div>
                    <div style={{ fontSize: 9, color: "#666", fontFamily: "'JetBrains Mono', monospace", marginTop: 3 }}>
                      {b.items.length} items · {N(b.units)} units{b.sup && b.sup !== "Unknown Supplier" ? ` · ${b.sup}` : ""}
                    </div>
                  </div>
                ))}
                {brandList.length === 0 && (
                  <div style={{ color: "#666", fontSize: 11, fontFamily: "'JetBrains Mono', monospace", padding: 20, textAlign: "center" }}>
                    No items need ordering for {activeStore}
                  </div>
                )}
              </div>

              {/* Right: PO Builder */}
              {poBrand && (
                <div>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
                    <div>
                      <span style={{ fontSize: 14, fontWeight: 700, color: "#e5e5e5", fontFamily: "'JetBrains Mono', monospace" }}>
                        {poBrand}
                      </span>
                      <span style={{ fontSize: 10, color: "#666", marginLeft: 8, fontFamily: "'JetBrains Mono', monospace" }}>
                        → {activeStore}
                      </span>
                      {brandSup && <span style={{ fontSize: 9, color: "#555", marginLeft: 8, fontFamily: "'JetBrains Mono', monospace" }}>
                        ({brandSup})
                      </span>}
                    </div>
                    <div style={{ display: "flex", gap: 8 }}>
                      <span style={{ fontSize: 12, fontWeight: 700, color: "#22c55e", fontFamily: "'JetBrains Mono', monospace", alignSelf: "center" }}>
                        {$(brandItems.reduce((a, p) => a + getQty(p) * p.uc, 0))}
                      </span>
                      <button onClick={exportPO} disabled={poExporting}
                        style={{
                          background: "#f9731622", color: "#f97316", border: "1px solid #f97316",
                          padding: "6px 16px", borderRadius: 4, cursor: "pointer",
                          fontFamily: "'JetBrains Mono', monospace", fontSize: 11, fontWeight: 700,
                        }}>
                        {poExporting ? "Generating..." : "⬡ Export PO"}
                      </button>
                    </div>
                  </div>

                  {/* Line items table */}
                  <div style={{ overflowX: "auto", fontSize: 10, fontFamily: "'JetBrains Mono', monospace" }}>
                    <table style={{ width: "100%", borderCollapse: "collapse" }}>
                      <thead><tr>
                        {["Product", "Cat", "Vel/Wk", "On Hand", "Par", "Order Qty", "Unit $", "Line $"].map((h, i) => (
                          <th key={i} style={{ ...th, textAlign: i >= 2 ? "right" : "left" }}>{h}</th>
                        ))}
                      </tr></thead>
                      <tbody>
                        {brandItems.map((p, ri) => {
                          const key = `${p.s}|${p.p}`;
                          const qty = getQty(p);
                          return (
                            <tr key={ri} style={{ background: ri % 2 === 0 ? "transparent" : "#0d0d0d" }}>
                              <td style={{ ...td, maxWidth: 300 }}>{p.p}</td>
                              <td style={td}>{p.cat}</td>
                              <td style={{ ...td, textAlign: "right" }}>{p.wv.toFixed(1)}</td>
                              <td style={{ ...td, textAlign: "right" }}>{p.oh}</td>
                              <td style={{ ...td, textAlign: "right", color: "#8b5cf6" }}>{p.par}</td>
                              <td style={{ ...td, textAlign: "right" }}>
                                <input type="number" min="0"
                                  value={qty}
                                  onChange={(e) => setPoEdits((prev) => ({ ...prev, [key]: Math.max(0, parseInt(e.target.value) || 0) }))}
                                  style={{
                                    width: 56, background: poEdits[key] !== undefined ? "#f9731622" : "#1a1a1a",
                                    border: `1px solid ${poEdits[key] !== undefined ? "#f97316" : "#333"}`,
                                    color: "#f97316", fontWeight: 700, textAlign: "right",
                                    padding: "3px 6px", borderRadius: 3, fontFamily: "'JetBrains Mono', monospace", fontSize: 10,
                                  }}
                                />
                              </td>
                              <td style={{ ...td, textAlign: "right" }}>{$(p.uc)}</td>
                              <td style={{ ...td, textAlign: "right", color: "#22c55e", fontWeight: 600 }}>
                                {$(qty * p.uc)}
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </div>
          </>);
        })()}
      </div>

      <style>{`
        @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.5} }
        .cA{color:#22c55e;font-weight:700} .cB{color:#3b82f6;font-weight:600} .cC{color:#f59e0b} .cD{color:#666}
      `}</style>
    </div>
  );
}

const lbl = { fontSize: 9, color: "#666", fontFamily: "'JetBrains Mono', monospace", display: "block", marginBottom: 2 };
const sel = { background: "#1a1a1a", border: "1px solid #222", color: "#e5e5e5", padding: "5px 8px", borderRadius: 4, fontFamily: "'JetBrains Mono', monospace", fontSize: 10 };
const th = { position: "sticky", top: 0, background: "#1a1a1a", color: "#22c55e", fontSize: 9, textTransform: "uppercase", letterSpacing: 0.5, padding: "7px 5px", cursor: "pointer", borderBottom: "1px solid #222", whiteSpace: "nowrap", userSelect: "none", zIndex: 1 };
const td = { padding: 5, borderBottom: "1px solid #1a1a1a", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis", maxWidth: 280, fontSize: 10, fontFamily: "'JetBrains Mono', monospace" };
const btnStyle = { background: "#22c55e11", color: "#22c55e", border: "1px solid #22c55e44", padding: "5px 14px", borderRadius: 4, cursor: "pointer", fontFamily: "'JetBrains Mono', monospace", fontSize: 10 };
