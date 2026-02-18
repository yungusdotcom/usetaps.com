import { useState, useEffect, useCallback, useRef } from "react";

const API_BASE = window.location.hostname === "localhost"
  ? "http://localhost:8000"
  : "https://usetapscom-production.up.railway.app"; // Update after deploy

const $ = (n) => n >= 1e6 ? "$" + (n / 1e6).toFixed(2) + "M" : n >= 1e3 ? "$" + (n / 1e3).toFixed(1) + "K" : "$" + n.toFixed(0);
const N = (n) => n >= 1e6 ? (n / 1e6).toFixed(1) + "M" : n >= 1e3 ? (n / 1e3).toFixed(1) + "K" : n.toLocaleString();
const pc = (n) => n.toFixed(1) + "%";

const TABS = [
  { n: "Command Center", i: "◉" }, { n: "Revenue", i: "$" }, { n: "Par Levels", i: "◎" },
  { n: "Stockouts", i: "⚠" }, { n: "Overstock", i: "▲" }, { n: "Dead Weight", i: "✕" },
  { n: "Store Deep Dive", i: "◫" }, { n: "Brand Deep Dive", i: "◈" }, { n: "Purchase Orders", i: "⬡" },
];

const CANNABIS_CATS = ["FLOWER", "Concentrates", "Edibles", "Infused Flower", "Capsules"];

export default function TAPSApp() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [salesPulling, setSalesPulling] = useState(false);
  const [tab, setTab] = useState(0);
  const [filters, setFilters] = useState({ s: "All", c: "All", cl: "All", b: "All", q: "" });
  const [sortStack, setSortStack] = useState([]);
  const [wos, setWos] = useState(2.5);
  const [storeView, setStoreView] = useState("");
  const [brandView, setBrandView] = useState("");
  const [error, setError] = useState(null);
  const pollRef = useRef(null);

  const fetchTaps = useCallback(async (refreshInv = false) => {
    try {
      const url = `${API_BASE}/api/taps?wos=${wos}&refresh_inventory=${refreshInv}`;
      const res = await fetch(url);
      if (!res.ok) throw new Error(`API error: ${res.status}`);
      const d = await res.json();
      setData(d);
      setError(null);
      if (!storeView && d.ss?.length) setStoreView(d.ss[0].s);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, [wos, storeView]);

  useEffect(() => { fetchTaps(); }, []);

  const refreshInventory = async () => {
    setRefreshing(true);
    await fetchTaps(true);
  };

  const refreshSales = async () => {
    setSalesPulling(true);
    try {
      await fetch(`${API_BASE}/api/refresh-sales`, { method: "POST" });
      // Poll for completion
      pollRef.current = setInterval(async () => {
        const res = await fetch(`${API_BASE}/api/sales-status`);
        const s = await res.json();
        if (!s.pulling) {
          clearInterval(pollRef.current);
          setSalesPulling(false);
          fetchTaps(true);
        }
      }, 5000);
    } catch (e) {
      setSalesPulling(false);
    }
  };

  const handleSort = (ci) => {
    setSortStack((prev) => {
      const existing = prev.findIndex((s) => s.ci === ci);
      if (existing !== -1) {
        if (existing === prev.length - 1) {
          // It's the most recent sort column
          if (prev[existing].asc) {
            // First click was asc → flip to desc
            const next = [...prev];
            next[existing] = { ci, asc: false };
            return next;
          } else {
            // Second click was desc → remove it (reset)
            return prev.filter((s) => s.ci !== ci);
          }
        } else {
          // Clicking a non-primary sort → move to end as asc
          return [...prev.filter((s) => s.ci !== ci), { ci, asc: true }];
        }
      }
      // New column → add as asc
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
        <button onClick={() => { setLoading(true); fetchTaps(); }} style={{ marginTop: 16, background: "#22c55e22", color: "#22c55e", border: "1px solid #22c55e", padding: "8px 24px", borderRadius: 4, cursor: "pointer", fontFamily: "inherit" }}>
          Retry
        </button>
      </div>
    </div>
  );

  const S = data.st;
  const SS = data.ss;
  const P = data.pd;

  // Apply WOS changes to products
  const products = P.map((p) => {
    const par = Math.max(Math.round(p.wv * wos), 0);
    return { ...p, par, oq: Math.max(par - p.oh, 0) };
  });

  // Filter
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

  // Multi-sort
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
      <div style={{ maxHeight: "calc(100vh - 230px)", overflow: "auto", border: "1px solid #222", borderRadius: 6 }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontFamily: "'JetBrains Mono', monospace", fontSize: 10 }}>
          <thead>
            <tr>{cols.map((c, i) => {
              const si = sortStack.findIndex((s) => s.ci === i);
              const ar = si !== -1 ? (sortStack[si].asc ? " ▲" : " ▼") : "";
              const badge = si !== -1 && sortStack.length > 1 ? si + 1 : "";
              return (
                <th key={i} onClick={() => handleSort(i)} style={{ ...th, textAlign: c.nm ? "right" : "left" }}>
                  {c.l}<span style={{ color: "#22c55e", fontSize: 8 }}>{ar}</span>
                  {badge && <span style={{ color: "#666", fontSize: 7, marginLeft: 1 }}>{badge}</span>}
                </th>
              );
            })}</tr>
          </thead>
          <tbody>{sorted.slice(0, maxRows).map((r, ri) => (
            <tr key={ri} style={{ background: ri % 2 === 0 ? "transparent" : "#0d0d0d" }}>
              {cols.map((c, ci) => (
                <td key={ci} style={{ ...td, textAlign: c.nm ? "right" : "left", ...(c.c ? c.c(r) : {}) }}>
                  {c.g(r)}
                </td>
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

  // ── TAB RENDERS ──
  const renderCC = () => {
    const dead = products.filter((p) => p.wv === 0);
    const deadC = dead.reduce((a, p) => a + p.ic, 0);
    const over = products.filter((p) => p.wos && p.wos > 8 && p.wv > 0);
    const overC = over.reduce((a, p) => a + p.ic, 0);
    const at = (S.cogs * 365 / 31) / S.total_inv_cost;
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

  // Trend indicator: WoW velocity change
  const Trend = (r) => {
    const t = r.tr || 0;
    if (t === 0 || r.wv === 0) return <span style={{ color: "#444" }}>—</span>;
    const arrow = t > 0 ? "▲" : "▼";
    const color = t >= 20 ? "#22c55e" : t > 0 ? "#4ade80" : t > -20 ? "#f97316" : "#ef4444";
    return <span style={{ color, fontWeight: 600, fontSize: 9 }}>{arrow} {Math.abs(t)}%</span>;
  };

  const revCols = [
    { l: "Store", g: (r) => r.s, k: "s" }, { l: "Product", g: (r) => r.p, k: "p" },
    { l: "Cat", g: (r) => r.cat, k: "cat" },
    { l: "Cls", g: (r) => <span className={`c${r.cls}`}>{r.cls}</span>, k: "cls" },
    { l: "Net Rev", g: (r) => $(r.nr), nm: 1, k: "nr", c: () => ({ color: "#22c55e", fontWeight: 600 }) },
    { l: "Margin%", g: (r) => r.mgn > 0 ? pc(r.mgn) : "—", nm: 1, k: "mgn", c: (r) => ({ color: r.mgn >= 55 ? "#22c55e" : r.mgn >= 45 ? "#f59e0b" : "#ef4444" }) },
    { l: "COGS", g: (r) => $(r.cogs), nm: 1, k: "cogs" },
    { l: "Vel/Wk", g: (r) => r.wv.toFixed(1), nm: 1, k: "wv" },
    { l: "Trend", g: (r) => Trend(r), nm: 1, k: "tr" },
    { l: "On Hand", g: (r) => N(r.oh), nm: 1, k: "oh" },
  ];

  const parCols = [
    { l: "Store", g: (r) => r.s, k: "s" }, { l: "Product", g: (r) => r.p, k: "p" },
    { l: "Cat", g: (r) => r.cat, k: "cat" },
    { l: "Cls", g: (r) => <span className={`c${r.cls}`}>{r.cls}</span>, k: "cls" },
    { l: "Vel/Wk", g: (r) => r.wv.toFixed(1), nm: 1, k: "wv", c: (r) => ({ color: r.wv >= 20 ? "#22c55e" : r.wv >= 10 ? "#3b82f6" : r.wv >= 3 ? "#f59e0b" : "#666" }) },
    { l: "Trend", g: (r) => Trend(r), nm: 1, k: "tr" },
    { l: "On Hand", g: (r) => N(r.oh), nm: 1, k: "oh" },
    { l: "WOS", g: (r) => r.wos ? r.wos.toFixed(1) : "—", nm: 1, k: "wos", c: (r) => !r.wos ? {} : r.wos < 1 ? { color: "#ef4444", fontWeight: 700 } : r.wos > 8 ? { color: "#f97316" } : {} },
    { l: "Par", g: (r) => r.par, nm: 1, k: "par", c: () => ({ color: "#8b5cf6" }) },
    { l: "Order", g: (r) => r.oq > 0 ? r.oq : "—", nm: 1, k: "oq", c: (r) => r.oq > 0 ? { color: "#f97316", fontWeight: 700 } : { color: "#666" } },
    { l: "Inv Cost", g: (r) => $(r.ic), nm: 1, k: "ic" },
  ];

  // ── RENDER ──
  return (
    <div style={{ background: "#0a0a0a", color: "#e5e5e5", fontFamily: "'Space Grotesk', sans-serif", minHeight: "100vh" }}>
      {/* Header */}
      <div style={{ padding: "16px 24px 10px", borderBottom: "1px solid #222", display: "flex", alignItems: "center", gap: 12 }}>
        <h1 style={{ fontSize: 22, fontWeight: 700, color: "#22c55e", fontFamily: "'JetBrains Mono', monospace", letterSpacing: 2, margin: 0 }}>TAPS</h1>
        <span style={{ background: "#22c55e22", color: "#22c55e", padding: "2px 8px", borderRadius: 3, fontSize: 10, fontFamily: "'JetBrains Mono', monospace", animation: "pulse 2s infinite" }}>● LIVE</span>
        <div style={{ marginLeft: "auto", display: "flex", gap: 8, alignItems: "center" }}>
          <button onClick={refreshInventory} disabled={refreshing} style={btnStyle}>
            {refreshing ? "Refreshing..." : "↻ Inventory"}
          </button>
          <button onClick={refreshSales} disabled={salesPulling} style={{ ...btnStyle, borderColor: "#f59e0b", color: "#f59e0b" }}>
            {salesPulling ? "Pulling Sales..." : "↻ Sales (slow)"}
          </button>
          {S.inventory_ts && <span style={{ fontSize: 9, color: "#666", fontFamily: "'JetBrains Mono', monospace" }}>
            Inv: {new Date(S.inventory_ts).toLocaleString([], {month:'short', day:'numeric', hour:'numeric', minute:'2-digit'})}
          </span>}
          {S.sales_ts && <span style={{ fontSize: 9, color: "#666", fontFamily: "'JetBrains Mono', monospace" }}>
            Sales: {new Date(S.sales_ts).toLocaleString([], {month:'short', day:'numeric', hour:'numeric', minute:'2-digit'})}
          </span>}
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

        {tab === 1 && (() => {
          let d = getFiltered((p) => p.nr > 0);
          if (!sortStack.length) d.sort((a, b) => b.nr - a.nr);
          const tr = d.reduce((a, p) => a + p.nr, 0), tc = d.reduce((a, p) => a + p.cogs, 0);
          return (<>{filterBar()}<Summary items={[
            { label: "Products", value: d.length }, { label: "Net Revenue", value: $(tr), color: "#22c55e" },
            { label: "COGS", value: $(tc) }, { label: "Margin", value: pc(tr > 0 ? (tr - tc) / tr * 100 : 0), color: "#22c55e" },
          ]} /><Table rows={d} cols={revCols} /></>);
        })()}

        {tab === 2 && (() => {
          let d = getFiltered((p) => p.wv > 0);
          if (!sortStack.length) d.sort((a, b) => b.wv - a.wv);
          return (<>{filterBar(
            <div style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: 6 }}>
              <label style={lbl}>WOS Target</label>
              <select style={{ ...sel, color: "#22c55e" }} value={wos} onChange={(e) => setWos(parseFloat(e.target.value))}>
                {[2, 2.5, 3, 3.5, 4].map((w) => <option key={w} value={w}>{w}</option>)}
              </select>
            </div>
          )}<Summary items={[{ label: "Products", value: d.length }, { label: "Inv", value: $(d.reduce((a, p) => a + p.ic, 0)) }]} />
          <Table rows={d} cols={parCols} /></>);
        })()}

        {tab === 3 && (() => {
          let d = getFiltered((p) => p.wos != null && p.wos < 2 && p.wv >= 1);
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

        {tab === 4 && (() => {
          let d = getFiltered((p) => p.wos && p.wos > 8 && p.wv > 0);
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

        {tab === 5 && (() => {
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

        {tab === 6 && (() => {
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
              { l: "Product", g: (r) => r.p, k: "p" }, { l: "Cat", g: (r) => r.cat, k: "cat" },
              { l: "Cls", g: (r) => r.cls, k: "cls" }, { l: "Vel/Wk", g: (r) => r.wv.toFixed(1), nm: 1, k: "wv" },
              { l: "Trend", g: (r) => Trend(r), nm: 1, k: "tr" },
              { l: "On Hand", g: (r) => N(r.oh), nm: 1, k: "oh" },
              { l: "WOS", g: (r) => r.wos ? r.wos.toFixed(1) : "—", nm: 1, k: "wos", c: (r) => !r.wos ? {} : r.wos < 1 ? { color: "#ef4444" } : r.wos > 8 ? { color: "#f97316" } : {} },
              { l: "Net Rev", g: (r) => r.nr > 0 ? $(r.nr) : "—", nm: 1, k: "nr", c: (r) => r.nr > 0 ? { color: "#22c55e" } : {} },
              { l: "Margin", g: (r) => r.mgn > 0 ? pc(r.mgn) : "—", nm: 1, k: "mgn" },
              { l: "Inv Cost", g: (r) => $(r.ic), nm: 1, k: "ic" },
            ]} />
          </>);
        })()}

        {tab === 7 && (() => {
          // Brand Deep Dive
          const allBrands = [...new Set(products.map((p) => p.b).filter(Boolean))].sort();
          const bv = brandView || allBrands[0] || "";
          const bp = products.filter((p) => p.b === bv);
          const bpSelling = bp.filter((p) => p.wv > 0);
          const bpDead = bp.filter((p) => p.wv === 0);

          // Aggregated metrics
          const totalRev = bp.reduce((a, p) => a + p.nr, 0);
          const totalCogs = bp.reduce((a, p) => a + p.cogs, 0);
          const totalInvCost = bp.reduce((a, p) => a + p.ic, 0);
          const totalUnits = bp.reduce((a, p) => a + p.oh, 0);
          const totalSold = bp.reduce((a, p) => a + (p.wv * 4.43), 0); // ~31 days
          const avgMargin = totalRev > 0 ? (totalRev - totalCogs) / totalRev * 100 : 0;
          const totalVel = bp.reduce((a, p) => a + p.wv, 0);
          const avgWos = totalVel > 0 ? totalUnits / totalVel : null;
          const deadCost = bpDead.reduce((a, p) => a + p.ic, 0);
          const overstock = bp.filter((p) => p.wos && p.wos > 8 && p.wv > 0);
          const overstockCost = overstock.reduce((a, p) => a + p.ic, 0);
          const stockouts = bp.filter((p) => p.wos != null && p.wos < 2 && p.wv >= 1);
          const poItems = bp.filter((p) => p.oq > 0);
          const poValue = poItems.reduce((a, p) => a + p.oq * p.uc, 0);

          // Week trend for entire brand
          const bw1 = bp.reduce((a, p) => a + (p.w1 || 0), 0);
          const bw2 = bp.reduce((a, p) => a + (p.w2 || 0), 0);
          const bw3 = bp.reduce((a, p) => a + (p.w3 || 0), 0);
          const bw4 = bp.reduce((a, p) => a + (p.w4 || 0), 0);
          const priorAvg = (bw2 + bw3 + bw4) / 3;
          const brandTrend = priorAvg > 0 ? ((bw1 - priorAvg) / priorAvg * 100) : (bw1 > 0 ? 100 : 0);

          // Store breakdown for this brand
          const brandStores = [...new Set(bp.map((p) => p.s))].sort();
          const storeBreakdown = brandStores.map((s) => {
            const sp = bp.filter((p) => p.s === s);
            const rev = sp.reduce((a, p) => a + p.nr, 0);
            const inv = sp.reduce((a, p) => a + p.ic, 0);
            const vel = sp.reduce((a, p) => a + p.wv, 0);
            const units = sp.reduce((a, p) => a + p.oh, 0);
            const sw1 = sp.reduce((a, p) => a + (p.w1 || 0), 0);
            const sw2 = sp.reduce((a, p) => a + (p.w2 || 0), 0);
            const sw3 = sp.reduce((a, p) => a + (p.w3 || 0), 0);
            const sw4 = sp.reduce((a, p) => a + (p.w4 || 0), 0);
            const pa = (sw2 + sw3 + sw4) / 3;
            const tr = pa > 0 ? ((sw1 - pa) / pa * 100) : (sw1 > 0 ? 100 : 0);
            return { s, rev, inv, vel: Math.round(vel * 10) / 10, units, skus: sp.length, dead: sp.filter((p) => p.wv === 0).length, tr: Math.round(tr) };
          });

          // Category breakdown
          const brandCats = [...new Set(bp.map((p) => p.cat).filter(Boolean))].sort();
          const catBreakdown = brandCats.map((c) => {
            const cp = bp.filter((p) => p.cat === c);
            return { cat: c, rev: cp.reduce((a, p) => a + p.nr, 0), vel: Math.round(cp.reduce((a, p) => a + p.wv, 0) * 10) / 10, skus: cp.length, inv: cp.reduce((a, p) => a + p.ic, 0) };
          }).sort((a, b) => b.rev - a.rev);

          // Velocity spark: weekly units as mini bar chart
          const maxW = Math.max(bw1, bw2, bw3, bw4, 1);
          const spark = (val, label) => (
            <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 2, flex: 1 }}>
              <div style={{ width: "100%", background: "#1a1a1a", borderRadius: 3, height: 60, display: "flex", alignItems: "flex-end", justifyContent: "center" }}>
                <div style={{ width: "70%", background: val === bw1 ? (brandTrend >= 0 ? "#22c55e" : "#ef4444") : "#333", borderRadius: "3px 3px 0 0", height: `${(val / maxW) * 100}%`, minHeight: 2, transition: "height 0.3s" }} />
              </div>
              <div style={{ fontSize: 11, fontWeight: 700, color: val === bw1 ? "#e5e5e5" : "#888", fontFamily: "'JetBrains Mono', monospace" }}>{val}</div>
              <div style={{ fontSize: 8, color: "#555", fontFamily: "'JetBrains Mono', monospace" }}>{label}</div>
            </div>
          );

          return (<>
            <div style={{ display: "flex", gap: 12, marginBottom: 16, alignItems: "flex-end", flexWrap: "wrap" }}>
              <div><label style={lbl}>Brand</label>
                <select style={{ ...sel, fontSize: 12, padding: "7px 10px" }} value={bv} onChange={(e) => setBrandView(e.target.value)}>
                  {allBrands.map((b) => <option key={b}>{b}</option>)}
                </select>
              </div>
              <div style={{ marginLeft: "auto", fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: "#555" }}>
                {bp.length} products across {brandStores.length} stores
              </div>
            </div>

            {/* Hero Metrics Row */}
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))", gap: 10, marginBottom: 16 }}>
              <KPI label="Net Revenue" value={$(totalRev)} color="#22c55e" />
              <KPI label="Margin" value={pc(avgMargin)} color={avgMargin >= 52 ? "#22c55e" : avgMargin >= 40 ? "#f59e0b" : "#ef4444"} />
              <KPI label="Total Velocity" value={totalVel.toFixed(1) + "/wk"} color="#3b82f6" />
              <KPI label="WoW Trend" value={(brandTrend >= 0 ? "▲ " : "▼ ") + Math.abs(Math.round(brandTrend)) + "%"} color={brandTrend >= 0 ? "#22c55e" : "#ef4444"} />
              <KPI label="Inventory" value={$(totalInvCost)} sub={N(totalUnits) + " units"} />
              <KPI label="Avg WOS" value={avgWos ? avgWos.toFixed(1) + "w" : "—"} color={!avgWos ? "#666" : avgWos < 2 ? "#ef4444" : avgWos > 6 ? "#f97316" : "#22c55e"} />
            </div>

            {/* Health Indicators */}
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 10, marginBottom: 16 }}>
              <div style={{ background: "#111", border: "1px solid #222", borderRadius: 6, padding: "10px 14px" }}>
                <div style={{ fontSize: 9, color: "#666", fontFamily: "'JetBrains Mono', monospace", textTransform: "uppercase", letterSpacing: 1 }}>PORTFOLIO HEALTH</div>
                <div style={{ display: "flex", gap: 6, marginTop: 8 }}>
                  <div style={{ flex: bpSelling.length, height: 6, background: "#22c55e", borderRadius: 3 }} />
                  <div style={{ flex: bpDead.length || 0.01, height: 6, background: "#ef4444", borderRadius: 3 }} />
                </div>
                <div style={{ fontSize: 10, color: "#888", fontFamily: "'JetBrains Mono', monospace", marginTop: 6 }}>
                  <span style={{ color: "#22c55e" }}>{bpSelling.length} selling</span> · <span style={{ color: "#ef4444" }}>{bpDead.length} dead</span>
                </div>
              </div>
              <div style={{ background: "#111", border: "1px solid #222", borderRadius: 6, padding: "10px 14px" }}>
                <div style={{ fontSize: 9, color: "#666", fontFamily: "'JetBrains Mono', monospace", textTransform: "uppercase", letterSpacing: 1 }}>RISK EXPOSURE</div>
                <div style={{ fontSize: 16, fontWeight: 700, color: "#f97316", fontFamily: "'JetBrains Mono', monospace", marginTop: 4 }}>
                  {$(deadCost + overstockCost)}
                </div>
                <div style={{ fontSize: 10, color: "#888", fontFamily: "'JetBrains Mono', monospace", marginTop: 2 }}>
                  {$(deadCost)} dead · {$(overstockCost)} overstock
                </div>
              </div>
              <div style={{ background: "#111", border: "1px solid #222", borderRadius: 6, padding: "10px 14px" }}>
                <div style={{ fontSize: 9, color: "#666", fontFamily: "'JetBrains Mono', monospace", textTransform: "uppercase", letterSpacing: 1 }}>ACTION ITEMS</div>
                <div style={{ fontSize: 10, color: "#888", fontFamily: "'JetBrains Mono', monospace", marginTop: 6 }}>
                  <span style={{ color: "#ef4444" }}>{stockouts.length} stockout risk</span> · <span style={{ color: "#f97316" }}>{poItems.length} need reorder</span> · <span style={{ color: "#22c55e" }}>{$(poValue)} PO value</span>
                </div>
              </div>
            </div>

            {/* Velocity Spark Chart */}
            <div style={{ background: "#111", border: "1px solid #222", borderRadius: 6, padding: "14px 18px", marginBottom: 16 }}>
              <div style={{ fontSize: 9, color: "#666", fontFamily: "'JetBrains Mono', monospace", textTransform: "uppercase", letterSpacing: 1, marginBottom: 10 }}>WEEKLY UNIT VELOCITY</div>
              <div style={{ display: "flex", gap: 8, maxWidth: 400 }}>
                {spark(bw4, "4wk ago")}
                {spark(bw3, "3wk ago")}
                {spark(bw2, "2wk ago")}
                {spark(bw1, "This wk")}
              </div>
            </div>

            {/* Store Breakdown Grid */}
            <div style={{ fontSize: 9, color: "#666", fontFamily: "'JetBrains Mono', monospace", textTransform: "uppercase", letterSpacing: 1, marginBottom: 8 }}>STORE PERFORMANCE</div>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 10, marginBottom: 16 }}>
              {storeBreakdown.sort((a, b) => b.rev - a.rev).map((sb) => (
                <div key={sb.s} style={{ background: "#111", border: "1px solid #222", borderRadius: 6, padding: "10px 14px" }}>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 6 }}>
                    <div style={{ fontSize: 12, fontWeight: 700, color: "#e5e5e5" }}>{sb.s}</div>
                    <div style={{ fontSize: 10, fontWeight: 600, color: sb.tr >= 0 ? "#22c55e" : "#ef4444", fontFamily: "'JetBrains Mono', monospace" }}>
                      {sb.tr >= 0 ? "▲" : "▼"} {Math.abs(sb.tr)}%
                    </div>
                  </div>
                  <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "4px 12px", fontSize: 10, fontFamily: "'JetBrains Mono', monospace" }}>
                    <div style={{ color: "#666" }}>Revenue</div><div style={{ textAlign: "right", color: "#22c55e" }}>{$(sb.rev)}</div>
                    <div style={{ color: "#666" }}>Velocity</div><div style={{ textAlign: "right", color: "#3b82f6" }}>{sb.vel}/wk</div>
                    <div style={{ color: "#666" }}>Inventory</div><div style={{ textAlign: "right" }}>{$(sb.inv)}</div>
                    <div style={{ color: "#666" }}>SKUs</div><div style={{ textAlign: "right" }}>{sb.skus} <span style={{ color: sb.dead > 0 ? "#ef4444" : "#666" }}>({sb.dead} dead)</span></div>
                  </div>
                </div>
              ))}
            </div>

            {/* Category Breakdown */}
            {catBreakdown.length > 1 && <>
              <div style={{ fontSize: 9, color: "#666", fontFamily: "'JetBrains Mono', monospace", textTransform: "uppercase", letterSpacing: 1, marginBottom: 8 }}>CATEGORY MIX</div>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))", gap: 8, marginBottom: 16 }}>
                {catBreakdown.map((cb) => (
                  <div key={cb.cat} style={{ background: "#111", border: "1px solid #222", borderRadius: 6, padding: "8px 12px" }}>
                    <div style={{ fontSize: 10, fontWeight: 600, color: "#e5e5e5", marginBottom: 4 }}>{cb.cat}</div>
                    <div style={{ fontSize: 10, fontFamily: "'JetBrains Mono', monospace", color: "#888" }}>
                      <span style={{ color: "#22c55e" }}>{$(cb.rev)}</span> · {cb.vel}/wk · {cb.skus} SKUs
                    </div>
                  </div>
                ))}
              </div>
            </>}

            {/* Full Product Table */}
            <div style={{ fontSize: 9, color: "#666", fontFamily: "'JetBrains Mono', monospace", textTransform: "uppercase", letterSpacing: 1, marginBottom: 8 }}>ALL PRODUCTS</div>
            <Table rows={bp.sort((a, b) => b.nr - a.nr)} cols={[
              { l: "Store", g: (r) => r.s, k: "s" }, { l: "Product", g: (r) => r.p, k: "p" },
              { l: "Cat", g: (r) => r.cat, k: "cat" },
              { l: "Cls", g: (r) => <span className={`c${r.cls}`}>{r.cls}</span>, k: "cls" },
              { l: "Vel/Wk", g: (r) => r.wv.toFixed(1), nm: 1, k: "wv", c: (r) => ({ color: r.wv >= 20 ? "#22c55e" : r.wv >= 10 ? "#3b82f6" : r.wv >= 3 ? "#f59e0b" : "#666" }) },
              { l: "Trend", g: (r) => Trend(r), nm: 1, k: "tr" },
              { l: "On Hand", g: (r) => N(r.oh), nm: 1, k: "oh" },
              { l: "WOS", g: (r) => r.wos ? r.wos.toFixed(1) : "—", nm: 1, k: "wos", c: (r) => !r.wos ? {} : r.wos < 1 ? { color: "#ef4444", fontWeight: 700 } : r.wos > 8 ? { color: "#f97316" } : {} },
              { l: "Net Rev", g: (r) => r.nr > 0 ? $(r.nr) : "—", nm: 1, k: "nr", c: (r) => r.nr > 0 ? { color: "#22c55e" } : {} },
              { l: "Margin", g: (r) => r.mgn > 0 ? pc(r.mgn) : "—", nm: 1, k: "mgn" },
              { l: "Inv Cost", g: (r) => $(r.ic), nm: 1, k: "ic" },
              { l: "Supplier", g: (r) => r.sup || "—", k: "sup" },
            ]} />
          </>);
        })()}

        {tab === 8 && (() => {
          let need = products.filter((p) => p.oq > 0);
          // Normalize supplier - empty becomes "Unknown Supplier"
          need = need.map((p) => ({ ...p, sup: p.sup || "Unknown Supplier" }));
          // Sort: known suppliers alphabetically first, Unknown at bottom, then by line value desc
          need.sort((a, b) => {
            const aUnk = a.sup === "Unknown Supplier" ? 1 : 0;
            const bUnk = b.sup === "Unknown Supplier" ? 1 : 0;
            if (aUnk !== bUnk) return aUnk - bUnk;
            if (a.sup !== b.sup) return a.sup < b.sup ? -1 : 1;
            return (b.oq * b.uc) - (a.oq * a.uc);
          });
          // Apply filters
          if (filters.s !== "All") need = need.filter((p) => p.s === filters.s);
          if (filters.c !== "All") need = need.filter((p) => p.cat === filters.c);
          if (filters.b !== "All") need = need.filter((p) => p.b === filters.b);
          if (filters.q) need = need.filter((p) => (p.p + p.b + p.sup).toLowerCase().includes(filters.q.toLowerCase()));
          const tv = need.reduce((a, p) => a + p.oq * p.uc, 0), tu = need.reduce((a, p) => a + p.oq, 0);
          const supList = [...new Set(need.map((p) => p.sup))];
          const poSup = filters._poSup || "All";
          const filtered = poSup === "All" ? need : need.filter((p) => p.sup === poSup);
          const supOptions = ["All", ...supList.filter((s) => s !== "Unknown Supplier").sort(), ...(supList.includes("Unknown Supplier") ? ["Unknown Supplier"] : [])];
          return (<>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(190px, 1fr))", gap: 10, marginBottom: 16 }}>
              <KPI label="PO Value" value={$(tv)} sub={N(tu) + " units"} color="#f97316" />
              <KPI label="Line Items" value={need.length} color="#3b82f6" />
              <KPI label="Suppliers" value={supList.length} />
              <KPI label="WOS Target" value={wos} color="#8b5cf6" />
            </div>
            <div style={{ display: "flex", gap: 8, marginBottom: 12, flexWrap: "wrap", alignItems: "flex-end" }}>
              <div><label style={lbl}>Supplier</label><select style={sel} value={poSup} onChange={(e) => setFilters((f) => ({ ...f, _poSup: e.target.value }))}>{supOptions.map((s) => <option key={s}>{s}</option>)}</select></div>
              <div><label style={lbl}>Store</label><select style={sel} value={filters.s} onChange={(e) => setFilters((f) => ({ ...f, s: e.target.value }))}>{stores.map((s) => <option key={s}>{s}</option>)}</select></div>
              <div><label style={lbl}>Category</label><select style={sel} value={filters.c} onChange={(e) => setFilters((f) => ({ ...f, c: e.target.value }))}>{cats.map((c) => <option key={c}>{c}</option>)}</select></div>
              <div><label style={lbl}>Search</label><input style={{ ...sel, width: 200 }} placeholder="Product, brand, or supplier..." value={filters.q} onChange={(e) => setFilters((f) => ({ ...f, q: e.target.value }))} /></div>
            </div>
            <Table rows={filtered} cols={[
              { l: "Supplier", g: (r) => r.sup, k: "sup", c: (r) => r.sup === "Unknown Supplier" ? { color: "#666", fontStyle: "italic" } : {} },
              { l: "Store", g: (r) => r.s, k: "s" }, { l: "Product", g: (r) => r.p, k: "p" },
              { l: "Cat", g: (r) => r.cat, k: "cat" },
              { l: "Cls", g: (r) => r.cls, k: "cls" }, { l: "Vel/Wk", g: (r) => r.wv.toFixed(1), nm: 1, k: "wv" },
              { l: "On Hand", g: (r) => r.oh, nm: 1, k: "oh" },
              { l: "Par", g: (r) => r.par, nm: 1, k: "par", c: () => ({ color: "#8b5cf6" }) },
              { l: "Order", g: (r) => r.oq, nm: 1, k: "oq", c: () => ({ color: "#f97316", fontWeight: 700 }) },
              { l: "Line $", g: (r) => $(r.oq * r.uc), nm: 1, c: () => ({ color: "#22c55e" }) },
            ]} />
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
const td = { padding: 5, borderBottom: "1px solid #1a1a1a", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis", maxWidth: 280 };
const btnStyle = { background: "#22c55e11", color: "#22c55e", border: "1px solid #22c55e44", padding: "5px 14px", borderRadius: 4, cursor: "pointer", fontFamily: "'JetBrains Mono', monospace", fontSize: 10 };
