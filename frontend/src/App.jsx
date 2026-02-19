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

const CANNABIS_CATS = ["FLOWER", "Pre Rolls", "Concentrates", "Carts", "Disposables", "Edibles", "Infused Flower", "Capsules", "Tinctures", "Topicals"];

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
          // ── BRAND DEEP DIVE ──
          const brandList = [...new Set(products.map((p) => p.b).filter(Boolean))].sort();
          const bv = brandView || brandList[0] || "";
          const bdStore = filters._bdStore || "All";
          const bpAll = products.filter((p) => p.b === bv);
          const bp = bdStore === "All" ? bpAll : bpAll.filter((p) => p.s === bdStore);
          const brandStoreList = ["All", ...[...new Set(bpAll.map((p) => p.s))].sort()];

          // Aggregate brand stats
          const bRev = bp.reduce((a, p) => a + p.nr, 0);
          const bCogs = bp.reduce((a, p) => a + p.cogs, 0);
          const bInvCost = bp.reduce((a, p) => a + p.ic, 0);
          const bInvUnits = bp.reduce((a, p) => a + p.oh, 0);
          const bUnitsSold = bp.reduce((a, p) => a + (p.wv * (S.period ? 31/7 : 1)), 0);
          const bMargin = bRev > 0 ? (bRev - bCogs) / bRev * 100 : 0;
          const bVel = bp.reduce((a, p) => a + p.wv, 0);
          const selling = bp.filter((p) => p.wv > 0);
          const dead = bp.filter((p) => p.wv === 0);
          const overstock = bp.filter((p) => p.wos && p.wos > 8 && p.wv > 0);
          const stockout = bp.filter((p) => p.wos != null && p.wos < 2 && p.wv >= 1);
          const needsOrder = bp.filter((p) => p.oq > 0);
          const orderVal = needsOrder.reduce((a, p) => a + p.oq * p.uc, 0);

          // WoW trend for brand
          const bW1 = bp.reduce((a, p) => a + (p.w1 || 0), 0);
          const bW2 = bp.reduce((a, p) => a + (p.w2 || 0), 0);
          const bW3 = bp.reduce((a, p) => a + (p.w3 || 0), 0);
          const bW4 = bp.reduce((a, p) => a + (p.w4 || 0), 0);
          const bPriorAvg = (bW2 + bW3 + bW4) / 3;
          const bTrendPct = bPriorAvg > 0 ? ((bW1 - bPriorAvg) / bPriorAvg * 100) : bW1 > 0 ? 100 : 0;
          const tArrow = bTrendPct > 0 ? "▲" : bTrendPct < 0 ? "▼" : "—";
          const tColor = bTrendPct >= 20 ? "#22c55e" : bTrendPct > 0 ? "#4ade80" : bTrendPct > -20 ? "#f97316" : "#ef4444";

          // Category breakdown
          const catBreak = {};
          bp.forEach((p) => {
            if (!catBreak[p.cat]) catBreak[p.cat] = { rev: 0, vel: 0, inv: 0, units: 0, count: 0 };
            catBreak[p.cat].rev += p.nr;
            catBreak[p.cat].vel += p.wv;
            catBreak[p.cat].inv += p.ic;
            catBreak[p.cat].units += p.oh;
            catBreak[p.cat].count++;
          });

          // Store breakdown (always from all stores for context)
          const storeBreak = {};
          bpAll.forEach((p) => {
            if (!storeBreak[p.s]) storeBreak[p.s] = { rev: 0, vel: 0, inv: 0, units: 0, count: 0, w1: 0, w2: 0, w3: 0, w4: 0 };
            storeBreak[p.s].rev += p.nr;
            storeBreak[p.s].vel += p.wv;
            storeBreak[p.s].inv += p.ic;
            storeBreak[p.s].units += p.oh;
            storeBreak[p.s].count++;
            storeBreak[p.s].w1 += (p.w1 || 0);
            storeBreak[p.s].w2 += (p.w2 || 0);
            storeBreak[p.s].w3 += (p.w3 || 0);
            storeBreak[p.s].w4 += (p.w4 || 0);
          });

          // Class distribution
          const clsDist = { A: 0, B: 0, C: 0, D: 0 };
          bp.forEach((p) => clsDist[p.cls]++);

          // Velocity sparkline (W4 → W3 → W2 → W1)
          const weeks = [bW4, bW3, bW2, bW1];
          const maxW = Math.max(...weeks, 1);
          const sparkW = 120, sparkH = 32;

          // Top products by revenue
          const topRev = [...bp].filter((p) => p.nr > 0).sort((a, b) => b.nr - a.nr).slice(0, 5);
          // Top movers (highest trend)
          const topMovers = [...bp].filter((p) => p.tr && p.wv > 0).sort((a, b) => b.tr - a.tr).slice(0, 5);
          // Biggest decliners
          const decliners = [...bp].filter((p) => p.tr < 0 && p.wv > 0).sort((a, b) => a.tr - b.tr).slice(0, 5);

          // All brand revenue for rank
          const allBrandRev = {};
          products.forEach((p) => { if (p.b) allBrandRev[p.b] = (allBrandRev[p.b] || 0) + p.nr; });
          const brandRanked = Object.entries(allBrandRev).sort((a, b) => b[1] - a[1]);
          const brandRank = brandRanked.findIndex(([b]) => b === bv) + 1;

          const bar = { background: "#1a1a1a", borderRadius: 4, overflow: "hidden", height: 6, marginTop: 3 };
          const barFill = (pct, color) => ({ width: Math.max(pct, 2) + "%", height: "100%", background: color, borderRadius: 4 });
          const miniCard = { background: "#111", border: "1px solid #1a1a1a", borderRadius: 6, padding: "8px 10px" };
          const miniLabel = { fontSize: 8, color: "#555", fontFamily: "'JetBrains Mono', monospace", textTransform: "uppercase", letterSpacing: 0.5 };
          const miniVal = { fontSize: 13, fontWeight: 700, fontFamily: "'JetBrains Mono', monospace" };

          return (<>
            {/* Brand + Store Selector */}
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

            {/* Hero KPIs */}
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(155px, 1fr))", gap: 8, marginBottom: 16 }}>
              <KPI label="Net Revenue" value={$(bRev)} color="#22c55e" />
              <KPI label="Margin" value={pc(bMargin)} color={bMargin >= 52 ? "#22c55e" : bMargin >= 40 ? "#f59e0b" : "#ef4444"} />
              <KPI label="Total Velocity" value={bVel.toFixed(1) + "/wk"} sub={selling.length + " SKUs selling"} color="#3b82f6" />
              <div style={{ background: "#111", border: "1px solid #222", borderRadius: 6, padding: "12px 14px" }}>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 9, color: "#666", textTransform: "uppercase", letterSpacing: 1 }}>WoW Trend</div>
                <div style={{ display: "flex", alignItems: "center", gap: 8, marginTop: 4 }}>
                  <span style={{ fontSize: 22, fontWeight: 800, color: tColor, fontFamily: "'JetBrains Mono', monospace" }}>{tArrow} {Math.abs(Math.round(bTrendPct))}%</span>
                  <svg width={sparkW} height={sparkH} style={{ marginLeft: "auto" }}>
                    {weeks.map((w, i) => {
                      const bw = sparkW / 4 - 2;
                      const h = Math.max((w / maxW) * sparkH, 2);
                      const fill = i === 3 ? (bTrendPct >= 0 ? "#22c55e" : "#ef4444") : "#333";
                      return <rect key={i} x={i * (bw + 2)} y={sparkH - h} width={bw} height={h} rx={2} fill={fill} />;
                    })}
                  </svg>
                </div>
                <div style={{ display: "flex", justifyContent: "space-between", marginTop: 2 }}>
                  {["W4", "W3", "W2", "W1"].map((w, i) => <span key={w} style={{ fontSize: 7, color: "#444", fontFamily: "'JetBrains Mono', monospace" }}>{w}: {weeks[i]}</span>)}
                </div>
              </div>
              <KPI label="Inventory" value={$(bInvCost)} sub={N(bInvUnits) + " units"} />
              <KPI label="Dead SKUs" value={dead.length} sub={dead.length > 0 ? $(dead.reduce((a, p) => a + p.ic, 0)) + " tied up" : "clean"} color={dead.length > 0 ? "#ef4444" : "#22c55e"} />
              <KPI label="Needs Order" value={needsOrder.length + " SKUs"} sub={orderVal > 0 ? $(orderVal) : "stocked up"} color="#f97316" />
              <KPI label="At Risk" value={stockout.length + " SKUs"} sub={stockout.length > 0 ? "< 2 WOS" : "all good"} color={stockout.length > 0 ? "#ef4444" : "#22c55e"} />
            </div>

            {/* Insights Row */}
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 12, marginBottom: 16 }}>
              {/* Top Revenue Products */}
              <div style={{ background: "#111", border: "1px solid #1a1a1a", borderRadius: 6, padding: 12 }}>
                <div style={{ ...miniLabel, marginBottom: 8, color: "#22c55e" }}>Top Revenue</div>
                {topRev.map((p, i) => (
                  <div key={i} style={{ display: "flex", justifyContent: "space-between", padding: "4px 0", borderBottom: "1px solid #1a1a1a", fontSize: 10, fontFamily: "'JetBrains Mono', monospace" }}>
                    <span style={{ color: "#ccc", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", maxWidth: "65%" }}>{p.p}</span>
                    <span style={{ color: "#22c55e", fontWeight: 600 }}>{$(p.nr)}</span>
                  </div>
                ))}
                {topRev.length === 0 && <div style={{ color: "#444", fontSize: 10 }}>No revenue data</div>}
              </div>

              {/* Hot Movers */}
              <div style={{ background: "#111", border: "1px solid #1a1a1a", borderRadius: 6, padding: 12 }}>
                <div style={{ ...miniLabel, marginBottom: 8, color: "#4ade80" }}>🔥 Hot Movers</div>
                {topMovers.map((p, i) => (
                  <div key={i} style={{ display: "flex", justifyContent: "space-between", padding: "4px 0", borderBottom: "1px solid #1a1a1a", fontSize: 10, fontFamily: "'JetBrains Mono', monospace" }}>
                    <span style={{ color: "#ccc", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", maxWidth: "60%" }}>{p.p}</span>
                    <span style={{ color: "#4ade80", fontWeight: 600 }}>▲ {p.tr}%</span>
                  </div>
                ))}
                {topMovers.length === 0 && <div style={{ color: "#444", fontSize: 10 }}>No trending data</div>}
              </div>

              {/* Decliners */}
              <div style={{ background: "#111", border: "1px solid #1a1a1a", borderRadius: 6, padding: 12 }}>
                <div style={{ ...miniLabel, marginBottom: 8, color: "#ef4444" }}>📉 Declining</div>
                {decliners.map((p, i) => (
                  <div key={i} style={{ display: "flex", justifyContent: "space-between", padding: "4px 0", borderBottom: "1px solid #1a1a1a", fontSize: 10, fontFamily: "'JetBrains Mono', monospace" }}>
                    <span style={{ color: "#ccc", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", maxWidth: "60%" }}>{p.p}</span>
                    <span style={{ color: "#ef4444", fontWeight: 600 }}>▼ {Math.abs(p.tr)}%</span>
                  </div>
                ))}
                {decliners.length === 0 && <div style={{ color: "#444", fontSize: 10 }}>No decliners — solid</div>}
              </div>
            </div>

            {/* Store Performance Heatmap */}
            <div style={{ background: "#111", border: "1px solid #1a1a1a", borderRadius: 6, padding: 12, marginBottom: 16 }}>
              <div style={{ ...miniLabel, marginBottom: 10, color: "#8b5cf6" }}>Store Performance</div>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))", gap: 8 }}>
                {Object.entries(storeBreak).sort((a, b) => b[1].rev - a[1].rev).map(([store, sd]) => {
                  const sPrior = (sd.w2 + sd.w3 + sd.w4) / 3;
                  const sTrend = sPrior > 0 ? ((sd.w1 - sPrior) / sPrior * 100) : sd.w1 > 0 ? 100 : 0;
                  const revPct = bRev > 0 ? (sd.rev / bRev * 100) : 0;
                  return (
                    <div key={store} style={miniCard}>
                      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                        <span style={{ fontSize: 11, fontWeight: 700, fontFamily: "'JetBrains Mono', monospace", color: "#e5e5e5" }}>{store}</span>
                        <span style={{ fontSize: 9, fontWeight: 600, fontFamily: "'JetBrains Mono', monospace",
                          color: sTrend >= 20 ? "#22c55e" : sTrend > 0 ? "#4ade80" : sTrend > -20 ? "#f97316" : "#ef4444" }}>
                          {sTrend > 0 ? "▲" : sTrend < 0 ? "▼" : "—"} {Math.abs(Math.round(sTrend))}%
                        </span>
                      </div>
                      <div style={{ display: "flex", justifyContent: "space-between", marginTop: 4, fontSize: 9, fontFamily: "'JetBrains Mono', monospace" }}>
                        <span style={{ color: "#22c55e" }}>{$(sd.rev)}</span>
                        <span style={{ color: "#666" }}>{sd.count} SKUs</span>
                        <span style={{ color: "#3b82f6" }}>{sd.vel.toFixed(1)}/wk</span>
                      </div>
                      <div style={bar}><div style={barFill(revPct, "#22c55e44")} /></div>
                    </div>
                  );
                })}
              </div>
            </div>

            {/* Category Breakdown */}
            <div style={{ background: "#111", border: "1px solid #1a1a1a", borderRadius: 6, padding: 12, marginBottom: 16 }}>
              <div style={{ ...miniLabel, marginBottom: 10, color: "#f59e0b" }}>Category Breakdown</div>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead><tr>
                  {["Category", "Revenue", "Vel/Wk", "SKUs", "Inventory", "Rev Share"].map((h) => (
                    <th key={h} style={{ ...th, cursor: "default" }}>{h}</th>
                  ))}
                </tr></thead>
                <tbody>
                  {Object.entries(catBreak).sort((a, b) => b[1].rev - a[1].rev).map(([cat, cd]) => (
                    <tr key={cat}>
                      <td style={{ padding: "5px 6px", fontSize: 10, fontFamily: "'JetBrains Mono', monospace", color: "#ccc" }}>{cat}</td>
                      <td style={{ padding: "5px 6px", fontSize: 10, fontFamily: "'JetBrains Mono', monospace", textAlign: "right", color: "#22c55e" }}>{$(cd.rev)}</td>
                      <td style={{ padding: "5px 6px", fontSize: 10, fontFamily: "'JetBrains Mono', monospace", textAlign: "right", color: "#3b82f6" }}>{cd.vel.toFixed(1)}</td>
                      <td style={{ padding: "5px 6px", fontSize: 10, fontFamily: "'JetBrains Mono', monospace", textAlign: "right", color: "#666" }}>{cd.count}</td>
                      <td style={{ padding: "5px 6px", fontSize: 10, fontFamily: "'JetBrains Mono', monospace", textAlign: "right" }}>{$(cd.inv)}</td>
                      <td style={{ padding: "5px 6px", fontSize: 10, fontFamily: "'JetBrains Mono', monospace", textAlign: "right" }}>
                        <div style={{ display: "flex", alignItems: "center", gap: 4, justifyContent: "flex-end" }}>
                          <span>{bRev > 0 ? pc(cd.rev / bRev * 100) : "—"}</span>
                          <div style={{ ...bar, width: 40, display: "inline-block" }}><div style={barFill(bRev > 0 ? cd.rev / bRev * 100 : 0, "#f59e0b")} /></div>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {/* Full Product Table */}
            <div style={{ ...miniLabel, marginBottom: 6, color: "#666" }}>All {bv} Products ({bp.length})</div>
            <Table rows={[...bp].sort((a, b) => b.nr - a.nr)} cols={[
              { l: "Store", g: (r) => r.s, k: "s" }, { l: "Product", g: (r) => r.p, k: "p" },
              { l: "Cat", g: (r) => r.cat, k: "cat" },
              { l: "Cls", g: (r) => <span className={`c${r.cls}`}>{r.cls}</span>, k: "cls" },
              { l: "Vel/Wk", g: (r) => r.wv.toFixed(1), nm: 1, k: "wv" },
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
