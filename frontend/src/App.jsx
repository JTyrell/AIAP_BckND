// Built from the original prototype:// Built from the original prototype: https://github.com/bztjtech/AIAP_BckND
// For the new Sprint, refer to:
// https://github.com/bztjtech/AIAP_Frontend/src/App.jsx

import { useState, useEffect } from "react";

const CARD_TYPES = ["Visa", "Mastercard"];

// Hourly transaction multipliers based on real Spar Nord peak pattern
const HOURLY_WEIGHTS = [
  0.02, 0.01, 0.01, 0.01, 0.01, 0.03, // 00–05
  0.08, 0.14, 0.18, 0.21, 0.24, 0.22, // 06–11  ← morning peak
  0.20, 0.19, 0.17, 0.18, 0.19, 0.16, // 12–17  ← afternoon peak
  0.13, 0.10, 0.08, 0.06, 0.04, 0.03  // 18–23
];

// Weekly pattern (Mon=0 … Sun=6). Saturday spike is real in Spar Nord data.
const WEEKLY_MULT = [0.88, 0.92, 0.95, 0.97, 1.05, 1.22, 0.80];

let ATM_DATA = [];
let MAINTENANCE_QUEUE = [];
let ALERTS = [];

const DataLoader = ({ token, children }) => {
  const [loaded, setLoaded] = useState(false);
  const [err, setErr] = useState(null);
  const [refreshKey, setRefreshKey] = useState(0);

  useEffect(() => {
    window.triggerFleetRefresh = () => setRefreshKey(k => k + 1);
    if (!token) return;
    fetch('/api/v1/staff/fleet/health', {
      headers: { 'Authorization': `Bearer ${token}` }
    })
      .then(res => {
        if (res.status === 401 || res.status === 422) {
          // Token expired or invalid — force re-login
          localStorage.removeItem('aiap_token');
          window.location.reload();
          return;
        }
        if (!res.ok) throw new Error(`Server responded ${res.status}`);
        return res.json();
      })
      .then(data => {
        if (!data) return; // guard for reload case
        ATM_DATA = data.map(atm => ({
          id: atm.id, name: atm.bank_name, short: atm.bank_name,
          location: atm.location,
          manufacturer: atm.manufacturer, model: atm.model,
          lat: 17.3985, lng: -76.5492, // mock geo
          street: atm.location, building: atm.location,
          services: ["Withdrawal", "Deposits", "Balance Check", "Bill Payments", "Transfer"],
          card_types: ["MasterCard", "Visa", "Multilink (Active)"],
          currency: "JMD",
          status: atm.status,
          health: Math.round(atm.health_score * 100) / 100, healthTrend: 0,
          uptime: Math.round(atm.uptime * 100) / 100, uptime_7d: [atm.uptime, atm.uptime, atm.uptime, atm.uptime, atm.uptime, atm.uptime, atm.uptime],
          errorCount7d: atm.error_count, errorRate: Math.round((atm.error_count / 7) * 100) / 100,
          errorAcceleration: Math.round(atm.error_acceleration * 100) / 100,
          lastMaintenance: "2026-03-01", daysSinceMaintenance: 30,
          maintenance_count_30d: 1, corrective: false,
          cashLevel: Math.round(atm.cash_level * 100) / 100, cashStress: Math.round((1 - (atm.cash_level / 100)) * 100) / 100, daysToDepletion: Math.round(atm.days_to_depletion),
          avgDailyWithdrawal: Math.round(atm.avg_daily_withdrawal || 0), transactions24h: atm.transactions_24h, transactionVelocity: Math.round(atm.transactions_24h / 24), avgAmount: Math.round(atm.avg_amount || 0),
          failureProbability: Math.round(atm.failure_probability * 100) / 100,
          alerts: atm.alerts || [],
          weeklyTxns: atm.historical_series?.weekly_txns || [0, 0, 0, 0, 0, 0, 0],
          weeklyLabels: atm.historical_series?.weekly_labels || ["M", "T", "W", "T", "F", "S", "S"],
          hourlyTxns: atm.historical_series?.hourly_txns || [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
          monthlyInactive: atm.historical_series?.monthly_inactive || [0, 0, 0],
          monthlyLabels: atm.historical_series?.monthly_labels || ["J", "F", "M"],
          cardBreakdown: { Visa: 60, Mastercard: 40, Maestro: 0, LINX: 0, JCB: 0 }
        }));

        const tempAlerts = [];
        ATM_DATA.forEach(atm => {
          atm.alerts.forEach(al => {
            let sev = "MEDIUM";
            if (al.includes("CRITICAL") || al.includes("OUT_OF_SERVICE")) sev = "CRITICAL";
            else if (al.includes("LOW_CASH") || al.includes("HIGH_RISK")) sev = "HIGH";
            tempAlerts.push({ time: "Today", atm: atm.id, severity: sev, msg: al });
          });
        });
        ALERTS = tempAlerts;

        MAINTENANCE_QUEUE = ATM_DATA
          .filter(a => a.health < 80 || a.cashLevel < 35)
          .map(a => ({
            id: a.id, priority: a.health < 55 ? "CRITICAL" : "HIGH",
            issue: `Automated ML flag. Cash: ${a.cashLevel}%, Health: ${a.health}%.`,
            eta: a.daysToDepletion < 3 ? "Today" : "Tomorrow", tech: "Unassigned", estimatedDuration: "1-2 hrs"
          }));

        setLoaded(true);
      })
      .catch(e => setErr(e.message));
  }, [token, refreshKey]);

  if (err) return <div style={{ color: "red", padding: 20 }}>Failed to load data: {err} <br /> Is the Python backend running on 5001?</div>;
  if (!loaded) return <div style={{ color: "#22c55e", background: "#0b1535", height: "100vh", display: "flex", alignItems: "center", justifyContent: "center", fontFamily: "'IBM Plex Mono',monospace" }}>Syncing Neural ATM Matrix...</div>;

  return children;
};

// ── HELPERS ───────────────────────────────────────────────────────────────────
const hc = (h) => h >= 80 ? "#22c55e" : h >= 55 ? "#f59e0b" : "#ef4444";
const hl = (h) => h >= 80 ? "Healthy" : h >= 55 ? "Fair" : "Critical";
const cc = (c) => c >= 50 ? "#22c55e" : c >= 30 ? "#f59e0b" : "#ef4444";
const sc = (s) => s === "in_service" ? "#22c55e" : "#ef4444";
const sl = (s) => s === "in_service" ? "In Service" : "Out of Service";
const ac = (sev) => ({
  CRITICAL: { bg: "#1a0000", border: "#ef4444", text: "#ef4444", pill: "#7f1d1d" },
  HIGH: { bg: "#1a0a00", border: "#f97316", text: "#f97316", pill: "#7c2d12" },
  MEDIUM: { bg: "#1a1500", border: "#f59e0b", text: "#f59e0b", pill: "#78350f" },
  LOW: { bg: "#001a08", border: "#22c55e", text: "#22c55e", pill: "#14532d" },
}[sev] || { bg: "#111", border: "#444", text: "#aaa", pill: "#333" });

const MiniSparkline = ({ data, color, h = 36 }) => {
  const max = Math.max(...data, 1);
  return (
    <div style={{ display: "flex", alignItems: "flex-end", gap: 2, height: h }}>
      {data.map((v, i) => (
        <div key={i} style={{
          flex: 1, background: color, borderRadius: "2px 2px 0 0",
          height: `${Math.max((v / max) * 100, v > 0 ? 4 : 0)}%`,
          opacity: 0.35 + (i / (data.length - 1)) * 0.65,
        }} />
      ))}
    </div>
  );
};

const UptimeDot = ({ val }) => (
  <span style={{
    display: "inline-block", width: 8, height: 8, borderRadius: "50%",
    background: val >= 97 ? "#22c55e" : val >= 90 ? "#f59e0b" : "#ef4444",
    marginRight: 3, flexShrink: 0
  }} />
);

const Pill = ({ label, color, bg }) => (
  <span style={{
    fontSize: 10, fontWeight: 700, padding: "2px 7px", borderRadius: 3,
    background: bg || "#1a1a1a", color: color || "#aaa",
    border: `1px solid ${color || "#444"}`, fontFamily: "'IBM Plex Mono',monospace",
    letterSpacing: 0.3
  }}>
    {label}
  </span>
);

const StatBox = ({ label, value, sub, color }) => (
  <div style={{ background: "#0a0a0a", border: "1px solid #1c1c1c", borderRadius: 8, padding: "14px 16px" }}>
    <div style={{ fontSize: 11, color: "#555", fontWeight: 600, textTransform: "uppercase", letterSpacing: 0.8, marginBottom: 6 }}>{label}</div>
    <div style={{ fontSize: 22, fontWeight: 700, color: color || "#e5e5e5", fontFamily: "'IBM Plex Mono',monospace", letterSpacing: -0.5 }}>{value}</div>
    {sub && <div style={{ fontSize: 11, color: "#444", marginTop: 3 }}>{sub}</div>}
  </div>
);

const SectionTitle = ({ children }) => (
  <div style={{
    fontSize: 10, fontWeight: 700, color: "#444", textTransform: "uppercase",
    letterSpacing: 2, marginBottom: 12, fontFamily: "'IBM Plex Mono',monospace",
    borderLeft: "2px solid #333", paddingLeft: 10
  }}>
    {children}
  </div>
);

// ── LOGIN ─────────────────────────────────────────────────────────────────────
const Login = ({ onLogin }) => {
  const [role, setRole] = useState("ops");
  const [user, setUser] = useState("");
  const [pass, setPass] = useState("");
  const [err, setErr] = useState("");
  const [loading, setLoading] = useState(false);

  const submit = async () => {
    setLoading(true);
    setErr("");

    if (role === "customer") {
      // Customer view is public — skip backend auth
      setTimeout(() => onLogin("customer", "Campus User", null), 600);
      return;
    }

    try {
      const res = await fetch('/api/v1/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: user, password: pass }),
      });
      const data = await res.json();
      if (!res.ok) {
        setErr(data.error || 'Authentication failed.');
        setLoading(false);
        return;
      }
      // Persist token in localStorage for session persistence
      localStorage.setItem('aiap_token', data.access_token);
      onLogin(data.role, data.username, data.access_token);
    } catch (e) {
      setErr('Cannot reach backend. Is the server running?');
      setLoading(false);
    }
  };

  return (
    <div style={{ minHeight: "100vh", background: "#0b1535", display: "flex", alignItems: "center", justifyContent: "center", fontFamily: "'DM Sans',sans-serif" }}>
      <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=DM+Sans:wght@300;400;500;600;700&display=swap" rel="stylesheet" />
      <style>{`* { box-sizing:border-box; } input::placeholder { color:#333; } input:focus { outline:none !important; border-color:#333 !important; }`}</style>

      {/* Grid background */}
      <div style={{ position: "fixed", inset: 0, backgroundImage: "linear-gradient(#111 1px,transparent 1px),linear-gradient(90deg,#111 1px,transparent 1px)", backgroundSize: "40px 40px", opacity: 0.4 }} />

      <div className="login-box">
        {/* Logo */}
        <div style={{ marginBottom: 32 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 6 }}>
            <div style={{ width: 36, height: 36, borderRadius: 8, background: "#111", border: "1px solid #222", display: "flex", alignItems: "center", justifyContent: "center" }}>
              <span style={{ fontSize: 18, color: "#22c55e" }}>◈</span>
            </div>
            <div>
              <div style={{ fontSize: 18, fontWeight: 700, color: "#e5e5e5", fontFamily: "'IBM Plex Mono',monospace", letterSpacing: 2 }}>AIAP</div>
              <div style={{ fontSize: 9, color: "#333", letterSpacing: 3, textTransform: "uppercase", fontFamily: "'IBM Plex Mono',monospace" }}>ATM Intelligence Platform</div>
            </div>
          </div>
          <div style={{ marginTop: 16, fontSize: 12, color: "#444" }}>UWI Mona Campus · Secure Access</div>
        </div>

        {/* Role toggle */}
        <div style={{ display: "flex", background: "#050505", border: "1px solid #1a1a1a", borderRadius: 8, padding: 3, marginBottom: 24, gap: 3 }}>
          {[["ops", "⬡ Operations"], ["customer", "◎ Customer"]].map(([r, l]) => (
            <button key={r} onClick={() => { setRole(r); setErr(""); setUser(""); setPass(""); }}
              style={{
                flex: 1, padding: "9px 0", border: "none", cursor: "pointer", borderRadius: 6, fontFamily: "'DM Sans',sans-serif", fontSize: 12, fontWeight: 600, transition: "all 0.15s",
                background: role === r ? "#111" : "transparent",
                color: role === r ? (r === "ops" ? "#22c55e" : "#60a5fa") : "#333",
                boxShadow: role === r ? "0 0 0 1px #1c1c1c" : "none"
              }}>
              {l}
            </button>
          ))}
        </div>

        {[["USERNAME", user, setUser, "text"], ["PASSWORD", pass, setPass, "password"]].map(([lbl, val, set, type]) => (
          <div key={lbl} style={{ marginBottom: 14 }}>
            <div style={{ fontSize: 10, color: "#444", fontFamily: "'IBM Plex Mono',monospace", letterSpacing: 1.5, marginBottom: 6 }}>{lbl}</div>
            <input value={val} type={type} onChange={e => { set(e.target.value); setErr(""); }}
              onKeyDown={e => e.key === "Enter" && submit()}
              style={{
                width: "100%", padding: "10px 12px", background: "#050505", border: `1px solid ${err ? "#ef4444" : "#1a1a1a"}`, borderRadius: 6,
                color: "#e5e5e5", fontSize: 13, fontFamily: "'IBM Plex Mono',monospace"
              }} />
          </div>
        ))}

        {err && <div style={{ color: "#ef4444", fontSize: 12, marginBottom: 10, fontFamily: "'IBM Plex Mono',monospace" }}>✕ {err}</div>}

        <button onClick={submit} disabled={loading}
          style={{
            width: "100%", padding: "12px", background: loading ? "#111" : role === "ops" ? "#052e16" : "#172554",
            border: `1px solid ${loading ? "#1a1a1a" : role === "ops" ? "#14532d" : "#1e3a8a"}`,
            borderRadius: 6, color: loading ? "#333" : role === "ops" ? "#22c55e" : "#60a5fa",
            fontSize: 13, fontWeight: 600, cursor: loading ? "not-allowed" : "pointer",
            fontFamily: "'DM Sans',sans-serif", letterSpacing: 0.3, transition: "all 0.2s", marginBottom: 20
          }}>
          {loading ? "Authenticating…" : "Sign In →"}
        </button>

        <div style={{ background: "#050505", border: "1px solid #111", borderRadius: 6, padding: "12px 14px" }}>
          <div style={{ fontSize: 9, color: "#333", letterSpacing: 2, fontFamily: "'IBM Plex Mono',monospace", marginBottom: 8 }}>DEMO CREDENTIALS</div>
          {[["ops.admin", "aiap2026", "Operations Dashboard"], ["customer", "uwiatm", "Customer ATM Finder"]].map(([u, p, desc]) => (
            <div key={u} style={{ fontSize: 11, color: "#444", fontFamily: "'IBM Plex Mono',monospace", lineHeight: 2 }}>
              <span style={{ color: "#22c55e" }}>{u}</span> / {p} <span style={{ color: "#333", fontSize: 10 }}>→ {desc}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

// ── OPS DASHBOARD ─────────────────────────────────────────────────────────────
const OpsDashboard = ({ userName, token, onLogout }) => {
  const [tab, setTab] = useState("overview");
  const [selected, setSelected] = useState(null);
  const [filter, setFilter] = useState("all");
  const [tick, setTick] = useState(0);

  useEffect(() => {
    const t = setInterval(() => setTick(x => x + 1), 30000);
    return () => clearInterval(t);
  }, []);

  const now = new Date();
  const timeStr = now.toLocaleTimeString("en-JM", { hour: "2-digit", minute: "2-digit" });

  const critCount = ATM_DATA.filter(a => a.health < 55).length;
  const alertCount = ATM_DATA.filter(a => a.alerts.length > 0).length;
  const avgHealth = Math.round((ATM_DATA.reduce((s, a) => s + a.health, 0) / ATM_DATA.length) * 100) / 100;
  const totalTxns = ATM_DATA.reduce((s, a) => s + a.transactions24h, 0);
  const offlineCount = ATM_DATA.filter(a => a.status !== "in_service").length;

  const filtered = ATM_DATA.filter(a =>
    filter === "all" ? true :
      filter === "critical" ? a.health < 55 :
        filter === "warning" ? (a.health >= 55 && a.health < 80) :
          filter === "healthy" ? a.health >= 80 :
            a.status === filter
  );

  const HealthBar = ({ value, width = 120 }) => (
    <div style={{ width, height: 4, background: "#111", borderRadius: 2, overflow: "hidden" }}>
      <div style={{ width: `${value}%`, height: "100%", background: hc(value), borderRadius: 2, transition: "width 1s ease" }} />
    </div>
  );

  return (
    <div style={{ minHeight: "100vh", background: "#160f55", color: "#e5e5e5", fontFamily: "'DM Sans',sans-serif" }}>
      <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=DM+Sans:wght@300;400;500;600;700&display=swap" rel="stylesheet" />
      <style>{`
        * { box-sizing:border-box; }
        ::-webkit-scrollbar { width:4px; height:4px; }
        ::-webkit-scrollbar-track { background:#050505; }
        ::-webkit-scrollbar-thumb { background:#1c1c1c; border-radius:2px; }
        @keyframes blink { 0%,100%{opacity:1} 50%{opacity:0.3} }
        @keyframes fadeIn { from{opacity:0;transform:translateY(8px)} to{opacity:1;transform:translateY(0)} }
      `}</style>

      {/* Unified Sticky Header Container */}
      <div style={{ position: "sticky", top: 0, zIndex: 110, background: "rgba(5, 5, 5, 0.8)", backdropFilter: "blur(12px)", borderBottom: "1px solid #111" }}>
        {/* Topbar */}
        <div style={{ height: 52, display: "flex", alignItems: "center", padding: "0 24px", gap: 20 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <span style={{ fontSize: 16, color: "#22c55e" }}>◈</span>
            <span style={{ fontFamily: "'IBM Plex Mono',monospace", fontSize: 14, fontWeight: 600, color: "#e5e5e5", letterSpacing: 2 }}>AIAP</span>
            <span style={{ fontSize: 10, color: "#333", fontFamily: "'IBM Plex Mono',monospace", letterSpacing: 1 }}>OPS</span>
          </div>
          <div style={{ flex: 1 }} />
          {/* Live indicator */}
          <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11, color: "#333", fontFamily: "'IBM Plex Mono',monospace" }}>
            <span style={{ width: 6, height: 6, borderRadius: "50%", background: "#22c55e", display: "inline-block", animation: "blink 2s infinite" }} />
            LIVE · {timeStr}
          </div>
          {critCount > 0 && (
            <div style={{ background: "#091038", border: "1px solid #ef4444", borderRadius: 4, padding: "3px 10px", fontSize: 11, color: "#ef4444", fontFamily: "'IBM Plex Mono',monospace", fontWeight: 600, display: "flex", alignItems: "center", gap: 6 }}>
              <span style={{ animation: "blink 1s infinite", display: "inline-block" }}>⚑</span> {critCount} CRITICAL
            </div>
          )}
          <div style={{ fontSize: 12, color: "#333" }}>
            <span style={{ color: "#555" }}>{userName}</span>
            <span style={{ margin: "0 8px", color: "#222" }}>·</span>
            <span style={{ cursor: "pointer", color: "#444" }} onClick={onLogout}>sign out</span>
          </div>
        </div>

        {/* Nav Tabs */}
        <div style={{ borderTop: "1px solid rgba(255,255,255,0.05)", padding: "0 24px", display: "flex", gap: 0 }}>
          {[["overview", "Overview"], ["atms", "Fleet"], ["alerts", "Alerts"], ["maintenance", "Maintenance"], ["analytics", "Analytics"], ["data", "Data Management"]].map(([id, lbl]) => (
            <button key={id} onClick={() => { setTab(id); if (id !== "atms") setSelected(null); }}
              style={{
                padding: "13px 18px", background: "none", border: "none", cursor: "pointer", fontFamily: "'DM Sans',sans-serif", fontSize: 12, fontWeight: 600, transition: "all 0.15s",
                color: tab === id ? "#e5e5e5" : "#444",
                borderBottom: tab === id ? "2px solid #22c55e" : "2px solid transparent"
              }}>
              {lbl}
              {id === "alerts" && alertCount > 0 && <span style={{ marginLeft: 6, background: "#ef4444", borderRadius: 3, padding: "1px 5px", fontSize: 9, color: "#fff", fontFamily: "'IBM Plex Mono',monospace" }}>{alertCount}</span>}
            </button>
          ))}
        </div>
      </div>

      <div style={{ padding: "20px 24px", maxWidth: 1400, margin: "0 auto", animation: "fadeIn 0.3s ease" }}>

        {/* ───── OVERVIEW ───── */}
        {tab === "overview" && (<>
          {/* KPIs */}
          <div className="grid-kpi">
            {[
              { lbl: "Fleet Health", val: `${avgHealth}%`, sub: `avg across ${ATM_DATA.length} ATMs`, col: hc(avgHealth) },
              { lbl: "Active", val: `${ATM_DATA.length - offlineCount}/${ATM_DATA.length}`, sub: `${offlineCount} offline`, col: offlineCount > 0 ? "#ef4444" : "#22c55e" },
              { lbl: "Alerts", val: ALERTS.filter(a => a.time !== "Yesterday").length, sub: `${critCount} critical`, col: critCount > 0 ? "#ef4444" : "#f59e0b" },
              { lbl: "Txns / 24h", val: totalTxns.toLocaleString(), sub: "all active ATMs", col: "#e5e5e5" },
              { lbl: "Cash Alerts", val: ATM_DATA.filter(a => a.cashLevel < 30).length, sub: "below 30% threshold", col: ATM_DATA.filter(a => a.cashLevel < 30).length > 0 ? "#ef4444" : "#22c55e" },
            ].map(k => <StatBox key={k.lbl} label={k.lbl} value={k.val} sub={k.sub} color={k.col} />)}
          </div>

          {/* Fleet grid */}
          <SectionTitle>ATM Fleet Status</SectionTitle>
          <div className="grid-fleet">
            {ATM_DATA.map(atm => (
              <div key={atm.id} onClick={() => { setSelected(atm); setTab("atms"); }}
                style={{
                  background: "#080808", border: `1px solid ${atm.health < 55 ? "#2d0a0a" : atm.health < 80 ? "#1a1400" : "#111"}`,
                  borderRadius: 8, padding: "16px 18px", cursor: "pointer", transition: "border-color 0.15s",
                  boxShadow: atm.health < 55 ? "0 0 16px rgba(239,68,68,0.08)" : "none"
                }}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 10 }}>
                  <div>
                    <div style={{ display: "flex", alignItems: "center", gap: 7, marginBottom: 3 }}>
                      <span style={{ width: 7, height: 7, borderRadius: "50%", background: sc(atm.status), display: "inline-block" }} />
                      <span style={{ fontSize: 10, color: "#444", fontFamily: "'IBM Plex Mono',monospace" }}>{atm.id}</span>
                      {atm.alerts.length > 0 && <span style={{ fontSize: 9, color: "#ef4444", fontFamily: "'IBM Plex Mono',monospace" }}>▲ {atm.alerts.length}</span>}
                    </div>
                    <div style={{ fontSize: 13, fontWeight: 600, color: "#d4d4d4" }}>{atm.short}</div>
                    <div style={{ fontSize: 11, color: "#60a5fa", marginTop: 2 }}>{atm.location}</div>
                    <div style={{ fontSize: 10, color: "#444", marginTop: 1 }}>{atm.model}</div>
                  </div>
                  <div style={{ textAlign: "right" }}>
                    <div style={{ fontSize: 26, fontWeight: 700, color: hc(atm.health), fontFamily: "'IBM Plex Mono',monospace", lineHeight: 1 }}>{atm.health}</div>
                    <div style={{ fontSize: 9, color: "#333", marginTop: 2 }}>{hl(atm.health)}</div>
                  </div>
                </div>

                <HealthBar value={atm.health} />

                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 6, marginTop: 10 }}>
                  {[
                    { lbl: "Uptime", v: `${atm.uptime}%`, c: atm.uptime > 95 ? "#22c55e" : atm.uptime > 88 ? "#f59e0b" : "#ef4444" },
                    { lbl: "Cash", v: `${atm.cashLevel}%`, c: cc(atm.cashLevel) },
                    { lbl: "Txns", v: atm.transactions24h, c: "#e5e5e5" },
                  ].map(m => (
                    <div key={m.lbl} style={{ background: "#050505", borderRadius: 4, padding: "5px 8px", textAlign: "center" }}>
                      <div style={{ fontSize: 12, fontWeight: 600, color: m.c, fontFamily: "'IBM Plex Mono',monospace" }}>{m.v}</div>
                      <div style={{ fontSize: 9, color: "#333" }}>{m.lbl}</div>
                    </div>
                  ))}
                </div>
                {atm.alerts.length > 0 && (
                  <div style={{ marginTop: 8, display: "flex", gap: 4, flexWrap: "wrap" }}>
                    {atm.alerts.map(a => <Pill key={a} label={a} color="#ef4444" bg="#1a0000" />)}
                  </div>
                )}
              </div>
            ))}
          </div>

          {/* Recent alerts */}
          <SectionTitle>Recent Alerts</SectionTitle>
          <div style={{ background: "#080808", border: "1px solid #111", borderRadius: 8, overflow: "hidden" }}>
            {ALERTS.slice(0, 5).map((a, i) => {
              const c = ac(a.severity);
              return (
                <div key={i} style={{
                  display: "grid", gridTemplateColumns: "64px 74px 80px 1fr", alignItems: "center", gap: 12,
                  padding: "12px 18px", borderBottom: i < 4 ? "1px solid #0d0d0d" : "none",
                  background: i === 0 ? c.bg : "transparent"
                }}>
                  <span style={{ fontSize: 10, color: "#333", fontFamily: "'IBM Plex Mono',monospace" }}>{a.time}</span>
                  <span style={{ fontSize: 11, color: "#60a5fa", fontFamily: "'IBM Plex Mono',monospace" }}>{a.atm}</span>
                  <Pill label={a.severity} color={c.text} bg={c.pill + "40"} />
                  <span style={{ fontSize: 12, color: "#999" }}>{a.msg}</span>
                </div>
              );
            })}
          </div>
        </>)}

        {/* ───── FLEET / ATM DETAIL ───── */}
        {tab === "atms" && (
          <div className={selected ? "grid-ops-split" : "grid-ops-single"}>
            {/* List */}
            <div>
              <div style={{ display: "flex", gap: 6, marginBottom: 14, flexWrap: "wrap" }}>
                <style>{`@keyframes gradBorder { 0%{background-position:0% 50%} 50%{background-position:100% 50%} 100%{background-position:0% 50%} }`}</style>
                {[
                  ["all", "All", null],
                  ["healthy", "● Healthy", "#22c55e"],
                  ["warning", "● Warning", "#f59e0b"],
                  ["critical", "● Critical", "#f97316"],
                  ["out_of_service", "● Offline", "#ef4444"],
                ].map(([f, l, c]) => (
                  <button key={f} onClick={() => setFilter(f)}
                    style={{
                      padding: f === "all" && filter === f ? "3px 9px" : "4px 10px",
                      borderRadius: 4, fontSize: 11, cursor: "pointer",
                      fontFamily: "'IBM Plex Mono',monospace", transition: "all 0.2s",
                      ...(f === "all" ? {
                        border: filter === f ? "2px solid transparent" : "1px solid #1a1a1a",
                        background: filter === f
                          ? "linear-gradient(#0a0f0a, #0a0f0a) padding-box, linear-gradient(90deg, #22c55e, #f59e0b, #f97316, #ef4444, #22c55e) border-box"
                          : "transparent",
                        backgroundSize: filter === f ? "300% 100%" : "auto",
                        animation: filter === f ? "gradBorder 4s ease infinite" : "none",
                        color: filter === f ? "#e5e5e5" : "#444",
                      } : {
                        border: `1px solid ${filter === f ? c : "#1a1a1a"}`,
                        background: filter === f ? `${c}18` : "transparent",
                        color: filter === f ? c : "#444",
                      }),
                    }}>
                    {l}
                  </button>
                ))}
              </div>
              <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                {filtered.map(atm => (
                  <div key={atm.id} onClick={() => setSelected(selected?.id === atm.id ? null : atm)}
                    style={{
                      background: selected?.id === atm.id ? "#0a1a0a" : "#080808",
                      border: `1px solid ${selected?.id === atm.id ? "#22c55e" : atm.health < 55 ? "#2d0a0a" : "#111"}`,
                      borderRadius: 8, padding: "12px 14px", cursor: "pointer", transition: "all 0.15s"
                    }}>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                      <div>
                        <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 2 }}>
                          <span style={{ width: 6, height: 6, borderRadius: "50%", background: sc(atm.status), display: "inline-block" }} />
                          <span style={{ fontSize: 10, color: "#444", fontFamily: "'IBM Plex Mono',monospace" }}>{atm.id}</span>
                        </div>
                        <div style={{ fontSize: 12, fontWeight: 600, color: "#d4d4d4" }}>{atm.short}</div>
                        <div style={{ fontSize: 10, color: "#60a5fa" }}>{atm.location}</div>
                      </div>
                      <div style={{ fontSize: 18, fontWeight: 700, color: hc(atm.health), fontFamily: "'IBM Plex Mono',monospace" }}>{atm.health}</div>
                    </div>
                    <div style={{ marginTop: 7, display: "flex", gap: 6, alignItems: "center" }}>
                      <HealthBar value={atm.health} width={null} />
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* Detail */}
            {selected && (
              <div style={{ background: "#080808", border: "1px solid #1a1a1a", borderRadius: 8, padding: "22px 24px", animation: "fadeIn 0.2s ease" }}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 20 }}>
                  <div>
                    <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 4 }}>
                      <span style={{ width: 8, height: 8, borderRadius: "50%", background: sc(selected.status), display: "inline-block" }} />
                      <span style={{ fontSize: 10, color: "#444", fontFamily: "'IBM Plex Mono',monospace" }}>{selected.id}</span>
                      <Pill label={sl(selected.status)} color={sc(selected.status)} bg={selected.status === "in_service" ? "#052e16" : "#1a0000"} />
                    </div>
                    <div style={{ fontSize: 20, fontWeight: 700, color: "#e5e5e5" }}>{selected.name}</div>
                    <div style={{ fontSize: 13, color: "#60a5fa", marginTop: 2 }}>{selected.location}</div>
                    <div style={{ fontSize: 11, color: "#444", marginTop: 2 }}>{selected.model}</div>
                  </div>
                  <button onClick={() => setSelected(null)} style={{ background: "none", border: "1px solid #1a1a1a", color: "#444", borderRadius: 6, padding: "5px 12px", cursor: "pointer", fontSize: 11, fontFamily: "'IBM Plex Mono',monospace" }}>✕</button>
                </div>

                <div style={{ display: "grid", gridTemplateColumns: "repeat(3,1fr)", gap: 10, marginBottom: 20 }}>
                  {[
                    { lbl: "Health Score", v: `${selected.health}`, unit: "%", c: hc(selected.health), sub: hl(selected.health) },
                    { lbl: "Uptime 24h", v: `${selected.uptime}`, unit: "%", c: selected.uptime > 95 ? "#22c55e" : selected.uptime > 88 ? "#f59e0b" : "#ef4444", sub: "availability" },
                    { lbl: "Cash Level", v: `${selected.cashLevel}`, unit: "%", c: cc(selected.cashLevel), sub: `~${selected.daysToDepletion}d to depletion` },
                    { lbl: "Error Rate", v: `${selected.errorRate}`, unit: "%", c: selected.errorRate > 8 ? "#ef4444" : selected.errorRate > 4 ? "#f59e0b" : "#22c55e", sub: `${selected.errorCount7d} errors / 7d` },
                    { lbl: "Txns 24h", v: selected.transactions24h, unit: "", c: "#e5e5e5", sub: `${selected.transactionVelocity}/hr velocity` },
                    { lbl: "Failure Risk", v: `${Math.round(selected.failureProbability * 10000) / 100}`, unit: "%", c: selected.failureProbability > 0.5 ? "#ef4444" : selected.failureProbability > 0.2 ? "#f59e0b" : "#22c55e", sub: "7-day prediction" },
                  ].map(m => (
                    <div key={m.lbl} style={{ background: "#050505", border: "1px solid #111", borderRadius: 6, padding: "12px 14px" }}>
                      <div style={{ fontSize: 10, color: "#444", marginBottom: 4 }}>{m.lbl}</div>
                      <div style={{ fontSize: 20, fontWeight: 700, color: m.c, fontFamily: "'IBM Plex Mono',monospace" }}>{m.v}<span style={{ fontSize: 12, marginLeft: 2 }}>{m.unit}</span></div>
                      <div style={{ fontSize: 10, color: "#333", marginTop: 2 }}>{m.sub}</div>
                    </div>
                  ))}
                </div>

                {/* 7-day txn trend */}
                <div style={{ marginBottom: 16 }}>
                  <SectionTitle>7-Day Transaction Volume</SectionTitle>
                  <div style={{ background: "#050505", border: "1px solid #111", borderRadius: 6, padding: "14px 16px" }}>
                    <MiniSparkline data={selected.weeklyTxns} color="#22c55e" h={56} />
                    <div style={{ display: "flex", justifyContent: "space-between", marginTop: 6 }}>
                      {["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"].map(d => <span key={d} style={{ fontSize: 9, color: "#333", fontFamily: "'IBM Plex Mono',monospace" }}>{d}</span>)}
                    </div>
                  </div>
                </div>

                {/* Uptime trend + card breakdown */}
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginBottom: 16 }}>
                  <div style={{ background: "#050505", border: "1px solid #111", borderRadius: 6, padding: "14px 16px" }}>
                    <SectionTitle>Uptime 7d</SectionTitle>
                    <div style={{ display: "flex", gap: 4, alignItems: "flex-end" }}>
                      {selected.uptime_7d.map((v, i) => (
                        <div key={i} style={{ flex: 1, display: "flex", flexDirection: "column", alignItems: "center", gap: 4 }}>
                          <div style={{ fontSize: 9, color: v > 95 ? "#22c55e" : v > 88 ? "#f59e0b" : "#ef4444", fontFamily: "'IBM Plex Mono',monospace" }}>{v}</div>
                          <div style={{ width: "100%", height: 4, borderRadius: 2, background: v > 95 ? "#22c55e" : v > 88 ? "#f59e0b" : "#ef4444" }} />
                        </div>
                      ))}
                    </div>
                  </div>
                  <div style={{ background: "#050505", border: "1px solid #111", borderRadius: 6, padding: "14px 16px" }}>
                    <SectionTitle>Card Type Mix</SectionTitle>
                    {Object.entries(selected.cardBreakdown).filter(([, v]) => v > 0).map(([k, v]) => (
                      <div key={k} style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 5 }}>
                        <span style={{ fontSize: 10, color: "#444", fontFamily: "'IBM Plex Mono',monospace", width: 72 }}>{k}</span>
                        <div style={{ flex: 1, height: 5, background: "#111", borderRadius: 2 }}>
                          <div style={{ width: `${v}%`, height: "100%", background: "#22c55e", borderRadius: 2, opacity: 0.7 }} />
                        </div>
                        <span style={{ fontSize: 10, color: "#555", fontFamily: "'IBM Plex Mono',monospace", width: 24, textAlign: "right" }}>{v}%</span>
                      </div>
                    ))}
                  </div>
                </div>

                {/* Services + maintenance */}
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
                  <div style={{ background: "#050505", border: "1px solid #111", borderRadius: 6, padding: "14px 16px" }}>
                    <SectionTitle>Services</SectionTitle>
                    <div style={{ display: "flex", flexWrap: "wrap", gap: 5 }}>
                      {selected.services.map(s => <Pill key={s} label={s} color="#60a5fa" bg="#0c1a30" />)}
                    </div>
                  </div>
                  <div style={{ background: "#050505", border: "1px solid #111", borderRadius: 6, padding: "14px 16px" }}>
                    <SectionTitle>Maintenance</SectionTitle>
                    <div style={{ fontSize: 12, color: "#555", lineHeight: 1.9 }}>
                      <div>Last: <span style={{ color: selected.daysSinceMaintenance > 60 ? "#ef4444" : selected.daysSinceMaintenance > 30 ? "#f59e0b" : "#e5e5e5" }}>{selected.lastMaintenance}</span></div>
                      <div>Days ago: <span style={{ color: selected.daysSinceMaintenance > 60 ? "#ef4444" : "#e5e5e5" }}>{selected.daysSinceMaintenance}</span></div>
                      <div>Type: <span style={{ color: "#e5e5e5" }}>{selected.corrective ? "Corrective" : "Preventive"}</span></div>
                    </div>
                  </div>
                </div>

                {selected.alerts.length > 0 && (
                  <div style={{ marginTop: 12, background: "#1a0000", border: "1px solid #7f1d1d", borderRadius: 6, padding: "12px 16px" }}>
                    <div style={{ fontSize: 9, color: "#ef4444", letterSpacing: 2, fontFamily: "'IBM Plex Mono',monospace", marginBottom: 8 }}>ACTIVE ALERTS</div>
                    <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
                      {selected.alerts.map(a => <Pill key={a} label={a} color="#ef4444" bg="#450a0a" />)}
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>
        )}

        {/* ───── ALERTS ───── */}
        {tab === "alerts" && (<>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: 10, marginBottom: 20 }}>
            {[["CRITICAL", "#ef4444", ALERTS.filter(a => a.severity === "CRITICAL").length],
            ["HIGH", "#f97316", ALERTS.filter(a => a.severity === "HIGH").length],
            ["MEDIUM", "#f59e0b", ALERTS.filter(a => a.severity === "MEDIUM").length],
            ["LOW", "#22c55e", ALERTS.filter(a => a.severity === "LOW").length],
            ].map(([sev, col, cnt]) => (
              <div key={sev} style={{ background: "#080808", border: `1px solid ${cnt > 0 ? col + "33" : "#111"}`, borderRadius: 8, padding: "14px 18px", textAlign: "center" }}>
                <div style={{ fontSize: 26, fontWeight: 700, color: cnt > 0 ? col : "#333", fontFamily: "'IBM Plex Mono',monospace" }}>{cnt}</div>
                <div style={{ fontSize: 10, color: "#444", marginTop: 4, letterSpacing: 1 }}>{sev}</div>
              </div>
            ))}
          </div>
          <div style={{ background: "#080808", border: "1px solid #111", borderRadius: 8, overflow: "hidden" }}>
            <div style={{ display: "grid", gridTemplateColumns: "64px 74px 86px 1fr", gap: 12, padding: "10px 18px", borderBottom: "1px solid #111" }}>
              {["TIME", "ATM", "SEVERITY", "MESSAGE"].map(h => <span key={h} style={{ fontSize: 9, color: "#333", fontFamily: "'IBM Plex Mono',monospace", letterSpacing: 1.5 }}>{h}</span>)}
            </div>
            {ALERTS.map((a, i) => {
              const c = ac(a.severity);
              return (
                <div key={i} style={{
                  display: "grid", gridTemplateColumns: "64px 74px 86px 1fr", gap: 12, alignItems: "center",
                  padding: "14px 18px", borderBottom: i < ALERTS.length - 1 ? "1px solid #0a0a0a" : "none",
                  background: a.severity === "CRITICAL" ? "#0d0000" : a.severity === "HIGH" ? "#0a0600" : "transparent"
                }}>
                  <span style={{ fontSize: 10, color: "#333", fontFamily: "'IBM Plex Mono',monospace" }}>{a.time}</span>
                  <span style={{ fontSize: 11, color: "#60a5fa", fontFamily: "'IBM Plex Mono',monospace" }}>{a.atm}</span>
                  <Pill label={a.severity} color={c.text} bg={c.pill + "40"} />
                  <span style={{ fontSize: 12, color: "#777" }}>{a.msg}</span>
                </div>
              );
            })}
          </div>
        </>)}

        {/* ───── MAINTENANCE ───── */}
        {tab === "maintenance" && (<>
          <div style={{ fontSize: 12, color: "#444", marginBottom: 16 }}>
            Queue prioritised by: health score + error acceleration + days since last service (Spar Nord model: refill frequency optimisation).
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
            {MAINTENANCE_QUEUE.map((item, i) => {
              const atm = ATM_DATA.find(a => a.id === item.id);
              const c = ac(item.priority);
              return (
                <div key={i} style={{
                  background: "#080808", border: `1px solid ${c.border}22`, borderRadius: 8, padding: "20px 22px",
                  boxShadow: item.priority === "CRITICAL" ? "0 0 20px rgba(239,68,68,0.06)" : "none"
                }}>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
                    <div style={{ flex: 1 }}>
                      <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 8 }}>
                        <Pill label={item.priority} color={c.text} bg={c.pill + "50"} />
                        <span style={{ fontSize: 14, fontWeight: 700, color: "#e5e5e5", fontFamily: "'IBM Plex Mono',monospace" }}>{item.id}</span>
                        <span style={{ fontSize: 13, color: "#555" }}>— {atm?.name}</span>
                      </div>
                      <div style={{ fontSize: 13, color: "#666", marginBottom: 12, lineHeight: 1.5 }}>{item.issue}</div>
                      {atm && (
                        <div style={{ display: "flex", gap: 16, flexWrap: "wrap" }}>
                          {[
                            [`Health: ${atm.health}%`, hc(atm.health)],
                            [`Uptime: ${atm.uptime}%`, atm.uptime > 95 ? "#22c55e" : "#f59e0b"],
                            [`Cash: ${atm.cashLevel}%`, cc(atm.cashLevel)],
                            [`Error rate: ${atm.errorRate}%`, atm.errorRate > 8 ? "#ef4444" : "#f59e0b"],
                            [`Last service: ${atm.daysSinceMaintenance}d ago`, atm.daysSinceMaintenance > 60 ? "#ef4444" : "#555"],
                            [`Failure risk: ${Math.round(atm.failureProbability * 100)}%`, atm.failureProbability > 0.5 ? "#ef4444" : "#555"],
                          ].map(([t, col]) => <span key={t} style={{ fontSize: 11, color: col, fontFamily: "'IBM Plex Mono',monospace" }}>{t}</span>)}
                        </div>
                      )}
                    </div>
                    <div style={{ textAlign: "right", marginLeft: 24, flexShrink: 0 }}>
                      <div style={{ fontSize: 10, color: "#333", marginBottom: 4, fontFamily: "'IBM Plex Mono',monospace" }}>SCHEDULED</div>
                      <div style={{ fontSize: 16, fontWeight: 700, color: item.eta === "Today" ? "#ef4444" : item.eta === "Tomorrow" ? "#f59e0b" : "#e5e5e5", fontFamily: "'IBM Plex Mono',monospace" }}>{item.eta}</div>
                      <div style={{ fontSize: 11, color: "#333", marginTop: 8 }}>Tech</div>
                      <div style={{ fontSize: 12, color: item.tech === "Unassigned" ? "#f59e0b" : "#555", fontFamily: "'IBM Plex Mono',monospace" }}>{item.tech}</div>
                      <div style={{ fontSize: 10, color: "#333", marginTop: 4 }}>{item.estimatedDuration}</div>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </>)}

        {/* ───── DATA MANAGEMENT ───── */}
        {tab === "data" && <DataManagementView token={token} />}
        {tab === "analytics" && (<>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 16 }}>

            {/* Health distribution */}
            <div style={{ background: "#080808", border: "1px solid #111", borderRadius: 8, padding: "20px 22px" }}>
              <SectionTitle>Health Score — All ATMs</SectionTitle>
              {ATM_DATA.map(atm => (
                <div key={atm.id} style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 10 }}>
                  <span style={{ fontSize: 10, color: "#444", fontFamily: "'IBM Plex Mono',monospace", width: 66 }}>{atm.id}</span>
                  <span style={{ fontSize: 10, color: "#333", width: 100 }}>{atm.short}</span>
                  <div style={{ flex: 1, height: 6, background: "#0d0d0d", borderRadius: 3 }}>
                    <div style={{ width: `${atm.health}%`, height: "100%", background: hc(atm.health), borderRadius: 3, transition: "width 1s" }} />
                  </div>
                  <span style={{ fontSize: 12, fontWeight: 700, color: hc(atm.health), fontFamily: "'IBM Plex Mono',monospace", width: 32, textAlign: "right" }}>{atm.health}</span>
                  <span style={{ fontSize: 9, color: atm.healthTrend > 0 ? "#22c55e" : "#ef4444", width: 36, textAlign: "right", fontFamily: "'IBM Plex Mono',monospace" }}>
                    {atm.healthTrend > 0 ? "+" : ""}{atm.healthTrend}%
                  </span>
                </div>
              ))}
            </div>

            {/* Cash depletion forecast */}
            <div style={{ background: "#080808", border: "1px solid #111", borderRadius: 8, padding: "20px 22px" }}>
              <SectionTitle>Cash Depletion Forecast</SectionTitle>
              {[...ATM_DATA].sort((a, b) => a.daysToDepletion - b.daysToDepletion).map(atm => (
                <div key={atm.id} style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 10 }}>
                  <span style={{ fontSize: 10, color: "#444", fontFamily: "'IBM Plex Mono',monospace", width: 66 }}>{atm.id}</span>
                  <div style={{ flex: 1, height: 6, background: "#0d0d0d", borderRadius: 3 }}>
                    <div style={{ width: `${Math.min(100, atm.daysToDepletion / 14 * 100)}%`, height: "100%", background: cc(atm.cashLevel), borderRadius: 3 }} />
                  </div>
                  <span style={{ fontSize: 11, color: cc(atm.cashLevel), fontFamily: "'IBM Plex Mono',monospace", width: 48, textAlign: "right" }}>{atm.daysToDepletion}d</span>
                  <span style={{ fontSize: 10, color: "#333", width: 36, textAlign: "right" }}>{atm.cashLevel}%</span>
                </div>
              ))}
            </div>
          </div>

          {/* Monthly inactive tx (Spar Nord insight) */}
          <div style={{ background: "#080808", border: "1px solid #111", borderRadius: 8, padding: "20px 22px", marginBottom: 16 }}>
            <SectionTitle>Monthly Inactive Transaction Rate — {ATM_DATA[0]?.monthlyLabels?.join(" / ") || "Jan / Feb / Mar"} (Spar Nord Pattern)</SectionTitle>
            <div className="grid-sparklines">
              {ATM_DATA.map(atm => (
                <div key={atm.id} style={{ background: "#050505", borderRadius: 6, padding: "12px" }}>
                  <div style={{ fontSize: 10, color: "#60a5fa", fontFamily: "'IBM Plex Mono',monospace", marginBottom: 8 }}>{atm.id}</div>
                  <MiniSparkline data={atm.monthlyInactive} color={atm.monthlyInactive[2] > 20 ? "#ef4444" : "#f59e0b"} h={40} />
                  <div style={{ display: "flex", justifyContent: "space-between", marginTop: 4 }}>
                    {(atm.monthlyLabels || ["J", "F", "M"]).map(m => <span key={m} style={{ fontSize: 9, color: "#333", fontFamily: "'IBM Plex Mono',monospace" }}>{m[0]}</span>)}
                  </div>
                  <div style={{ fontSize: 11, color: atm.monthlyInactive[2] > 20 ? "#ef4444" : "#555", marginTop: 6, fontFamily: "'IBM Plex Mono',monospace" }}>
                    {atm.monthlyInactive[2]} inactive
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Transaction volume per ATM */}
          <div style={{ background: "#080808", border: "1px solid #111", borderRadius: 8, padding: "20px 22px" }}>
            <SectionTitle>7-Day Transaction Volume — All ATMs (Sat spike reflects Spar Nord pattern)</SectionTitle>
            <div className="grid-sparklines">
              {ATM_DATA.map(atm => (
                <div key={atm.id} style={{ background: "#050505", borderRadius: 6, padding: "12px" }}>
                  <div style={{ fontSize: 10, color: "#60a5fa", fontFamily: "'IBM Plex Mono',monospace", marginBottom: 8 }}>{atm.id}</div>
                  <MiniSparkline data={atm.weeklyTxns} color="#22c55e" h={48} />
                  <div style={{ display: "flex", justifyContent: "space-between", marginTop: 4 }}>
                    {(atm.weeklyLabels || ["M", "T", "W", "T", "F", "S", "S"]).map((d, i) => (
                      <span key={i} style={{ fontSize: 8, color: d === "Sat" ? "#22c55e" : "#333", fontFamily: "'IBM Plex Mono',monospace" }}>{d[0]}</span>
                    ))}
                  </div>
                  <div style={{ fontSize: 11, color: "#555", marginTop: 5, fontFamily: "'IBM Plex Mono',monospace" }}>{atm.transactions24h} today</div>
                </div>
              ))}
            </div>
          </div>
        </>)}

      </div>
    </div>
  );
};

// ── DATA MANAGEMENT VIEW ───────────────────────────────────────────────────
const DataManagementView = ({ token }) => {
  const [file, setFile] = useState(null);
  const [status, setStatus] = useState("idle"); // idle, uploading, success, error
  const [message, setMessage] = useState("");
  const [retrainingResult, setRetrainingResult] = useState(null);

  const handleFileChange = (e) => {
    if (e.target.files && e.target.files[0]) {
      setFile(e.target.files[0]);
      setStatus("idle");
    }
  };

  const onUpload = async () => {
    if (!file) return;
    setStatus("uploading");
    setMessage("Uploading metrics and initiating intelligent imputation...");

    const formData = new FormData();
    formData.append("file", file);

    try {
      // Point to backend Flask server
      const response = await fetch("/api/v1/data/upload/metrics", {
        method: "POST",
        headers: { 'Authorization': `Bearer ${token}` },
        body: formData,
      });
      const data = await response.json();

      if (response.ok) {
        setStatus("success");
        setMessage("Success! Data ingested and models retrained.");
        setRetrainingResult(data);
        if (window.triggerFleetRefresh) window.triggerFleetRefresh();
      } else {
        setStatus("error");
        setMessage(data.error || "Upload failed. Please check the file schema.");
      }
    } catch (err) {
      setStatus("error");
      setMessage("Error connecting to AIAP backend. Is the server running?");
    }
  };

  const DATASET_SCHEMAS = [
    {
      type: "atm_metadata", color: "#60a5fa", cols: [
        { name: "atm_id", desc: "Unique ATM identifier (NCB0001)" },
        { name: "atm_bank", desc: "Bank name" },
        { name: "location", desc: "Campus location" },
        { name: "atm_model", desc: "Hardware model" },
      ]
    },
    {
      type: "cash_status", color: "#22c55e", cols: [
        { name: "cash_id", desc: "Record identifier" },
        { name: "atm_id", desc: "ATM reference" },
        { name: "timestamp", desc: "Date (YYYY-MM-DD)" },
        { name: "remaining_cash", desc: "Cash remaining (JMD)" },
      ]
    },
    {
      type: "transactions", color: "#f59e0b", cols: [
        { name: "transaction_id", desc: "Transaction identifier" },
        { name: "atm_id", desc: "ATM reference" },
        { name: "transaction_time", desc: "Timestamp (YYYY-MM-DD HH:MM:SS)" },
        { name: "withdrawal_amount", desc: "Amount withdrawn (JMD)" },
        { name: "transaction_status", desc: "Success (1) or failure (0)" },
      ]
    },
    {
      type: "operational_logs", color: "#f97316", cols: [
        { name: "log_id", desc: "Log entry identifier" },
        { name: "atm_id", desc: "ATM reference" },
        { name: "timestamp", desc: "Timestamp (YYYY-MM-DD HH:MM:SS)" },
        { name: "uptime_status", desc: "Online (1) or offline (0)" },
        { name: "error_code", desc: "Error code if applicable" },
        { name: "downtime_duration", desc: "Minutes of downtime" },
      ]
    },
    {
      type: "maintenance_records", color: "#a78bfa", cols: [
        { name: "maintenance_id", desc: "Record identifier" },
        { name: "atm_id", desc: "ATM reference" },
        { name: "maintenance_date", desc: "Timestamp (YYYY-MM-DD HH:MM:SS)" },
        { name: "maintenance_type", desc: "Preventive or Corrective" },
        { name: "amount_added", desc: "Cash replenished (JMD)" },
      ]
    },
  ];

  return (
    <div style={{ animation: "fadeIn 0.3s ease" }}>
      <div className="grid-data-split">
        <div>
          <SectionTitle>Upload Training Data</SectionTitle>
          <div style={{ background: "#080808", border: "1px dashed #222", borderRadius: 12, padding: "40px", textAlign: "center", marginBottom: 16 }}>
            <div style={{ fontSize: 40, color: "#22c55e", marginBottom: 16 }}>⬘</div>
            <div style={{ fontSize: 16, fontWeight: 700, color: "#e5e5e5", marginBottom: 8 }}>Metrics Ingestion Engine</div>
            <div style={{ fontSize: 13, color: "#555", marginBottom: 24, maxWidth: 400, margin: "0 auto" }}>
              Upload any of the 5 recognised CSV types. The engine auto-detects the schema and routes data accordingly.
            </div>

            <input type="file" accept=".csv" onChange={handleFileChange} id="csv-upload" style={{ display: "none" }} />
            <div style={{ display: "flex", justifyContent: "center", gap: 12 }}>
              <label htmlFor="csv-upload" style={{
                background: "#111", border: "1px solid #222", borderRadius: 6, padding: "10px 20px",
                color: "#e5e5e5", fontSize: 12, fontWeight: 600, cursor: "pointer", transition: "all 0.2s"
              }}>
                {file ? "Change File" : "Select CSV"}
              </label>
              {file && (
                <button onClick={onUpload} disabled={status === "uploading"} style={{
                  background: "#052e16", border: "1px solid #14532d", borderRadius: 6, padding: "10px 24px",
                  color: "#22c55e", fontSize: 12, fontWeight: 700, cursor: "pointer"
                }}>
                  {status === "uploading" ? "Syncing..." : "Process & Train →"}
                </button>
              )}
            </div>
            {file && <div style={{ marginTop: 12, fontSize: 11, color: "#333", fontFamily: "'IBM Plex Mono',monospace" }}>Selected: {file.name}</div>}
          </div>

          {(status === "success" || status === "error" || status === "uploading") && (
            <div style={{
              background: status === "error" ? "#1a0000" : "#050505",
              border: `1px solid ${status === "error" ? "#450a0a" : status === "success" ? "#14532d" : "#222"}`,
              borderRadius: 8, padding: "20px", marginBottom: 16
            }}>
              <div style={{ display: "flex", gap: 12, alignItems: "center" }}>
                <div style={{ fontSize: 18, color: status === "error" ? "#ef4444" : "#22c55e", animation: status === "uploading" ? "blink 1s infinite" : "none" }}>
                  {status === "success" ? "✓" : status === "error" ? "✕" : "⚑"}
                </div>
                <div>
                  <div style={{ fontSize: 13, fontWeight: 700, color: "#e5e5e5" }}>{status === "uploading" ? "Processing Engine Active" : status === "success" ? "Ingestion Complete" : "Ingestion Failed"}</div>
                  <div style={{ fontSize: 12, color: "#555", marginTop: 2 }}>{message}</div>
                </div>
              </div>
              {retrainingResult && (
                <div style={{ marginTop: 16, paddingTop: 16, borderTop: "1px solid #111", display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
                  <StatBox label="Rows Processed" value={retrainingResult.rows_processed} sub="Successfully imputed" color="#22c55e" />
                  <StatBox label="Pipeline Status" value={retrainingResult.status.toUpperCase()} sub="Training complete" color="#60a5fa" />
                </div>
              )}
            </div>
          )}
        </div>

        <div>
          <SectionTitle>Accepted CSV Schemas</SectionTitle>
          <div style={{ background: "#050505", border: "1px solid #111", borderRadius: 8, padding: "16px", maxHeight: 520, overflowY: "auto" }}>
            <div style={{ fontSize: 11, color: "#444", marginBottom: 14, lineHeight: 1.5 }}>
              Upload any of the following dataset types. The engine auto-detects the format from column names.
            </div>
            {DATASET_SCHEMAS.map(ds => (
              <div key={ds.type} style={{ marginBottom: 14, borderBottom: "1px solid #0d0d0d", paddingBottom: 10 }}>
                <div style={{ fontSize: 11, fontWeight: 700, color: ds.color, fontFamily: "'IBM Plex Mono',monospace", marginBottom: 6, letterSpacing: 0.5 }}>
                  {ds.type}.csv
                </div>
                {ds.cols.map(c => (
                  <div key={c.name} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 3, paddingLeft: 8 }}>
                    <span style={{ fontSize: 10, color: "#888", fontFamily: "'IBM Plex Mono',monospace" }}>{c.name}</span>
                    <span style={{ fontSize: 9, color: "#444", textAlign: "right" }}>{c.desc}</span>
                  </div>
                ))}
              </div>
            ))}
            <div style={{ marginTop: 8, padding: "10px", background: "#080808", borderRadius: 4, fontSize: 10, color: "#333" }}>
              Auto-detection matches column signatures. New data is appended to existing datasets.
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};


// ── CUSTOMER INTERFACE ────────────────────────────────────────────────────────
const CustomerView = ({ onLogout }) => {
  const [atms, setAtms] = useState([]);
  const [loading, setLoading] = useState(true);
  const [selected, setSelected] = useState(null);
  const [filterService, setFilterService] = useState("All");
  const [search, setSearch] = useState("");
  const [showUnavailable, setShowUnavailable] = useState(true);

  useEffect(() => {
    fetch('/api/v1/public/atms')
      .then(r => r.json())
      .then(data => {
        const mapped = data.map(a => ({
          id: a.id,
          name: a.short || a.name,
          location: a.name || a.building,
          building: a.building || a.name,
          short: a.short || a.name,
          status: a.status || "in_service",
          health: Math.round((a.health != null ? a.health : 75) * 100) / 100,
          cashLevel: Math.round((a.cash_level != null ? a.cash_level : 70) * 100) / 100,
          transactions24h: a.transactions_24h || 0,
          services: typeof a.services === 'string' && a.services.length > 0
            ? a.services.split(',').map(s => s.trim())
            : ["Withdrawal", "Deposits", "Balance Check", "Bill Payments", "Transfer"],
          card_types: typeof a.card_types === 'string' && a.card_types.length > 0
            ? a.card_types.split(',').map(s => s.trim())
            : ["Visa", "Mastercard"],
          uptime: Math.round((a.uptime != null ? a.uptime : 95) * 100) / 100,
          daysToDepletion: Math.round(a.days_to_depletion != null ? a.days_to_depletion : 7),
          hourlyTxns: a.historical_series?.hourly_txns || [0, 0, 0, 0, 1, 5, 10, 20, 30, 40, 30, 25, 20, 20, 25, 30, 20, 15, 10, 5, 2, 1, 0, 0],
        }));
        setAtms(mapped);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, []);

  const ALL_SERVICES = ["All", "Withdrawal", "Deposits", "Balance Check", "Bill Payments", "Transfer"];

  const activityLevel = (txns) => txns > 220 ? "High" : txns > 100 ? "Moderate" : txns > 0 ? "Low" : "—";
  const activityColor = (txns) => txns > 220 ? "#ef4444" : txns > 100 ? "#f59e0b" : txns > 0 ? "#22c55e" : "#444";
  const activityDesc = (txns) => txns > 220 ? "Expect queues" : txns > 100 ? "Some wait expected" : txns > 0 ? "Usually available immediately" : "Machine offline";

  const waitTime = (txns) => txns > 220 ? "5–10 min wait" : txns > 100 ? "2–5 min wait" : txns > 0 ? "No wait" : "Unavailable";

  if (loading) return <div style={{ color: "#22c55e", background: "#f9fafb", height: "100vh", display: "flex", alignItems: "center", justifyContent: "center", fontFamily: "'DM Sans',sans-serif", fontSize: 16 }}>Loading ATM data...</div>;

  const filtered = atms
    .filter(a => showUnavailable || a.status === "in_service")
    .filter(a => filterService === "All" || a.services.includes(filterService))
    .filter(a => search === "" || a.name.toLowerCase().includes(search.toLowerCase()) || a.building.toLowerCase().includes(search.toLowerCase()));

  const now = new Date();
  const hour = now.getHours();
  const isPeak = (hour >= 10 && hour <= 12) || (hour >= 15 && hour <= 17);

  return (
    <div style={{ minHeight: "100vh", background: "#f9fafb", fontFamily: "'DM Sans',sans-serif", color: "#111" }}>
      <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=DM+Sans:wght@300;400;500;600;700&display=swap" rel="stylesheet" />
      <style>{`* { box-sizing:border-box; } input:focus { outline:none; } @keyframes fadeIn { from{opacity:0;transform:translateY(6px)} to{opacity:1;transform:translateY(0)} }`}</style>

      {/* Header */}
      <div style={{ background: "rgba(0, 0, 0, 0.8)", backdropFilter: "blur(12px)", padding: "0 24px", height: 52, display: "flex", alignItems: "center", gap: 14, position: "sticky", top: 0, zIndex: 100, borderBottom: "1px solid rgba(255,255,255,0.05)" }}>
        <span style={{ fontSize: 15, color: "#22c55e" }}>◈</span>
        <span style={{ fontFamily: "'IBM Plex Mono',monospace", fontSize: 13, fontWeight: 600, color: "#e5e5e5", letterSpacing: 2 }}>AIAP</span>
        <span style={{ fontSize: 11, color: "#444" }}>ATM Finder · UWI Mona</span>
        <div style={{ flex: 1 }} />
        {isPeak && (
          <div style={{ background: "#1a0a00", border: "1px solid #f97316", borderRadius: 4, padding: "3px 10px", fontSize: 11, color: "#f97316", fontFamily: "'IBM Plex Mono',monospace" }}>
            ⚡ Peak Hours
          </div>
        )}
        <span style={{ fontSize: 12, color: "#444", cursor: "pointer" }} onClick={onLogout}>← sign out</span>
      </div>

      <div style={{ maxWidth: 960, margin: "0 auto", padding: "28px 24px" }}>
        {/* Page header */}
        <div style={{ marginBottom: 20 }}>
          <h1 style={{ fontSize: 26, fontWeight: 700, color: "#111", margin: "0 0 4px", letterSpacing: -0.5 }}>Find an ATM</h1>
          <p style={{ fontSize: 13, color: "#666", margin: 0 }}>
            {filtered.filter(a => a.status === "in_service").length} ATMs available on campus right now
            {isPeak && <span style={{ marginLeft: 8, color: "#f97316", fontWeight: 600 }}>· Peak hours — expect queues</span>}
          </p>
        </div>

        {/* Search + filters */}
        <div style={{ display: "flex", gap: 10, marginBottom: 20, flexWrap: "wrap", alignItems: "center" }}>
          <input value={search} onChange={e => setSearch(e.target.value)} placeholder="Search by name or building…"
            style={{
              flex: "1 1 220px", padding: "9px 14px", background: "#fff", border: "1px solid #e5e5e5", borderRadius: 8,
              fontSize: 13, color: "#111", minWidth: 180
            }} />
          <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
            {ALL_SERVICES.map(s => (
              <button key={s} onClick={() => setFilterService(s)}
                style={{
                  padding: "7px 12px", borderRadius: 6, border: `1px solid ${filterService === s ? "#111" : "#e5e5e5"}`,
                  background: filterService === s ? "#111" : "#fff", color: filterService === s ? "#fff" : "#666",
                  fontSize: 11, cursor: "pointer", fontWeight: 500, transition: "all 0.15s"
                }}>
                {s}
              </button>
            ))}
          </div>
          <label style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 12, color: "#888", cursor: "pointer", whiteSpace: "nowrap" }}>
            <input type="checkbox" checked={showUnavailable} onChange={e => setShowUnavailable(e.target.checked)} style={{ accentColor: "#111" }} />
            Show offline ATMs
          </label>
        </div>

        {/* ATM Cards Grid */}
        <div className={selected ? "grid-customer-split" : "grid-customer-single"}>
          <div className="grid-customer-cards">
            {filtered.map(atm => {
              const isSelected = selected?.id === atm.id;
              const isOffline = atm.status !== "in_service";
              return (
                <div key={atm.id} onClick={() => setSelected(isSelected ? null : atm)}
                  style={{
                    background: "#fff", border: `1.5px solid ${isSelected ? "#111" : isOffline ? "#fee2e2" : "#f0f0f0"}`,
                    borderRadius: 12, padding: "18px 20px", cursor: "pointer",
                    boxShadow: isSelected ? "0 4px 20px rgba(0,0,0,0.1)" : "0 1px 4px rgba(0,0,0,0.04)",
                    opacity: isOffline ? 0.7 : 1, transition: "all 0.15s",
                    animation: "fadeIn 0.25s ease"
                  }}>

                  {/* Status + Name */}
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 10 }}>
                    <div style={{ flex: 1 }}>
                      <div style={{ display: "flex", alignItems: "center", gap: 7, marginBottom: 4 }}>
                        <span style={{ width: 8, height: 8, borderRadius: "50%", background: sc(atm.status), display: "inline-block", flexShrink: 0 }} />
                        <span style={{ fontSize: 11, fontWeight: 600, color: isOffline ? "#dc2626" : "#16a34a" }}>
                          {isOffline ? "Unavailable" : "Available"}
                        </span>
                        {!isOffline && atm.cashLevel < 30 && (
                          <span style={{ fontSize: 10, background: "#fef3c7", color: "#92400e", padding: "1px 6px", borderRadius: 3, fontWeight: 600 }}>Low Cash</span>
                        )}
                      </div>
                      <div style={{ fontSize: 14, fontWeight: 700, color: "#111", lineHeight: 1.2 }}>{atm.name}</div>
                      <div style={{ fontSize: 11, color: "#888", marginTop: 3 }}>📍 {atm.building}</div>
                    </div>
                    {!isOffline && (
                      <div style={{ textAlign: "right", flexShrink: 0, marginLeft: 10 }}>
                        <div style={{ fontSize: 12, fontWeight: 700, color: activityColor(atm.transactions24h) }}>{activityLevel(atm.transactions24h)}</div>
                        <div style={{ fontSize: 10, color: "#aaa" }}>activity</div>
                      </div>
                    )}
                  </div>

                  {/* Activity bar */}
                  {!isOffline && (
                    <div style={{ marginBottom: 12 }}>
                      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}>
                        <span style={{ fontSize: 10, color: "#aaa" }}>Current activity</span>
                        <span style={{ fontSize: 10, color: activityColor(atm.transactions24h), fontWeight: 600 }}>{waitTime(atm.transactions24h)}</span>
                      </div>
                      <div style={{ height: 5, background: "#f3f4f6", borderRadius: 3 }}>
                        <div style={{ width: `${Math.min(100, atm.transactions24h / 400 * 100)}%`, height: "100%", background: activityColor(atm.transactions24h), borderRadius: 3, transition: "width 0.8s" }} />
                      </div>
                    </div>
                  )}

                  {/* Services chips */}
                  <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
                    {atm.services.slice(0, 3).map(s => (
                      <span key={s} style={{ fontSize: 10, background: "#f9fafb", color: "#555", border: "1px solid #e5e5e5", padding: "2px 8px", borderRadius: 4, fontWeight: 500 }}>{s}</span>
                    ))}
                    {atm.services.length > 3 && <span style={{ fontSize: 10, color: "#aaa" }}>+{atm.services.length - 3}</span>}
                  </div>
                </div>
              );
            })}
          </div>

          {/* Detail Panel */}
          {selected && (
            <div style={{ background: "#fff", border: "1.5px solid #111", borderRadius: 12, padding: "22px 22px", boxShadow: "0 8px 30px rgba(0,0,0,0.12)", position: "sticky", top: 20, animation: "fadeIn 0.2s ease" }}>
              <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 16 }}>
                <div style={{ fontSize: 16, fontWeight: 700, color: "#111", flex: 1, lineHeight: 1.2 }}>{selected.name}</div>
                <button onClick={() => setSelected(null)} style={{ background: "none", border: "1px solid #e5e5e5", borderRadius: 6, padding: "4px 10px", cursor: "pointer", fontSize: 11, color: "#888", marginLeft: 8 }}>✕</button>
              </div>

              {/* Status banner */}
              <div style={{
                borderRadius: 8, padding: "12px 14px", marginBottom: 16,
                background: selected.status === "out_of_service" ? "#fef2f2" : "#f0fdf4",
                border: `1px solid ${selected.status === "out_of_service" ? "#fecaca" : "#bbf7d0"}`
              }}>
                <div style={{ fontSize: 13, fontWeight: 700, color: selected.status === "out_of_service" ? "#dc2626" : "#15803d" }}>
                  {selected.status === "out_of_service" ? "❌ Currently Unavailable" : "✅ Available Now"}
                </div>
                {selected.status === "in_service" && (
                  <div style={{ fontSize: 11, color: "#166534", marginTop: 3 }}>Uptime today: {selected.uptime}%</div>
                )}
              </div>

              {/* Location */}
              <div style={{ marginBottom: 14 }}>
                <div style={{ fontSize: 10, color: "#aaa", fontWeight: 600, textTransform: "uppercase", letterSpacing: 0.8, marginBottom: 4 }}>Location</div>
                <div style={{ fontSize: 13, color: "#444" }}>📍 {selected.building}</div>
                <div style={{ fontSize: 12, color: "#999", marginTop: 2 }}>{selected.street}</div>
              </div>

              {/* Activity + cash */}
              {selected.status === "in_service" && (<>
                <div style={{ marginBottom: 14 }}>
                  <div style={{ fontSize: 10, color: "#aaa", fontWeight: 600, textTransform: "uppercase", letterSpacing: 0.8, marginBottom: 6 }}>Current Activity</div>
                  <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 6 }}>
                    <div style={{ flex: 1, height: 8, background: "#f3f4f6", borderRadius: 4 }}>
                      <div style={{ width: `${Math.min(100, selected.transactions24h / 400 * 100)}%`, height: "100%", background: activityColor(selected.transactions24h), borderRadius: 4 }} />
                    </div>
                    <span style={{ fontSize: 12, fontWeight: 700, color: activityColor(selected.transactions24h), width: 60, textAlign: "right" }}>{activityLevel(selected.transactions24h)}</span>
                  </div>
                  <div style={{ fontSize: 12, color: "#888" }}>{activityDesc(selected.transactions24h)}</div>
                  <div style={{ fontSize: 12, color: activityColor(selected.transactions24h), fontWeight: 600, marginTop: 4 }}>{waitTime(selected.transactions24h)}</div>
                </div>

                <div style={{ marginBottom: 14 }}>
                  <div style={{ fontSize: 10, color: "#aaa", fontWeight: 600, textTransform: "uppercase", letterSpacing: 0.8, marginBottom: 6 }}>Cash Status</div>
                  <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                    <div style={{ flex: 1, height: 8, background: "#f3f4f6", borderRadius: 4 }}>
                      <div style={{ width: `${selected.cashLevel}%`, height: "100%", background: cc(selected.cashLevel), borderRadius: 4 }} />
                    </div>
                    <span style={{ fontSize: 12, fontWeight: 700, color: cc(selected.cashLevel), width: 36, textAlign: "right" }}>{selected.cashLevel}%</span>
                  </div>
                  <div style={{ fontSize: 12, color: selected.cashLevel < 30 ? "#dc2626" : selected.cashLevel < 50 ? "#92400e" : "#666", marginTop: 4, fontWeight: selected.cashLevel < 30 ? 600 : 400 }}>
                    {selected.cashLevel >= 50 ? "✅ Well stocked" : selected.cashLevel >= 30 ? "⚠ Cash running low" : "⚠ Very low — may run out soon"}
                  </div>
                </div>

                {/* Hourly chart */}
                <div style={{ marginBottom: 14 }}>
                  <div style={{ fontSize: 10, color: "#aaa", fontWeight: 600, textTransform: "uppercase", letterSpacing: 0.8, marginBottom: 8 }}>Typical Activity Today</div>
                  <div style={{ background: "#f9fafb", borderRadius: 6, padding: "10px 12px" }}>
                    <MiniSparkline data={selected.hourlyTxns} color="#111" h={36} />
                    <div style={{ display: "flex", justifyContent: "space-between", marginTop: 4 }}>
                      {[0, 6, 12, 18, 23].map(h => <span key={h} style={{ fontSize: 9, color: "#ccc", fontFamily: "'IBM Plex Mono',monospace" }}>{h}:00</span>)}
                    </div>
                  </div>
                  <div style={{ fontSize: 11, color: "#999", marginTop: 4 }}>Busiest: 10am–12pm & 3pm–5pm</div>
                </div>
              </>)}

              {/* Card types */}
              <div style={{ marginBottom: 14 }}>
                <div style={{ fontSize: 10, color: "#aaa", fontWeight: 600, textTransform: "uppercase", letterSpacing: 0.8, marginBottom: 6 }}>Accepted Cards</div>
                <div style={{ display: "flex", gap: 5, flexWrap: "wrap" }}>
                  {selected.card_types.map(c => (
                    <span key={c} style={{ fontSize: 11, background: "#f3f4f6", color: "#333", border: "1px solid #e5e5e5", padding: "3px 9px", borderRadius: 4, fontWeight: 500 }}>{c}</span>
                  ))}
                </div>
              </div>

              {/* Services */}
              <div>
                <div style={{ fontSize: 10, color: "#aaa", fontWeight: 600, textTransform: "uppercase", letterSpacing: 0.8, marginBottom: 6 }}>Available Services</div>
                <div style={{ display: "flex", flexDirection: "column", gap: 5 }}>
                  {selected.services.map(s => (
                    <div key={s} style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 13, color: "#444" }}>
                      <span style={{ color: "#22c55e" }}>✓</span> {s}
                    </div>
                  ))}
                </div>
              </div>

              {selected.status === "out_of_service" && (
                <div style={{ marginTop: 16, background: "#fef2f2", border: "1px solid #fecaca", borderRadius: 6, padding: "10px 14px", fontSize: 12, color: "#dc2626" }}>
                  This ATM is currently offline. Try ATM-001 (Library) or ATM-006 (Canteen).
                </div>
              )}
            </div>
          )}
        </div>

        {/* Legend */}
        <div style={{ marginTop: 28, padding: "14px 18px", background: "#fff", border: "1px solid #f0f0f0", borderRadius: 8, display: "flex", gap: 24, flexWrap: "wrap" }}>
          <div style={{ fontSize: 11, color: "#999", fontWeight: 600, marginRight: 4 }}>ACTIVITY KEY</div>
          {[["Low", "#22c55e", "Usually no wait"], ["Moderate", "#f59e0b", "2–5 min wait"], ["High", "#ef4444", "5–10+ min wait"]].map(([l, c, d]) => (
            <div key={l} style={{ display: "flex", alignItems: "center", gap: 6 }}>
              <span style={{ width: 8, height: 8, borderRadius: "50%", background: c, display: "inline-block" }} />
              <span style={{ fontSize: 11, color: "#555", fontWeight: 600 }}>{l}</span>
              <span style={{ fontSize: 11, color: "#aaa" }}>— {d}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

// ── APP ───────────────────────────────────────────────────────────────────────
export default function App() {
  const [auth, setAuth] = useState(() => {
    // Restore session and role from localStorage
    const savedRole = localStorage.getItem('aiap_role');
    const savedName = localStorage.getItem('aiap_name');

    if (savedRole === 'customer') {
      return { role: 'customer', name: savedName || 'Campus User', token: null };
    }

    const savedToken = localStorage.getItem('aiap_token');
    if (savedRole === 'ops' && savedToken) {
      return { role: 'ops', name: savedName || 'Ops Admin', token: savedToken };
    }

    return null;
  });

  const handleLogin = (role, name, token) => {
    localStorage.setItem('aiap_role', role);
    localStorage.setItem('aiap_name', name);
    if (token) localStorage.setItem('aiap_token', token);
    setAuth({ role, name, token });
  };

  const handleLogout = () => {
    localStorage.removeItem('aiap_token');
    localStorage.removeItem('aiap_role');
    localStorage.removeItem('aiap_name');
    ATM_DATA = [];
    MAINTENANCE_QUEUE = [];
    ALERTS = [];
    setAuth(null);
  };

  // No auth → show login
  if (!auth) return <Login onLogin={handleLogin} />;

  // Customer view → public, no token needed, no DataLoader
  if (auth.role === "customer") return <CustomerView onLogout={handleLogout} />;

  // Ops view → requires token + DataLoader
  return (
    <DataLoader token={auth.token}>
      <OpsDashboard userName={auth.name} token={auth.token} onLogout={handleLogout} />
    </DataLoader>
  );
}
