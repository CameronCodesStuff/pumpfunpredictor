/* =========================================================
   SCANLINE — local pump.fun scanner
   ---------------------------------------------------------
   This connects directly to a pumpdev.io websocket feed from
   the browser. It's meant to run locally (open index.html on
   your own machine) — the key below is embedded in plain text,
   so don't deploy this file publicly with your key still in it.

   IMPORTANT HONESTY NOTE:
   pumpdev.io's exact message schema isn't fully documented
   publicly. This file normalizes several plausible field-name
   variants (see normalizeNewToken / normalizeTrade below), and
   the "show raw payloads" toggle in the footer log prints every
   message as-is. If your feed uses different key names than
   what's mapped here, check that raw output and adjust the two
   normalize functions — everything downstream (scoring, UI)
   will pick it up automatically.
========================================================= */

const SOCKET_URL = "wss://pumpdev.io/ws?key=iBO7sF5WAwB7A-ZwEBUqlVrC3l0x_j9WpxDf1SWvHkV62Jh37_3bGpV912ACLqCA";

// how many of the newest tokens we keep actively subscribed to trade events for
const MAX_TRACKED_TRADE_SUBS = 60;

// ---------------------------------------------------------
// state
// ---------------------------------------------------------
const state = {
  tokens: new Map(),     // mint -> token record
  order: [],             // mint list, newest first
  creatorSeen: new Map(),// creatorAddress -> Set(mints) this session
  tradeSubs: [],         // mints currently subscribed to trade events (FIFO)
  activeMint: null,
  tradeTimestamps: [],   // for trades/min counter
  seenCount: 0,
  showRaw: false,
  sort: "new",
  filter: "",
};

let ws = null;
let reconnectDelay = 1500;

// ---------------------------------------------------------
// connection
// ---------------------------------------------------------
function connect() {
  setConn("connecting");
  log("sys", `connecting to ${SOCKET_URL.split("?")[0]} …`);

  try {
    ws = new WebSocket(SOCKET_URL);
  } catch (err) {
    setConn("error");
    log("sys", `failed to open socket: ${err.message}`);
    scheduleReconnect();
    return;
  }

  ws.onopen = () => {
    setConn("connected");
    reconnectDelay = 1500;
    log("sys", "connected — subscribing to new token stream");
    safeSend({ method: "subscribeNewToken" });
    // also try the alt PumpPortal-style topic subscribe shape, harmless if ignored
    safeSend({ action: "subscribe", topic: "subscribeNewToken" });
  };

  ws.onmessage = (evt) => {
    let msg;
    try {
      msg = JSON.parse(evt.data);
    } catch {
      log("sys", `non-JSON message: ${String(evt.data).slice(0, 120)}`);
      return;
    }
    if (state.showRaw) log("raw", JSON.stringify(msg));
    handleMessage(msg);
  };

  ws.onerror = () => {
    setConn("error");
  };

  ws.onclose = () => {
    setConn("error");
    log("sys", "connection closed — retrying…");
    scheduleReconnect();
  };
}

function scheduleReconnect() {
  setTimeout(connect, reconnectDelay);
  reconnectDelay = Math.min(reconnectDelay * 1.6, 20000);
}

function safeSend(obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function setConn(stateName) {
  const el = document.getElementById("connStatus");
  el.dataset.state = stateName;
  el.querySelector(".conn-label").textContent = stateName;
}

// ---------------------------------------------------------
// message routing
// ---------------------------------------------------------
function handleMessage(msg) {
  if (!msg || typeof msg !== "object") return;

  // heuristics to classify a message as "new token" vs "trade" vs "migration" vs other
  const type = (msg.txType || msg.type || msg.event || "").toString().toLowerCase();
  const looksLikeNewToken =
    type === "create" || type === "newtoken" || (!type && msg.mint && msg.name && msg.symbol && msg.marketCapSol == null && msg.txType == null);
  const looksLikeTrade =
    type === "buy" || type === "sell" || (msg.txType && (msg.txType === "buy" || msg.txType === "sell"));
  const looksLikeMigration = type === "migrate" || type === "migration";

  if (looksLikeTrade) {
    handleTrade(msg);
  } else if (looksLikeMigration) {
    const mint = msg.mint || msg.tokenAddress;
    if (mint && state.tokens.has(mint)) {
      const t = state.tokens.get(mint);
      t.migrated = true;
      log("sys", `${t.symbol || mint.slice(0, 6)} migrated to PumpSwap`);
      renderFeed();
      if (state.activeMint === mint) renderDetail(mint);
    }
  } else if (msg.mint || msg.tokenAddress || msg.mintAddress) {
    // fallback: if it has a mint + name/symbol, treat as a new-token style payload
    handleNewToken(msg);
  }
}

// ---------------------------------------------------------
// new token normalization + intake
// ---------------------------------------------------------
function normalizeNewToken(msg) {
  return {
    mint: msg.mint || msg.tokenAddress || msg.mintAddress,
    name: msg.name || msg.tokenName || "Unknown",
    symbol: msg.symbol || msg.ticker || "?",
    creator: msg.creator || msg.traderPublicKey || msg.devWallet || msg.creatorAddress || null,
    createdAt: Date.now(),
    initialBuySol: num(msg.initialBuy ?? msg.solAmount ?? msg.devBuySol),
    marketCapSol: num(msg.marketCapSol ?? msg.marketCap),
    uri: msg.uri || msg.metadataUri || null,
    twitter: msg.twitter || msg.twitterUrl || extractFromUri(msg, "twitter"),
    telegram: msg.telegram || msg.telegramUrl || extractFromUri(msg, "telegram"),
    website: msg.website || msg.websiteUrl || extractFromUri(msg, "website"),
  };
}

function extractFromUri() {
  // pump.fun metadata (name/symbol/socials) usually lives behind the `uri` field
  // (an IPFS/Arweave JSON blob) which requires a follow-up fetch to resolve.
  // We don't fetch it automatically to keep this scanner fully local/offline-safe
  // beyond the single websocket connection — leaving this as null is honest about
  // that limitation rather than guessing.
  return null;
}

function handleNewToken(rawMsg) {
  const t = normalizeNewToken(rawMsg);
  if (!t.mint || state.tokens.has(t.mint)) return;

  t.buys = 0;
  t.sells = 0;
  t.volumeSol = 0;
  t.maxSingleTradeSol = 0;
  t.uniqueTraders = new Set();
  t.lastMarketCapSol = t.marketCapSol || 0;
  t.migrated = false;
  t.raw = rawMsg;

  state.tokens.set(t.mint, t);
  state.order.unshift(t.mint);
  state.seenCount++;

  if (t.creator) {
    if (!state.creatorSeen.has(t.creator)) state.creatorSeen.set(t.creator, new Set());
    state.creatorSeen.get(t.creator).add(t.mint);
  }

  subscribeTrades(t.mint);
  scoreToken(t);

  document.getElementById("statSeen").textContent = state.seenCount;
  log("new", `NEW  ${t.symbol.padEnd(8)} ${t.name}`);
  renderFeed();
}

function subscribeTrades(mint) {
  safeSend({ method: "subscribeTokenTrade", keys: [mint] });
  state.tradeSubs.push(mint);
  if (state.tradeSubs.length > MAX_TRACKED_TRADE_SUBS) {
    const drop = state.tradeSubs.shift();
    safeSend({ method: "unsubscribeTokenTrade", keys: [drop] });
  }
}

// ---------------------------------------------------------
// trade normalization + intake
// ---------------------------------------------------------
function normalizeTrade(msg) {
  return {
    mint: msg.mint || msg.tokenAddress,
    side: (msg.txType || msg.side || "").toLowerCase(), // "buy" | "sell"
    trader: msg.traderPublicKey || msg.trader || msg.wallet,
    solAmount: num(msg.solAmount ?? msg.amountSol ?? msg.sol),
    marketCapSol: num(msg.marketCapSol ?? msg.marketCap),
  };
}

function handleTrade(rawMsg) {
  const tr = normalizeTrade(rawMsg);
  if (!tr.mint) return;
  const t = state.tokens.get(tr.mint);
  if (!t) return; // trade for a token we haven't indexed as "new" yet — skip

  if (tr.side === "buy") t.buys++;
  else if (tr.side === "sell") t.sells++;

  t.volumeSol += tr.solAmount || 0;
  t.maxSingleTradeSol = Math.max(t.maxSingleTradeSol, tr.solAmount || 0);
  if (tr.trader) t.uniqueTraders.add(tr.trader);
  if (tr.marketCapSol) t.lastMarketCapSol = tr.marketCapSol;

  state.tradeTimestamps.push(Date.now());
  trimTradeTimestamps();

  scoreToken(t);
  log(tr.side === "sell" ? "sell" : "buy",
    `${tr.side === "sell" ? "SELL" : "BUY "} ${(t.symbol || "?").padEnd(8)} ${fmtSol(tr.solAmount)} SOL`);

  // re-render only if visible in current sort context or currently active
  renderFeed();
  if (state.activeMint === tr.mint) renderDetail(tr.mint);
}

function trimTradeTimestamps() {
  const cutoff = Date.now() - 60000;
  while (state.tradeTimestamps.length && state.tradeTimestamps[0] < cutoff) {
    state.tradeTimestamps.shift();
  }
  document.getElementById("statTradeRate").textContent = state.tradeTimestamps.length;
}
setInterval(trimTradeTimestamps, 4000);

// ---------------------------------------------------------
// scoring — one function per factor, each returns {score:0-100, note}
// composite score is a weighted blend, clearly labeled as heuristic
// ---------------------------------------------------------
const WEIGHTS = {
  liquidity: 0.16,
  marketCap: 0.10,
  holders: 0.14,
  volume: 0.12,
  buySell: 0.14,
  creator: 0.14,
  whale: 0.12,
  age: 0.08,
};

function scoreLiquidity(t) {
  // proxy: current market cap in SOL as a stand-in for bonding-curve depth
  const mc = t.lastMarketCapSol || t.marketCapSol || 0;
  const score = clamp(logScale(mc, 5, 300) * 100, 0, 100);
  return { score, note: mc ? `~${fmtSol(mc)} SOL market cap as depth proxy` : "no liquidity data yet" };
}

function scoreMarketCap(t) {
  const mc = t.lastMarketCapSol || t.marketCapSol || 0;
  // sweet spot: not near-zero (unproven), not huge (overextended, late)
  let score;
  if (mc <= 0) score = 40;
  else if (mc < 15) score = 45 + (mc / 15) * 25;         // ramping up, still early
  else if (mc < 120) score = 70 - ((mc - 15) / 105) * 20; // fine zone tapering
  else score = clamp(50 - Math.log10(mc / 120) * 25, 5, 50); // overextended penalty
  return { score: clamp(score, 0, 100), note: `${fmtSol(mc)} SOL market cap` };
}

function scoreHolders(t) {
  const n = t.uniqueTraders.size;
  const score = clamp(logScale(n, 3, 80) * 100, 0, 100);
  return { score, note: `${n} unique wallet${n === 1 ? "" : "s"} observed this session` };
}

function scoreVolume(t) {
  const v = t.volumeSol;
  const score = clamp(logScale(v, 2, 150) * 100, 0, 100);
  return { score, note: `${fmtSol(v)} SOL traded this session` };
}

function scoreBuySell(t) {
  const total = t.buys + t.sells;
  if (total === 0) return { score: 50, note: "no trades observed yet" };
  const ratio = t.buys / total;
  const score = clamp(ratio * 100, 0, 100);
  return { score, note: `${t.buys} buys / ${t.sells} sells (${(ratio * 100).toFixed(0)}% buys)` };
}

function scoreCreator(t) {
  if (!t.creator) return { score: 50, note: "creator wallet unknown" };
  const launches = state.creatorSeen.get(t.creator)?.size || 1;
  // this session only — we can't see this wallet's full on-chain rug history
  // without an external indexer, so this is explicitly a partial signal
  const score = launches === 1 ? 65 : clamp(65 - (launches - 1) * 18, 5, 65);
  const note = launches === 1
    ? "first token seen from this creator this session"
    : `creator has launched ${launches} tokens seen this session — repeat launchers correlate with abandoned projects`;
  return { score, note };
}

function scoreWhale(t) {
  if (t.volumeSol <= 0) return { score: 55, note: "no volume yet to assess concentration" };
  const concentration = t.maxSingleTradeSol / t.volumeSol;
  const score = clamp((1 - concentration) * 100, 0, 100);
  const note = `largest single trade is ${(concentration * 100).toFixed(0)}% of all session volume`;
  return { score, note };
}

function scoreAge(t) {
  const ageMin = (Date.now() - t.createdAt) / 60000;
  // very fresh = risky (score low), climbs over the first ~40 min, then plateaus
  const score = clamp(20 + logScale(ageMin, 0.5, 45) * 65, 10, 85);
  const note = ageMin < 1 ? "under 1 minute old" : `${ageMin.toFixed(0)} min old`;
  return { score, note };
}

function scoreToken(t) {
  const factors = {
    liquidity: scoreLiquidity(t),
    marketCap: scoreMarketCap(t),
    holders: scoreHolders(t),
    volume: scoreVolume(t),
    buySell: scoreBuySell(t),
    creator: scoreCreator(t),
    whale: scoreWhale(t),
    age: scoreAge(t),
  };
  let composite = 0;
  for (const k in WEIGHTS) composite += factors[k].score * WEIGHTS[k];
  t.factors = factors;
  t.compositeScore = Math.round(composite);
  t.flags = buildFlags(t, factors);
  return t;
}

function buildFlags(t, f) {
  const flags = [];
  if (f.creator.score < 40) flags.push({ level: "bad", text: "repeat creator wallet" });
  if (f.whale.score < 40) flags.push({ level: "bad", text: "whale-concentrated volume" });
  if (f.age.score < 30) flags.push({ level: "warn", text: "very new token" });
  if (f.buySell.score < 35) flags.push({ level: "bad", text: "sell-heavy flow" });
  if (f.buySell.score > 70) flags.push({ level: "good", text: "buy-dominant flow" });
  if (!t.twitter && !t.telegram && !t.website) flags.push({ level: "warn", text: "no social links detected" });
  if (t.migrated) flags.push({ level: "good", text: "migrated to PumpSwap" });
  if (t.compositeScore >= 70) flags.push({ level: "good", text: "strong composite score" });
  if (t.compositeScore <= 30) flags.push({ level: "bad", text: "weak composite score" });
  return flags;
}

// ---------------------------------------------------------
// rendering — feed list
// ---------------------------------------------------------
function renderFeed() {
  const list = document.getElementById("feedList");
  let mints = [...state.order];

  if (state.filter) {
    const f = state.filter.toLowerCase();
    mints = mints.filter((m) => {
      const t = state.tokens.get(m);
      return t && (t.name.toLowerCase().includes(f) || t.symbol.toLowerCase().includes(f) || m.toLowerCase().includes(f));
    });
  }

  const arr = mints.map((m) => state.tokens.get(m)).filter(Boolean);
  if (state.sort === "score") arr.sort((a, b) => b.compositeScore - a.compositeScore);
  else if (state.sort === "volume") arr.sort((a, b) => b.volumeSol - a.volumeSol);
  else if (state.sort === "risk") arr.sort((a, b) => a.compositeScore - b.compositeScore);
  // "new" keeps insertion order already

  list.innerHTML = "";
  if (!arr.length) {
    const empty = document.createElement("div");
    empty.className = "feed-empty";
    empty.id = "feedEmpty";
    empty.textContent = state.order.length ? "No tokens match your filter." : "Waiting for tokens from the socket…";
    list.appendChild(empty);
    return;
  }
  for (const t of arr) list.appendChild(feedItemEl(t));
}

function feedItemEl(t) {
  const div = document.createElement("div");
  div.className = "feed-item" + (t.mint === state.activeMint ? " active" : "");
  div.dataset.mint = t.mint;

  const ageMin = ((Date.now() - t.createdAt) / 60000).toFixed(0);
  const total = t.buys + t.sells;
  const buyPct = total ? Math.round((t.buys / total) * 100) : null;

  div.innerHTML = `
    <div class="fi-score ${scoreClass(t.compositeScore)}">${t.compositeScore}</div>
    <div class="fi-main">
      <div class="fi-name"><b>${escapeHtml(t.symbol)}</b><span>${escapeHtml(t.name)}</span></div>
      <div class="fi-meta">
        <span>${fmtSol(t.volumeSol)} SOL vol</span>
        ${buyPct !== null ? `<span class="${buyPct >= 50 ? "up" : "down"}">${buyPct}% buys</span>` : `<span>no trades</span>`}
      </div>
    </div>
    <div class="fi-age">${ageMin}m</div>
  `;
  div.addEventListener("click", () => selectToken(t.mint));
  return div;
}

function scoreClass(s) {
  if (s >= 65) return "score-high";
  if (s >= 40) return "score-mid";
  return "score-low";
}

// ---------------------------------------------------------
// rendering — detail panel
// ---------------------------------------------------------
function selectToken(mint) {
  state.activeMint = mint;
  renderFeed();
  renderDetail(mint);
}

const FACTOR_LABELS = {
  liquidity: "Liquidity",
  marketCap: "Market Cap",
  holders: "Holder Count",
  volume: "Volume",
  buySell: "Buy / Sell Ratio",
  creator: "Creator Wallet",
  whale: "Whale Holdings",
  age: "Token Age",
};

function renderDetail(mint) {
  const t = state.tokens.get(mint);
  const panel = document.getElementById("detailPanel");
  if (!t) {
    panel.innerHTML = `<div class="detail-empty"><p>Select a token from the feed to see its full breakdown.</p></div>`;
    return;
  }

  const flagsHtml = t.flags.map((f) => `<span class="pill ${f.level}">${escapeHtml(f.text)}</span>`).join("");

  const factorsHtml = Object.entries(FACTOR_LABELS).map(([key, label]) => {
    const f = t.factors[key];
    const cls = scoreBarClass(f.score);
    return `
      <div class="factor">
        <div class="factor-top">
          <span class="factor-name">${label}</span>
          <span class="factor-score" style="color:${cls.color}">${Math.round(f.score)}</span>
        </div>
        <div class="factor-bar"><div class="factor-bar-fill" style="width:${f.score}%;background:${cls.color}"></div></div>
        <div class="factor-note">${escapeHtml(f.note)}</div>
      </div>`;
  }).join("");

  const socials = [
    t.twitter ? `<span class="pill good">X / Twitter linked</span>` : "",
    t.telegram ? `<span class="pill good">Telegram linked</span>` : "",
    t.website ? `<span class="pill good">Website linked</span>` : "",
  ].join("");

  panel.innerHTML = `
    <div class="detail-head">
      <div class="dh-top">
        <div class="dh-title">
          <h1>${escapeHtml(t.name)} <span style="color:var(--dim);font-family:var(--mono);font-size:14px;">${escapeHtml(t.symbol)}</span></h1>
          <div class="mint">${escapeHtml(t.mint)}</div>
        </div>
        <div class="dh-score" style="border-color:${scoreBarClass(t.compositeScore).color}">
          <div class="num" style="color:${scoreBarClass(t.compositeScore).color}">${t.compositeScore}</div>
          <div class="lbl">heuristic score</div>
        </div>
      </div>
      <div class="verdict-row">${flagsHtml}${socials}</div>
    </div>
    <div class="factor-grid">${factorsHtml}</div>
    <div class="raw-block">
      <div class="raw-block-title">last known raw payload for this token</div>
      ${escapeHtml(JSON.stringify(t.raw, null, 2))}
    </div>
  `;
}

function scoreBarClass(score) {
  if (score >= 65) return { color: "var(--bull)" };
  if (score >= 40) return { color: "var(--warn)" };
  return { color: "var(--bear)" };
}

// ---------------------------------------------------------
// footer event log
// ---------------------------------------------------------
function log(kind, text) {
  const body = document.getElementById("logBody");
  const line = document.createElement("div");
  line.className = "log-line " + kind;
  const time = new Date().toLocaleTimeString([], { hour12: false });
  line.innerHTML = `<span class="t">${time}</span>${escapeHtml(text)}`;
  body.appendChild(line);
  // cap log length
  while (body.children.length > 400) body.removeChild(body.firstChild);
  body.scrollTop = body.scrollHeight;
}

// ---------------------------------------------------------
// helpers
// ---------------------------------------------------------
function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}
function fmtSol(v) {
  if (!v) return "0";
  if (v >= 1000) return (v / 1000).toFixed(1) + "k";
  return v.toFixed(v < 1 ? 3 : 1);
}
function clamp(v, lo, hi) { return Math.max(lo, Math.min(hi, v)); }
function logScale(v, lo, hi) {
  // maps v in [lo, hi] logarithmically to [0, 1], clamped
  if (v <= 0) return 0;
  const l = Math.log(Math.max(v, 0.0001));
  const a = Math.log(lo), b = Math.log(hi);
  return clamp((l - a) / (b - a), 0, 1);
}
function escapeHtml(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}

// ---------------------------------------------------------
// wire up controls
// ---------------------------------------------------------
document.getElementById("searchBox").addEventListener("input", (e) => {
  state.filter = e.target.value;
  renderFeed();
});
document.getElementById("sortSelect").addEventListener("change", (e) => {
  state.sort = e.target.value;
  renderFeed();
});
document.getElementById("rawToggle").addEventListener("change", (e) => {
  state.showRaw = e.target.checked;
});
document.getElementById("clearLog").addEventListener("click", () => {
  document.getElementById("logBody").innerHTML = "";
});

// periodic re-render so age / rate labels stay fresh even without new events
setInterval(() => {
  renderFeed();
  if (state.activeMint) renderDetail(state.activeMint);
}, 15000);

// ---------------------------------------------------------
// go
// ---------------------------------------------------------
connect();
