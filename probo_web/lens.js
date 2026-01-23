const API_BASE = (window.PROBO_API_BASE || "").replace(/\/$/, "");
const ADDRESS_REGEX = /^0x[a-fA-F0-9]{40}$/;

const addressInput = document.getElementById("lensAddress");
const checkButton = document.getElementById("lensCheck");
const statusEl = document.getElementById("lensStatus");
const analysisSummary = document.getElementById("analysisSummary");
const analysisList = document.getElementById("analysisList");
const txSummary = document.getElementById("txSummary");
const txList = document.getElementById("txList");

const setStatus = (text, tone = "") => {
  statusEl.textContent = text;
  statusEl.dataset.tone = tone;
};

const clearList = (node) => {
  while (node.firstChild) node.removeChild(node.firstChild);
};

const addListItem = (node, text) => {
  const item = document.createElement("li");
  item.textContent = text;
  node.appendChild(item);
};

const shorten = (value) => {
  if (!value || value.length < 12) return value || "";
  return `${value.slice(0, 6)}...${value.slice(-4)}`;
};

const parseErrorDetail = (detail) => {
  if (!detail) return null;
  if (Array.isArray(detail)) {
    return detail
      .map((item) => item.msg || item.message || JSON.stringify(item))
      .filter(Boolean)
      .join("; ");
  }
  if (typeof detail === "string") return detail;
  return detail.message || JSON.stringify(detail);
};

const requestJson = async (path, payload) => {
  const response = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const rawText = await response.text();
  let data = null;
  try {
    data = rawText ? JSON.parse(rawText) : null;
  } catch (err) {
    data = null;
  }
  if (!response.ok) {
    const message =
      parseErrorDetail(data && data.detail) ||
      (data && data.message) ||
      rawText ||
      response.statusText ||
      "Request failed";
    throw new Error(message);
  }
  return data;
};

const renderAnalysis = (data) => {
  const score = data.score ?? "--";
  const label = data.label || "Unknown";
  analysisSummary.textContent = `Trust signal: ${score} (${label}).`;

  clearList(analysisList);
  if (data.reasons && data.reasons.length) {
    data.reasons.forEach((reason) => {
      addListItem(analysisList, `${reason.code || "Signal"}: ${reason.detail || ""}`.trim());
    });
  } else {
    addListItem(analysisList, "No analysis reasons returned.");
  }
};

const renderTransfers = (payload) => {
  const transfers = Array.isArray(payload.transfers) ? payload.transfers : [];
  const windowDays = payload.window_days || payload.windowDays || "";
  const truncated = payload.transfers_truncated ? "yes" : "no";
  const shown = Math.min(25, transfers.length);
  txSummary.textContent = `Transfers: ${transfers.length} (showing ${shown}). Window: ${windowDays}d. Truncated: ${truncated}.`;

  clearList(txList);
  if (!transfers.length) {
    addListItem(txList, "No transfers available for this window.");
    return;
  }

  transfers.slice(0, shown).forEach((tx) => {
    const timestamp = tx.metadata && tx.metadata.blockTimestamp ? tx.metadata.blockTimestamp : "";
    const from = shorten(tx.from || "");
    const to = shorten(tx.to || "");
    const asset = tx.asset || tx.category || "asset";
    const value = tx.value !== undefined && tx.value !== null ? tx.value : "";
    const line = `${timestamp} ${from} → ${to} | ${asset} ${value}`.trim();
    addListItem(txList, line);
  });
};

const runLens = async () => {
  const address = addressInput.value.trim();
  if (!address) {
    setStatus("Paste an address to run the lens.", "error");
    return;
  }
  if (!API_BASE) {
    setStatus("Missing API base. Set window.PROBO_API_BASE in lens.html.", "error");
    return;
  }
  if (!ADDRESS_REGEX.test(address)) {
    setStatus("Enter a valid 0x address with 40 hex characters.", "error");
    return;
  }

  setStatus("Running lens...", "loading");
  checkButton.disabled = true;
  try {
    const [analysis, extraction] = await Promise.all([
      requestJson("/analyze", {
        address,
        run_extract: true,
        save_extraction: false,
        include_infra: true,
      }),
      requestJson("/extraction", {
        address,
        run_extract: true,
        save_extraction: false,
      }),
    ]);

    renderAnalysis(analysis);
    renderTransfers(extraction.payload || {});
    setStatus("Ready.");
  } catch (error) {
    setStatus(`Error: ${error.message}`, "error");
  } finally {
    checkButton.disabled = false;
  }
};

checkButton.addEventListener("click", runLens);
addressInput.addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    runLens();
  }
});

setStatus("Ready.");
