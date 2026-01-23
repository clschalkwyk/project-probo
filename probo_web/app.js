const API_BASE = (window.PROBO_API_BASE || "").replace(/\/$/, "");
const ADDRESS_REGEX = /^0x[a-fA-F0-9]{40}$/;

const EXAMPLES = [
  {
    address: "0xf514a399a052252d22b9ff87b8a743197c8afa33",
    label: "Low",
  },
  {
    address: "0x68645df0cc2d808f69320906de974fa062d6280f",
    label: "Medium",
  },
  {
    address: "0x267be1c1d684f78cb4f6a176c4911b741e4ffdc0",
    label: "High",
  },
];

const addressInput = document.getElementById("addressInput");
const trustDot = document.getElementById("trustDot");
const exampleList = document.getElementById("exampleList");
const statusEl = document.getElementById("status");
const statusText = document.getElementById("statusText");
const statusSpinner = document.getElementById("statusSpinner");
const resultEl = document.getElementById("result");
const toggleDetails = document.getElementById("toggleDetails");
const explainButton = document.getElementById("explainButton");
const explainText = document.getElementById("explainText");
const explainSpinner = document.getElementById("explainSpinner");
const resultAddress = document.getElementById("resultAddress");
const scoreChip = document.getElementById("scoreChip");
const trustBand = document.getElementById("trustBand");
const reasonList = document.getElementById("reasonList");
const patternList = document.getElementById("patternList");
const statList = document.getElementById("statList");

let lastAnalysis = null;

const formatNumber = (value, digits = 0) => {
  if (value === null || value === undefined) return "n/a";
  const num = Number(value);
  if (Number.isNaN(num)) return "n/a";
  return num.toLocaleString(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: digits,
  });
};

const setStatus = (text, tone = "") => {
  statusText.textContent = text;
  statusEl.dataset.tone = tone;
  statusSpinner.classList.toggle("visible", tone === "loading");
};

const setDetailsReady = (ready) => {
  toggleDetails.dataset.ready = ready ? "true" : "false";
};

const setExplainLoading = (loading) => {
  explainSpinner.classList.toggle("visible", loading);
  explainButton.classList.toggle("loading", loading);
  explainButton.disabled = loading;
};

const setDetailsOpen = (open) => {
  resultEl.classList.toggle("hidden", !open);
  toggleDetails.textContent = open ? "Hide details" : "Show details";
};

const setTrustDot = (label) => {
  const tone = (label || "unknown").toLowerCase();
  trustDot.dataset.tone = tone;
};

const clearList = (node) => {
  while (node.firstChild) node.removeChild(node.firstChild);
};

const addListItem = (node, text) => {
  const item = document.createElement("li");
  item.textContent = text;
  node.appendChild(item);
};

const getBandTone = (label) => {
  switch ((label || "").toLowerCase()) {
    case "high":
      return "High trust";
    case "medium":
      return "Moderate trust";
    case "low":
      return "Low trust";
    default:
      return "Unknown";
  }
};

const updateResult = (data) => {
  lastAnalysis = data;
  resultAddress.textContent = data.address || "Unknown address";
  const scoreValue = scoreChip.querySelector(".score-value");
  scoreValue.textContent = data.score ?? "--";
  trustBand.textContent = getBandTone(data.label);
  setTrustDot(data.label);

  clearList(reasonList);
  if (data.reasons && data.reasons.length) {
    data.reasons.forEach((reason) => {
      addListItem(reasonList, `${reason.code || "Signal"}: ${reason.detail || ""}`.trim());
    });
  } else {
    addListItem(reasonList, "No reasons returned.");
  }

  clearList(patternList);
  if (data.infra && data.infra.explain && data.infra.explain.length) {
    data.infra.explain.forEach((item) => addListItem(patternList, item));
  } else {
    addListItem(patternList, "No pattern highlights in this window.");
  }

  clearList(statList);
  const features = data.features || {};
  addListItem(statList, `Transfers (window): ${formatNumber(features.tx_count_30)}`);
  addListItem(statList, `Active days (window): ${formatNumber(features.active_days_30)}`);
  addListItem(statList, `Unique counterparties: ${formatNumber(features.unique_counterparties_30)}`);
  addListItem(statList, `Wallet age (days): ${formatNumber(features.wallet_age_days)}`);
  addListItem(statList, `Transfer cap hit: ${features.transfers_truncated ? "yes" : "no"}`);
  if (features.tx_acceleration_flag !== undefined) {
    addListItem(
      statList,
      `Acceleration flag: ${features.tx_acceleration_flag ? "yes" : "no"}`
    );
  }
};

const humanizeSignals = (data) => {
  if (!data) return "Run an analysis first to get a plain-language summary.";
  const reasons = (data.reasons || [])
    .map((item) => item.detail || item.code)
    .filter(Boolean);
  const patterns = (data.infra && data.infra.explain) || [];
  const combined = [...reasons, ...patterns].filter(Boolean);
  if (!combined.length) {
    return "No strong behavioral signals were detected in this window.";
  }
  const highlights = combined.slice(0, 3).join("; ");
  return `Summary: ${highlights}.`;
};

const toggleExplain = async () => {
  if (!lastAnalysis) {
    explainText.textContent = humanizeSignals(lastAnalysis);
    explainText.classList.toggle("hidden", false);
    return;
  }
  const reasons = (lastAnalysis.reasons || [])
    .map((item) => item.detail || item.code)
    .filter(Boolean);
  const patterns = (lastAnalysis.infra && lastAnalysis.infra.explain) || [];
  setExplainLoading(true);
  try {
    const response = await fetch(`${API_BASE}/explain`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ reasons, patterns }),
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
        "Explain failed";
      throw new Error(message);
    }
    explainText.textContent = data.summary || humanizeSignals(lastAnalysis);
    explainText.classList.toggle("hidden", false);
  } catch (error) {
    explainText.textContent = humanizeSignals(lastAnalysis);
    explainText.classList.toggle("hidden", false);
  } finally {
    setExplainLoading(false);
  }
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

const callApi = async (address) => {
  if (!address) return;
  if (!API_BASE) {
    setStatus("Missing API base. Set window.PROBO_API_BASE in index.html.", "error");
    return;
  }
  if (!ADDRESS_REGEX.test(address)) {
    setStatus("Enter a valid 0x address with 40 hex characters.", "error");
    return;
  }
  setStatus("Checking address...", "loading");
  setDetailsReady(false);
  setTrustDot("unknown");
  explainText.classList.add("hidden");
  try {
    const response = await fetch(`${API_BASE}/analyze`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        address,
        run_extract: true,
        save_extraction: false,
        include_infra: true,
      }),
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
    updateResult(data);
    setDetailsReady(true);
    setStatus("Ready.");
  } catch (error) {
    setTrustDot("unknown");
    setStatus(`Error: ${error.message}`, "error");
  }
};

EXAMPLES.forEach((example) => {
  const address = example.address;
  const chip = document.createElement("button");
  const label = (example.label || "neutral").toLowerCase();
  chip.className = `chip chip-${label}`;
  chip.type = "button";
  chip.textContent = `${example.label}: ${address.slice(0, 6)}...${address.slice(-4)}`;
  chip.addEventListener("click", () => {
    addressInput.value = address;
    callApi(address);
  });
  exampleList.appendChild(chip);
});

addressInput.addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    const value = addressInput.value.trim();
    if (!value) {
      setStatus("Paste an address to check.", "error");
      return;
    }
    callApi(value);
  }
});

addressInput.addEventListener("input", () => {
  setTrustDot("unknown");
  setDetailsReady(false);
  lastAnalysis = null;
  explainText.classList.add("hidden");
});

toggleDetails.addEventListener("click", () => {
  setDetailsOpen(resultEl.classList.contains("hidden"));
});

explainButton.addEventListener("click", () => {
  toggleExplain();
});

setStatus("Ready.");
setDetailsOpen(false);
setDetailsReady(false);
setTrustDot("unknown");
explainText.classList.add("hidden");
setExplainLoading(false);
