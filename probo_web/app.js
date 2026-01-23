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
const runButton = document.getElementById("runButton");
const runSpinner = document.getElementById("runSpinner");
const explainButton = document.getElementById("explainButton");
const explainText = document.getElementById("explainText");
const explainSpinner = document.getElementById("explainSpinner");
const explainCacheToggle = document.getElementById("explainCacheToggle");
const explainLangs = document.getElementById("explainLangs");
const explainLangButtons = explainLangs
  ? Array.from(explainLangs.querySelectorAll("button[data-lang]"))
  : [];
const trustBand = document.getElementById("trustBand");
const trustScore = document.getElementById("trustScore");
const resultInline = document.getElementById("resultInline");
const resultSummary = document.getElementById("resultSummary");
const resultActions = document.getElementById("resultActions");
const lensLink = document.getElementById("lensLink");

let lastAnalysis = null;
let lastExplainSummary = null;
let explainOpen = false;
let selectedExplainLang = null;

const setStatus = (text, tone = "") => {
  statusText.textContent = text;
  statusEl.dataset.tone = tone;
  statusSpinner.classList.toggle("visible", tone === "loading");
};

const setResultReady = (ready) => {
  if (resultInline) {
    resultInline.classList.toggle("hidden", !ready);
  }
  if (resultActions) {
    resultActions.classList.toggle("hidden", !ready);
  }
  if (lensLink) {
    lensLink.classList.toggle("hidden", !ready);
  }
};

const setRunLoading = (loading) => {
  if (runSpinner) {
    runSpinner.classList.toggle("visible", loading);
  }
  runButton.classList.toggle("loading", loading);
  runButton.disabled = loading;
  addressInput.disabled = loading;
};

const setExplainLoading = (loading) => {
  explainSpinner.classList.toggle("visible", loading);
  explainButton.classList.toggle("loading", loading);
  explainButton.disabled = loading;
  if (explainCacheToggle) {
    explainCacheToggle.disabled = loading;
  }
};

const setExplainOpen = (open) => {
  explainOpen = open;
  explainText.classList.toggle("hidden", !open);
  explainButton.setAttribute("aria-expanded", open ? "true" : "false");
  const label = explainButton.querySelector(".btn-label");
  if (label) {
    label.textContent = open ? "Hide story" : "Tell me the story";
  }
  if (!open && explainLangs) {
    explainLangs.classList.add("hidden");
  }
  updateExplainLanguages();
};

const setTrustDot = (label) => {
  const tone = (label || "unknown").toLowerCase();
  trustDot.dataset.tone = tone;
};

const pickSummaryText = (summary, preferred) => {
  if (!summary) return "";
  if (typeof summary === "string") return summary;
  if (typeof summary === "object") {
    const order = [preferred, "en", "fr", "pt", "zu"];
    for (const key of order) {
      const value = summary[key];
      if (typeof value === "string" && value.trim()) {
        return value.trim();
      }
    }
    const fallback = Object.values(summary).find(
      (value) => typeof value === "string" && value.trim(),
    );
    return fallback ? fallback.trim() : "";
  }
  return String(summary);
};

const getPreferredLanguage = () =>
  (navigator.language || "en").slice(0, 2).toLowerCase();

const getAvailableExplainLanguages = () => {
  if (!lastExplainSummary || typeof lastExplainSummary !== "object") {
    return [];
  }
  return ["en", "fr", "pt", "zu"].filter((lang) => {
    const value = lastExplainSummary[lang];
    return typeof value === "string" && value.trim();
  });
};

const updateExplainText = () => {
  const preferred = selectedExplainLang || getPreferredLanguage();
  const summaryText = pickSummaryText(lastExplainSummary, preferred);
  explainText.textContent = summaryText || humanizeSignals(lastAnalysis);
};

const updateExplainLanguages = () => {
  if (!explainLangs) return;
  const available = getAvailableExplainLanguages();
  if (!available.length || !explainOpen) {
    explainLangs.classList.add("hidden");
    return;
  }
  if (!selectedExplainLang || !available.includes(selectedExplainLang)) {
    const preferred = getPreferredLanguage();
    selectedExplainLang = available.includes(preferred) ? preferred : available[0];
  }
  explainLangs.classList.remove("hidden");
  explainLangButtons.forEach((button) => {
    const lang = button.dataset.lang;
    const enabled = available.includes(lang);
    button.disabled = !enabled;
    button.setAttribute("aria-pressed", lang === selectedExplainLang ? "true" : "false");
  });
};

const getBandTone = (label) => {
  switch ((label || "").toLowerCase()) {
    case "high":
      return "High";
    case "medium":
      return "Medium";
    case "low":
      return "Low";
    default:
      return "Unknown";
  }
};

const updateResult = (data) => {
  lastAnalysis = data;
  trustScore.textContent = data.score ?? "--";
  trustBand.textContent = getBandTone(data.label);
  setTrustDot(data.label);
  if (lensLink && data.address) {
    const encoded = encodeURIComponent(data.address);
    lensLink.href = `probo_lens/?q=${encoded}`;
  }
  if (resultSummary) {
    resultSummary.textContent = humanizeSignals(data);
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
    return;
  }
  const reasons = (lastAnalysis.reasons || [])
    .map((item) => item.detail || item.code)
    .filter(Boolean);
  const patterns = (lastAnalysis.infra && lastAnalysis.infra.explain) || [];
  const address = lastAnalysis.address || "";
  const useCache = explainCacheToggle ? explainCacheToggle.checked : true;
  setStatus("Translating signals...", "loading");
  setExplainLoading(true);
  try {
    const response = await fetch(`${API_BASE}/explain`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        address,
        reasons,
        patterns,
        use_cache: useCache,
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
        "Explain failed";
      throw new Error(message);
    }
    lastExplainSummary = data.summary;
    updateExplainLanguages();
    updateExplainText();
  } catch (error) {
    lastExplainSummary = null;
    updateExplainLanguages();
    explainText.textContent = humanizeSignals(lastAnalysis);
  } finally {
    setExplainLoading(false);
    setStatus("");
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
  setResultReady(false);
  setRunLoading(true);
  setTrustDot("unknown");
  setExplainOpen(false);
  explainText.textContent = "";
  lastExplainSummary = null;
  selectedExplainLang = null;
  trustBand.textContent = "--";
  trustScore.textContent = "--";
  if (resultSummary) {
    resultSummary.textContent = "";
  }
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
    setResultReady(true);
    setStatus("");
  } catch (error) {
    setTrustDot("unknown");
    setStatus(`Error: ${error.message}`, "error");
  } finally {
    setRunLoading(false);
  }
};

if (explainLangButtons.length) {
  explainLangButtons.forEach((button) => {
    button.addEventListener("click", () => {
      const lang = button.dataset.lang;
      if (!lang) return;
      selectedExplainLang = lang;
      updateExplainLanguages();
      updateExplainText();
    });
  });
}

EXAMPLES.forEach((example) => {
  const address = example.address;
  const chip = document.createElement("button");
  const label = (example.label || "neutral").toLowerCase();
  chip.className = `try-hint try-${label}`;
  chip.type = "button";
  chip.textContent = `${example.label} ${address.slice(0, 6)}...${address.slice(-4)}`;
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
  setResultReady(false);
  lastAnalysis = null;
  if (resultSummary) {
    resultSummary.textContent = "";
  }
  trustBand.textContent = "--";
  trustScore.textContent = "--";
  explainText.textContent = "";
  setExplainOpen(false);
});

runButton.addEventListener("click", () => {
  const value = addressInput.value.trim();
  if (!value) {
    setStatus("Paste an address to check.", "error");
    return;
  }
  callApi(value);
});

explainButton.addEventListener("click", async () => {
  if (explainOpen) {
    setExplainOpen(false);
    return;
  }
  await toggleExplain();
  setExplainOpen(true);
});

setStatus("");
setResultReady(false);
setTrustDot("unknown");
setExplainOpen(false);
setExplainLoading(false);
setRunLoading(false);
