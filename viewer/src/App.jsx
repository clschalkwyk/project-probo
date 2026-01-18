import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  forceCenter,
  forceCollide,
  forceLink,
  forceManyBody,
  forceSimulation,
  forceX,
  forceY,
} from "d3-force";

const DB_NAME = "probo-viewer";
const DB_VERSION = 1;
const STORE_NAME = "jsonfiles";

const formatNumber = (value, digits = 2) => {
  if (value === null || value === undefined) return "n/a";
  if (Number.isNaN(value)) return "n/a";
  return Number(value).toLocaleString(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: digits,
  });
};

const formatIso = (value) => {
  if (!value) return "n/a";
  if (typeof value === "string") return value;
  const date = new Date(value * 1000);
  return date.toISOString();
};

const openDb = () =>
  new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION);
    request.onerror = () => reject(request.error);
    request.onsuccess = () => resolve(request.result);
    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(STORE_NAME)) {
        db.createObjectStore(STORE_NAME, { keyPath: "id" });
      }
    };
  });

const withStore = async (mode, fn) => {
  const db = await openDb();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_NAME, mode);
    const store = tx.objectStore(STORE_NAME);
    const result = fn(store);
    tx.oncomplete = () => resolve(result);
    tx.onerror = () => reject(tx.error);
  });
};

const saveFile = async (name, payload) => {
  const record = {
    id: `${name}-${Date.now()}`,
    name,
    createdAt: Date.now(),
    payload,
  };
  await withStore("readwrite", (store) => store.put(record));
  return record;
};

const listFiles = async () =>
  withStore("readonly", (store) => {
    return new Promise((resolve, reject) => {
      const request = store.getAll();
      request.onsuccess = () => resolve(request.result || []);
      request.onerror = () => reject(request.error);
    });
  });

const getFile = async (id) =>
  withStore("readonly", (store) => {
    return new Promise((resolve, reject) => {
      const request = store.get(id);
      request.onsuccess = () => resolve(request.result || null);
      request.onerror = () => reject(request.error);
    });
  });

const removeFile = async (id) =>
  withStore("readwrite", (store) => store.delete(id));

const clearFiles = async () => withStore("readwrite", (store) => store.clear());

const parseAmount = (value) => {
  if (value === null || value === undefined) return 0;
  if (typeof value === "number") return value;
  const parsed = Number(value);
  return Number.isNaN(parsed) ? 0 : parsed;
};

const safeLower = (value) => (value ? String(value).toLowerCase() : "");

const TRANSFER_SAMPLE_LIMIT = 5000;
const GRAPH_TRANSFER_LIMIT = 2000;

const deriveStats = (payload) => {
  const address = payload?.address || "";
  const transfers = payload?.transfers || [];
  const sample = transfers.length > TRANSFER_SAMPLE_LIMIT
    ? transfers.slice(0, TRANSFER_SAMPLE_LIMIT)
    : transfers;
  const seed = address.toLowerCase();
  const categories = new Map();
  const assets = new Map();
  const counterparties = new Map();
  const dayBuckets = new Map();
  let inCount = 0;
  let outCount = 0;
  let totalIn = 0;
  let totalOut = 0;
  let firstSeen = null;
  let lastSeen = null;

  sample.forEach((item) => {
    const category = item.category || "unknown";
    categories.set(category, (categories.get(category) || 0) + 1);

    const asset = item.asset || "unknown";
    assets.set(asset, (assets.get(asset) || 0) + 1);

    const fromAddr = safeLower(item.from);
    const toAddr = safeLower(item.to);
    const value = parseAmount(item.value);

    let ts = null;
    if (item.blockTimestamp) {
      ts = Date.parse(item.blockTimestamp) / 1000;
    } else if (item.metadata?.blockTimestamp) {
      ts = Date.parse(item.metadata.blockTimestamp) / 1000;
    }
    if (!Number.isNaN(ts) && ts) {
      firstSeen = firstSeen === null ? ts : Math.min(firstSeen, ts);
      lastSeen = lastSeen === null ? ts : Math.max(lastSeen, ts);
      const day = new Date(ts * 1000).toISOString().slice(0, 10);
      dayBuckets.set(day, (dayBuckets.get(day) || 0) + 1);
    }

    if (fromAddr === seed) {
      outCount += 1;
      totalOut += value;
      if (toAddr) counterparties.set(toAddr, (counterparties.get(toAddr) || 0) + 1);
    } else if (toAddr === seed) {
      inCount += 1;
      totalIn += value;
      if (fromAddr) counterparties.set(fromAddr, (counterparties.get(fromAddr) || 0) + 1);
    }
  });

  const topEntries = (map, limit = 6) =>
    Array.from(map.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, limit);

  const daysSorted = Array.from(dayBuckets.entries()).sort((a, b) =>
    a[0].localeCompare(b[0])
  );
  const dayBucketsLimited = daysSorted.slice(-90);

  return {
    firstSeen,
    lastSeen,
    inCount,
    outCount,
    totalIn,
    totalOut,
    categories: topEntries(categories),
    assets: topEntries(assets),
    counterparties: topEntries(counterparties),
    dayBuckets: dayBucketsLimited,
    sampled: transfers.length > sample.length,
    sampleSize: sample.length,
    totalTransfers: transfers.length,
  };
};

const GRAPH_LIMITS = {
  maxNodes: 240,
  maxLinks: 380,
};

const TYPE_COLORS = {
  address: "#2f6bff",
  contract: "#ff8c3a",
  token: "#1f9d7a",
  coin: "#7f5af0",
};

const nodeLabel = (node) => {
  if (!node) return "";
  if (node.type === "coin") return node.label || node.id;
  if (node.type === "token") return node.label || node.id.slice(0, 6);
  if (node.isSeed) return "seed";
  return node.id.slice(0, 6);
};

const buildGraph = (payload) => {
  if (!payload) return null;
  const transfers = payload.transfers || [];
  const sample = transfers.length > GRAPH_TRANSFER_LIMIT
    ? transfers.slice(0, GRAPH_TRANSFER_LIMIT)
    : transfers;
  const tokenMeta = payload.token_metadata || {};
  const tokenBalances = payload.token_balances?.tokenBalances || [];
  const seed = (payload.address || "").toLowerCase();
  const nodeMap = new Map();
  const linkMap = new Map();
  const tokenEdgeMap = new Map();
  const coinEdgeMap = new Map();
  const contractCandidates = new Set();

  const addNode = (id, type = "address", label = "") => {
    if (!id) return;
    const key = id.toLowerCase();
    if (!nodeMap.has(key)) {
      nodeMap.set(key, {
        id: key,
        label,
        type,
        degree: 0,
        isSeed: key === seed,
      });
    } else if (type !== "address") {
      const node = nodeMap.get(key);
      node.type = node.type === "address" ? type : node.type;
    }
  };

  const addLink = (source, target, type = "transfer") => {
    const s = source.toLowerCase();
    const t = target.toLowerCase();
    if (!s || !t || s === t) return;
    const key = `${s}::${t}::${type}`;
    const current = linkMap.get(key);
    if (current) {
      current.value += 1;
    } else {
      linkMap.set(key, { source: s, target: t, value: 1, type });
    }
  };

  sample.forEach((item) => {
    const fromAddr = safeLower(item.from);
    const toAddr = safeLower(item.to);
    if (fromAddr && toAddr) {
      addNode(fromAddr, "address");
      addNode(toAddr, "address");
      addLink(fromAddr, toAddr, "transfer");
    }
    const rawContract = item.rawContract?.address;
    if (rawContract) {
      contractCandidates.add(rawContract.toLowerCase());
    }
    const asset = item.asset;
    if (!rawContract && asset) {
      const coinId = `coin:${asset}`;
      addNode(coinId, "coin", asset);
      if (fromAddr) coinEdgeMap.set(`${coinId}::${fromAddr}`, (coinEdgeMap.get(`${coinId}::${fromAddr}`) || 0) + 1);
      if (toAddr) coinEdgeMap.set(`${coinId}::${toAddr}`, (coinEdgeMap.get(`${coinId}::${toAddr}`) || 0) + 1);
    }
  });

  Object.keys(tokenMeta).forEach((address) => {
    const metadata = tokenMeta[address]?.metadata || {};
    const label = metadata.symbol || metadata.name || address.slice(0, 6);
    addNode(address.toLowerCase(), "token", label);
  });

  contractCandidates.forEach((address) => {
    addNode(address, "contract");
  });

  const hasNonZeroBalance = (value) => {
    if (!value) return false;
    if (typeof value === "string") {
      if (value.startsWith("0x")) {
        return value !== "0x0";
      }
      return Number(value) !== 0;
    }
    return Number(value) !== 0;
  };

  tokenBalances.forEach((item) => {
    const address = item.contractAddress;
    if (!address) return;
    if (!hasNonZeroBalance(item.tokenBalance)) return;
    addNode(address.toLowerCase(), "token");
    addLink(seed, address.toLowerCase(), "balance");
  });

  sample.forEach((item) => {
    const contract = item.rawContract?.address;
    if (!contract) return;
    const tokenId = contract.toLowerCase();
    if (!nodeMap.has(tokenId)) {
      addNode(tokenId, "contract");
    }
    const fromAddr = safeLower(item.from);
    const toAddr = safeLower(item.to);
    if (fromAddr) tokenEdgeMap.set(`${tokenId}::${fromAddr}`, (tokenEdgeMap.get(`${tokenId}::${fromAddr}`) || 0) + 1);
    if (toAddr) tokenEdgeMap.set(`${tokenId}::${toAddr}`, (tokenEdgeMap.get(`${tokenId}::${toAddr}`) || 0) + 1);
  });

  tokenEdgeMap.forEach((count, key) => {
    const [tokenId, addr] = key.split("::");
    addLink(tokenId, addr, "token");
    const linkKey = `${tokenId}::${addr}::token`;
    const link = linkMap.get(linkKey);
    if (link) link.value = count;
  });

  coinEdgeMap.forEach((count, key) => {
    const [coinId, addr] = key.split("::");
    addLink(coinId, addr, "coin");
    const linkKey = `${coinId}::${addr}::coin`;
    const link = linkMap.get(linkKey);
    if (link) link.value = count;
  });

  const nodes = Array.from(nodeMap.values());
  const links = Array.from(linkMap.values());

  nodes.forEach((node) => {
    node.degree = 0;
  });
  links.forEach((link) => {
    const source = nodeMap.get(link.source);
    const target = nodeMap.get(link.target);
    if (source) source.degree += link.value;
    if (target) target.degree += link.value;
  });

  const rankedNodes = nodes
    .sort((a, b) => b.degree - a.degree)
    .slice(0, GRAPH_LIMITS.maxNodes);
  const keep = new Set(rankedNodes.map((node) => node.id));
  const filteredLinks = links
    .filter((link) => keep.has(link.source) && keep.has(link.target))
    .sort((a, b) => b.value - a.value)
    .slice(0, GRAPH_LIMITS.maxLinks);

  return {
    nodes: rankedNodes,
    links: filteredLinks,
    sampled: transfers.length > sample.length,
    sampleSize: sample.length,
    totalTransfers: transfers.length,
  };
};

const ForceGraph = ({ graph }) => {
  const canvasRef = useRef(null);
  const wrapperRef = useRef(null);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [isActive, setIsActive] = useState(false);
  const [hoveredNode, setHoveredNode] = useState(null);
  const [hoverPos, setHoverPos] = useState({ x: 0, y: 0 });
  const hoveredRef = useRef(null);
  const transformRef = useRef({ scale: 1, x: 0, y: 0 });
  const userTransformRef = useRef(false);

  useEffect(() => {
    const canvas = canvasRef.current;
    const wrapper = wrapperRef.current;
    if (!canvas || !wrapper || !graph || !isActive) return;

    let width = wrapper.clientWidth;
    let height = Math.max(360, wrapper.clientHeight || 420);
    canvas.width = width * window.devicePixelRatio;
    canvas.height = height * window.devicePixelRatio;
    canvas.style.width = `${width}px`;
    canvas.style.height = `${height}px`;
    const context = canvas.getContext("2d");
    context.scale(window.devicePixelRatio, window.devicePixelRatio);

    const nodes = graph.nodes.map((node) => ({ ...node }));
    const links = graph.links.map((link) => ({ ...link }));
    let draggingNode = null;
    let selectedNode = null;
    let panning = false;
    let panStart = { x: 0, y: 0 };
    let rafId = null;
    const padding = 16;

    const simulation = forceSimulation(nodes)
      .force(
        "link",
        forceLink(links)
          .id((d) => d.id)
          .distance((d) => (d.type === "token" || d.type === "coin" ? 90 : 60))
          .strength(0.55)
      )
      .force("charge", forceManyBody().strength(-45))
      .force("center", forceCenter(width / 2, height / 2))
      .force("collide", forceCollide(14))
      .force("x", forceX(width / 2).strength(0.06))
      .force("y", forceY(height / 2).strength(0.06));

    const draw = () => {
      const { scale, x, y } = transformRef.current;
      const minX = padding / scale;
      const minY = padding / scale;
      const maxX = (width - padding) / scale;
      const maxY = (height - padding) / scale;
      nodes.forEach((node) => {
        if (!Number.isFinite(node.x) || !Number.isFinite(node.y)) return;
        node.x = Math.min(maxX, Math.max(minX, node.x));
        node.y = Math.min(maxY, Math.max(minY, node.y));
      });
      context.clearRect(0, 0, width, height);
      context.save();
      context.translate(x, y);
      context.scale(scale, scale);
      context.save();
      context.globalAlpha = 0.4;
      links.forEach((link) => {
        context.beginPath();
        const linkColor =
          link.type === "transfer"
            ? "#c9b7a6"
            : link.type === "token"
              ? "#1f9d7a"
              : link.type === "balance"
                ? "#ff8c3a"
                : "#7f5af0";
        context.strokeStyle = linkColor;
        context.lineWidth = Math.max(0.5, Math.log(link.value + 1));
        context.moveTo(link.source.x, link.source.y);
        context.lineTo(link.target.x, link.target.y);
        context.stroke();

        if (link.type === "transfer") {
          const angle = Math.atan2(
            link.target.y - link.source.y,
            link.target.x - link.source.x
          );
          const arrowSize = 6;
          const offset = 10;
          const tx = link.target.x - Math.cos(angle) * offset;
          const ty = link.target.y - Math.sin(angle) * offset;
          context.beginPath();
          context.fillStyle = linkColor;
          context.moveTo(tx, ty);
          context.lineTo(
            tx - Math.cos(angle - Math.PI / 6) * arrowSize,
            ty - Math.sin(angle - Math.PI / 6) * arrowSize
          );
          context.lineTo(
            tx - Math.cos(angle + Math.PI / 6) * arrowSize,
            ty - Math.sin(angle + Math.PI / 6) * arrowSize
          );
          context.closePath();
          context.fill();
        }
      });
      context.restore();

      const hovered = hoveredRef.current;
      const maxDegree = Math.max(1, ...nodes.map((node) => node.degree || 0));
      const lerp = (a, b, t) => Math.round(a + (b - a) * t);
      const colorForNode = (node) => {
        const base = TYPE_COLORS[node.type] || "#2f6bff";
        const strength = Math.min(1, Math.log1p(node.degree || 0) / Math.log1p(maxDegree));
        const [r, g, b] = [
          parseInt(base.slice(1, 3), 16),
          parseInt(base.slice(3, 5), 16),
          parseInt(base.slice(5, 7), 16),
        ];
        const mix = 0.25 + 0.75 * strength;
        return `rgb(${lerp(255, r, mix)}, ${lerp(255, g, mix)}, ${lerp(255, b, mix)})`;
      };

      nodes.forEach((node) => {
        const base = node.isSeed ? 7 : node.type === "token" || node.type === "coin" ? 5 : 4;
        const scaled = base + Math.min(6, Math.log1p(node.degree || 0));
        const radius = Math.max(base, scaled);
        context.beginPath();
        context.fillStyle = colorForNode(node);
        context.moveTo(node.x + radius, node.y);
        context.arc(node.x, node.y, radius, 0, Math.PI * 2);
        context.fill();

        if (hovered && hovered.id === node.id) {
          context.beginPath();
          context.strokeStyle = "#15110e";
          context.lineWidth = 1.5;
          context.arc(node.x, node.y, radius + 4, 0, Math.PI * 2);
          context.stroke();
        }

        if (node.fx != null && node.fy != null) {
          context.beginPath();
          context.strokeStyle = "#15110e";
          context.setLineDash([4, 4]);
          context.lineWidth = 1;
          context.arc(node.x, node.y, radius + 6, 0, Math.PI * 2);
          context.stroke();
          context.setLineDash([]);
        }

        if (node.isSeed || node.type !== "address") {
          context.fillStyle = "#2d221a";
          context.font = "11px Space Grotesk";
          context.fillText(nodeLabel(node), node.x + radius + 4, node.y + 3);
        }
      });
      context.restore();
    };

    simulation.on("tick", draw);

    const fitToView = () => {
      if (userTransformRef.current) return;
      let minX = Infinity;
      let maxX = -Infinity;
      let minY = Infinity;
      let maxY = -Infinity;
      nodes.forEach((node) => {
        if (!Number.isFinite(node.x) || !Number.isFinite(node.y)) return;
        minX = Math.min(minX, node.x);
        maxX = Math.max(maxX, node.x);
        minY = Math.min(minY, node.y);
        maxY = Math.max(maxY, node.y);
      });
      if (!Number.isFinite(minX)) return;
      const graphWidth = Math.max(1, maxX - minX);
      const graphHeight = Math.max(1, maxY - minY);
      const scale = Math.min(1.4, Math.max(0.6, 0.9 * Math.min(width / graphWidth, height / graphHeight)));
      const x = width / 2 - ((minX + maxX) / 2) * scale;
      const y = height / 2 - ((minY + maxY) / 2) * scale;
      transformRef.current = { scale, x, y };
    };

    const resize = () => {
      width = wrapper.clientWidth;
      height = Math.max(360, wrapper.clientHeight || 420);
      canvas.width = width * window.devicePixelRatio;
      canvas.height = height * window.devicePixelRatio;
      canvas.style.width = `${width}px`;
      canvas.style.height = `${height}px`;
      context.setTransform(window.devicePixelRatio, 0, 0, window.devicePixelRatio, 0, 0);
      transformRef.current = { scale: 1, x: 0, y: 0 };
      userTransformRef.current = false;
      simulation.force("center", forceCenter(width / 2, height / 2));
      simulation.force("x", forceX(width / 2).strength(0.06));
      simulation.force("y", forceY(height / 2).strength(0.06));
      simulation.alpha(0.8).restart();
    };

    const observer = new ResizeObserver(resize);
    observer.observe(wrapper);

    const getPointer = (event) => {
      const rect = canvas.getBoundingClientRect();
      return {
        x: event.clientX - rect.left,
        y: event.clientY - rect.top,
      };
    };

    const findNode = (pos) => {
      const { scale, x, y } = transformRef.current;
      const gx = (pos.x - x) / scale;
      const gy = (pos.y - y) / scale;
      for (let i = nodes.length - 1; i >= 0; i -= 1) {
        const node = nodes[i];
        const radius = node.isSeed ? 10 : node.type === "token" || node.type === "coin" ? 8 : 6;
        const dx = node.x - gx;
        const dy = node.y - gy;
        if (dx * dx + dy * dy <= radius * radius) {
          return node;
        }
      }
      return null;
    };

    const redraw = () => {
      if (rafId) cancelAnimationFrame(rafId);
      rafId = requestAnimationFrame(draw);
    };

    const onMove = (event) => {
      const pos = getPointer(event);
      setHoverPos(pos);
      if (draggingNode) {
        const { scale, x, y } = transformRef.current;
        draggingNode.fx = (pos.x - x) / scale;
        draggingNode.fy = (pos.y - y) / scale;
        simulation.alpha(0.6).restart();
        redraw();
        return;
      }
      if (panning) {
        const dx = pos.x - panStart.x;
        const dy = pos.y - panStart.y;
        userTransformRef.current = true;
        transformRef.current = {
          ...transformRef.current,
          x: transformRef.current.x + dx,
          y: transformRef.current.y + dy,
        };
        panStart = pos;
        redraw();
        return;
      }
      const found = findNode(pos);
      hoveredRef.current = found;
      setHoveredNode(found);
      if (found) {
        selectedNode = found;
      }
      redraw();
    };

    const onDown = (event) => {
      const pos = getPointer(event);
      const found = findNode(pos);
      if (found) {
        draggingNode = found;
        selectedNode = found;
        const { scale, x, y } = transformRef.current;
        draggingNode.fx = (pos.x - x) / scale;
        draggingNode.fy = (pos.y - y) / scale;
        simulation.alphaTarget(0.3).restart();
      } else {
        panning = true;
        panStart = pos;
      }
    };

    const onUp = () => {
      if (draggingNode) {
        draggingNode = null;
        simulation.alphaTarget(0);
      }
      panning = false;
    };

    const onKey = (event) => {
      if (event.key.toLowerCase() !== "p") return;
      if (!selectedNode) return;
      if (selectedNode.fx == null && selectedNode.fy == null) {
        selectedNode.fx = selectedNode.x;
        selectedNode.fy = selectedNode.y;
      } else {
        selectedNode.fx = null;
        selectedNode.fy = null;
      }
      redraw();
    };

    const onWheel = (event) => {
      event.preventDefault();
      const delta = Math.sign(event.deltaY);
      const scale = transformRef.current.scale;
      const nextScale = Math.min(3, Math.max(0.4, scale - delta * 0.1));
      if (nextScale === scale) return;
      const pos = getPointer(event);
      const { x, y } = transformRef.current;
      const dx = pos.x - x;
      const dy = pos.y - y;
      const ratio = nextScale / scale;
      userTransformRef.current = true;
      transformRef.current = {
        scale: nextScale,
        x: pos.x - dx * ratio,
        y: pos.y - dy * ratio,
      };
      redraw();
    };

    canvas.addEventListener("mousemove", onMove);
    canvas.addEventListener("mousedown", onDown);
    canvas.addEventListener("wheel", onWheel, { passive: false });
    window.addEventListener("mouseup", onUp);
    window.addEventListener("keydown", onKey);
    canvas.addEventListener("mouseleave", () => {
      if (!draggingNode) {
        hoveredRef.current = null;
        setHoveredNode(null);
      }
    });

    const fitTimer = setTimeout(() => {
      fitToView();
      redraw();
    }, 700);

    return () => {
      observer.disconnect();
      simulation.stop();
      canvas.removeEventListener("mousemove", onMove);
      canvas.removeEventListener("mousedown", onDown);
      canvas.removeEventListener("wheel", onWheel);
      window.removeEventListener("mouseup", onUp);
      window.removeEventListener("keydown", onKey);
      clearTimeout(fitTimer);
      if (rafId) cancelAnimationFrame(rafId);
    };
  }, [graph, isActive]);

  useEffect(() => {
    const onChange = () => {
      const active = Boolean(document.fullscreenElement);
      setIsFullscreen(active);
      if (!active) {
        setIsActive(false);
      }
    };
    document.addEventListener("fullscreenchange", onChange);
    return () => document.removeEventListener("fullscreenchange", onChange);
  }, []);

  if (!graph) return null;

  return (
    <div className="graph" ref={wrapperRef}>
      <div className="graph-controls">
        <button
          className="ghost"
          onClick={() => {
            if (!wrapperRef.current) return;
            if (document.fullscreenElement) {
              document.exitFullscreen();
              return;
            }
            setIsActive(true);
            wrapperRef.current.requestFullscreen();
          }}
        >
          {isFullscreen ? "Exit fullscreen" : "Launch graph"}
        </button>
        <span className="hint muted">Scroll zoom, drag pan, press P to pin</span>
      </div>
      {isActive ? (
        <>
          <canvas ref={canvasRef} />
      {hoveredNode && (
            <div
              className="tooltip"
              style={{ left: hoverPos.x + 12, top: hoverPos.y + 12 }}
            >
              <div className="mono">{hoveredNode.id}</div>
              <div className="muted">type: {hoveredNode.type}</div>
              <div className="muted">degree: {hoveredNode.degree}</div>
            </div>
          )}
        </>
      ) : (
        <div className="graph-placeholder">
          <h4>Graph ready</h4>
          <p className="muted">Launch it in fullscreen to explore connections.</p>
        </div>
      )}
      <div className="legend">
        <span className="legend-dot address">Address</span>
        <span className="legend-dot contract">Contract</span>
        <span className="legend-dot token">Token</span>
        <span className="legend-dot coin">Coin</span>
        <span className="legend-line transfer">Transfer</span>
        <span className="legend-line token">Token link</span>
        <span className="legend-line balance">Balance</span>
        <span className="legend-note">Size/color = degree</span>
      </div>
    </div>
  );
};

const TransferRow = ({ transfer }) => {
  const ts = transfer.blockTimestamp || transfer.metadata?.blockTimestamp;
  return (
    <tr>
      <td className="mono">{transfer.hash || "n/a"}</td>
      <td>{transfer.category || "unknown"}</td>
      <td>{transfer.asset || "unknown"}</td>
      <td className="mono">{transfer.from || "n/a"}</td>
      <td className="mono">{transfer.to || "n/a"}</td>
      <td className="mono">{transfer.value ?? "n/a"}</td>
      <td>{ts || "n/a"}</td>
    </tr>
  );
};

export default function App() {
  const [files, setFiles] = useState([]);
  const [activeId, setActiveId] = useState(null);
  const [payload, setPayload] = useState(null);
  const [error, setError] = useState("");
  const [filter, setFilter] = useState("");
  const [limit, setLimit] = useState(150);
  const [isDragging, setIsDragging] = useState(false);

  const refreshFiles = useCallback(async () => {
    const list = await listFiles();
    list.sort((a, b) => b.createdAt - a.createdAt);
    setFiles(list);
  }, []);

  useEffect(() => {
    refreshFiles().catch((err) => setError(err.message));
  }, [refreshFiles]);

  const handleFile = useCallback(
    async (file) => {
      try {
        const text = await file.text();
        const json = JSON.parse(text);
        const record = await saveFile(file.name, json);
        await refreshFiles();
        setActiveId(record.id);
        setPayload(json);
        setError("");
      } catch (err) {
        setError(err.message || "Failed to parse JSON.");
      }
    },
    [refreshFiles]
  );

  const handleDrop = useCallback(
    (event) => {
      event.preventDefault();
      setIsDragging(false);
      const file = event.dataTransfer.files?.[0];
      if (file) {
        handleFile(file);
      }
    },
    [handleFile]
  );

  const handleSelect = async (id) => {
    setActiveId(id);
    const record = await getFile(id);
    setPayload(record?.payload || null);
  };

  const handleDelete = async (id) => {
    await removeFile(id);
    await refreshFiles();
    if (activeId === id) {
      setActiveId(null);
      setPayload(null);
    }
  };

  const stats = useMemo(() => deriveStats(payload || {}), [payload]);
  const graph = useMemo(() => buildGraph(payload), [payload]);

  const filteredTransfers = useMemo(() => {
    if (!payload?.transfers) return [];
    const term = filter.trim().toLowerCase();
    const list = payload.transfers;
    if (!term) return list.slice(0, limit);
    return list
      .filter((item) => {
        return [
          item.hash,
          item.category,
          item.asset,
          item.from,
          item.to,
        ]
          .filter(Boolean)
          .some((value) => String(value).toLowerCase().includes(term));
      })
      .slice(0, limit);
  }, [payload, filter, limit]);

  const maxBucket = Math.max(1, ...stats.dayBuckets.map(([, count]) => count));

  return (
    <div className="app">
      <header className="hero">
        <div>
          <p className="eyebrow">Probo Field Viewer</p>
          <h1>What the chain is saying.</h1>
          <p className="subhead">
            Load an extraction JSON, cache it in the browser, and get a fast
            reality check on activity patterns.
          </p>
        </div>
        <div className="hero-card">
          <div
            className={`dropzone ${isDragging ? "dragging" : ""}`}
            onDragOver={(event) => {
              event.preventDefault();
              setIsDragging(true);
            }}
            onDragLeave={() => setIsDragging(false)}
            onDrop={handleDrop}
          >
            <p>Drop JSON here or upload</p>
            <input
              type="file"
              accept="application/json"
              onChange={(event) => {
                const file = event.target.files?.[0];
                if (file) handleFile(file);
              }}
            />
            <span>Stored in IndexedDB</span>
          </div>
          {error ? <div className="error">{error}</div> : null}
        </div>
      </header>

      <section className="grid">
        <aside className="panel">
          <div className="panel-header">
            <h2>Cached files</h2>
            <button className="ghost" onClick={() => clearFiles().then(refreshFiles)}>
              Clear
            </button>
          </div>
          <div className="file-list">
            {files.length === 0 && <p className="muted">No cached JSON yet.</p>}
            {files.map((file) => (
              <button
                key={file.id}
                className={`file-item ${file.id === activeId ? "active" : ""}`}
                onClick={() => handleSelect(file.id)}
              >
                <div>
                  <strong>{file.name}</strong>
                  <span>{new Date(file.createdAt).toLocaleString()}</span>
                </div>
                <span className="mono">{file.id.slice(-6)}</span>
                <span
                  className="delete"
                  onClick={(event) => {
                    event.stopPropagation();
                    handleDelete(file.id);
                  }}
                >
                  x
                </span>
              </button>
            ))}
          </div>
        </aside>

        <main className="panel">
          {!payload && (
            <div className="empty">
              <h2>Upload a JSON file</h2>
              <p>
                Use the extractor to generate a file, then drop it here to explore
                activity, counterparties, and fan-out.
              </p>
            </div>
          )}

          {payload && (
            <>
              <div className="panel-header">
                <div>
                  <h2>Snapshot</h2>
                  <p className="muted">{payload.address}</p>
                </div>
                <div className="pill">
                  Window {payload.window?.from_iso} {"->"} {payload.window?.to_iso}
                </div>
              </div>

              <div className="stats">
                <div>
                  <h3>Transfers</h3>
                  <p className="stat">{formatNumber(payload.counts?.transfers, 0)}</p>
                  <span className="muted">
                    truncated: {String(payload.transfers_truncated)}
                  </span>
                  {stats.sampled && (
                    <span className="muted">
                      sample: {stats.sampleSize} / {stats.totalTransfers}
                    </span>
                  )}
                </div>
                <div>
                  <h3>Tokens</h3>
                  <p className="stat">{formatNumber(payload.counts?.token_balances, 0)}</p>
                  <span className="muted">token balances</span>
                </div>
                <div>
                  <h3>Activity</h3>
                  <p className="stat">
                    {formatNumber(stats.inCount + stats.outCount, 0)}
                  </p>
                  <span className="muted">
                    in {stats.inCount} / out {stats.outCount}
                  </span>
                </div>
                <div>
                  <h3>Last seen</h3>
                  <p className="stat">{formatIso(stats.lastSeen)}</p>
                  <span className="muted">first {formatIso(stats.firstSeen)}</span>
                </div>
              </div>

              <div className="chart">
                <div className="chart-header">
                  <h3>Daily activity</h3>
                  <span className="muted">counts by day</span>
                </div>
                <div className="bar-grid">
                  {stats.dayBuckets.length === 0 && (
                    <p className="muted">No timestamps available.</p>
                  )}
                  {stats.dayBuckets.map(([day, count]) => (
                    <div key={day} className="bar-row">
                      <span className="mono">{day}</span>
                      <div className="bar">
                        <span style={{ width: `${(count / maxBucket) * 100}%` }} />
                      </div>
                      <span>{count}</span>
                    </div>
                  ))}
                </div>
              </div>

              <section className="two-col">
                <div>
                  <h3>Top categories</h3>
                  <ul>
                    {stats.categories.map(([name, count]) => (
                      <li key={name}>
                        <span>{name}</span>
                        <strong>{count}</strong>
                      </li>
                    ))}
                  </ul>
                </div>
                <div>
                  <h3>Top assets</h3>
                  <ul>
                    {stats.assets.map(([name, count]) => (
                      <li key={name}>
                        <span>{name}</span>
                        <strong>{count}</strong>
                      </li>
                    ))}
                  </ul>
                </div>
                <div>
                  <h3>Top counterparties</h3>
                  <ul>
                    {stats.counterparties.map(([name, count]) => (
                      <li key={name}>
                        <span className="mono">{name}</span>
                        <strong>{count}</strong>
                      </li>
                    ))}
                  </ul>
                </div>
                <div className="totals">
                  <h3>Total flow</h3>
                  <p className="stat">{formatNumber(stats.totalIn, 4)}</p>
                  <span className="muted">inflow</span>
                  <p className="stat">{formatNumber(stats.totalOut, 4)}</p>
                  <span className="muted">outflow</span>
                </div>
              </section>

              {payload.fanout && (
                <section className="fanout">
                  <h3>Fan-out</h3>
                  <div className="fanout-grid">
                    <div>
                      <span>Nodes</span>
                      <strong>{payload.fanout.nodes?.length || 0}</strong>
                    </div>
                    <div>
                      <span>Edges</span>
                      <strong>{payload.fanout.edges?.length || 0}</strong>
                    </div>
                    <div>
                      <span>Capped</span>
                      <strong>{String(payload.fanout.capped)}</strong>
                    </div>
                  </div>
                </section>
              )}

              {graph && (
                <section className="graph-panel">
                  <div className="panel-header">
                    <div>
                      <h3>Connection graph</h3>
                      <p className="muted">Addresses, tokens, and contracts (top links only).</p>
                    </div>
                    <span className="pill">Force layout</span>
                  </div>
                  {graph.sampled && (
                    <p className="muted">
                      Graph sample: {graph.sampleSize} / {graph.totalTransfers} transfers
                    </p>
                  )}
                  <ForceGraph graph={graph} />
                </section>
              )}

              <section className="table">
                <div className="panel-header">
                  <h3>Transfers</h3>
                  <div className="controls">
                    <input
                      placeholder="Filter by hash, address, asset..."
                      value={filter}
                      onChange={(event) => setFilter(event.target.value)}
                    />
                    <select
                      value={limit}
                      onChange={(event) => setLimit(Number(event.target.value))}
                    >
                      {[50, 150, 300, 600].map((value) => (
                        <option key={value} value={value}>
                          {value} rows
                        </option>
                      ))}
                    </select>
                  </div>
                </div>
                <div className="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>Hash</th>
                        <th>Type</th>
                        <th>Asset</th>
                        <th>From</th>
                        <th>To</th>
                        <th>Value</th>
                        <th>Timestamp</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filteredTransfers.map((transfer, index) => (
                        <TransferRow key={`${transfer.hash}-${index}`} transfer={transfer} />
                      ))}
                    </tbody>
                  </table>
                </div>
              </section>
            </>
          )}
        </main>
      </section>
    </div>
  );
}
