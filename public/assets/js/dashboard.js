/* ==========================================================================
   Philippine Commodity Price Tracker (Bantay Presyo)
   Modular Sidebar Router, Tree Controller & Plotly Visualization Engine
   ========================================================================== */

(function () {
  "use strict";

  const payload = window.DASHBOARD_DATA || {};
  const categoryChartPayload = payload.categoryChart || { data: [], layout: {} };
  const timeseriesData = payload.timeseries || {};
  const commodityMetadata = payload.commodityMeta || {};
  const defaultCommodity = payload.defaultCommodity || Object.keys(commodityMetadata)[0] || "";

  // Build slug <-> commodity lookup maps and ordered commodity list
  const slugToCommodity = {};
  const commodityList = Object.keys(commodityMetadata);
  commodityList.forEach((comm) => {
    const meta = commodityMetadata[comm];
    if (meta && meta.slug) {
      slugToCommodity[meta.slug] = comm;
    }
  });

  let activeView = "overview";
  let activeCommodity = defaultCommodity;
  let activeTimeframe = "ALL";
  let categoryChartInitialized = false;

  // ========================================================================
  // 1. Formatting Helpers
  // ========================================================================

  function formatBadgeHtml(val, isPct = true) {
    if (val === null || val === undefined || isNaN(val)) {
      return '<span class="badge badge-neutral">—</span>';
    }
    const num = Number(val);
    if (num > 0.0001) {
      const formatted = isPct ? `+${num.toFixed(2)}%` : `+₱${num.toFixed(2)}`;
      return `<span class="badge badge-increase"><span class="badge-arrow">▲</span> ${formatted}</span>`;
    } else if (num < -0.0001) {
      const formatted = isPct ? `${num.toFixed(2)}%` : `-₱${Math.abs(num).toFixed(2)}`;
      return `<span class="badge badge-decrease"><span class="badge-arrow">▼</span> ${formatted}</span>`;
    } else {
      const formatted = isPct ? `0.00%` : `₱0.00`;
      return `<span class="badge badge-neutral"><span class="badge-arrow">—</span> ${formatted}</span>`;
    }
  }

  function formatKpiHeadlineChange(val) {
    if (val === null || val === undefined || isNaN(val)) {
      return '<span class="text-flat">—</span>';
    }
    const num = Number(val);
    if (num > 0.0001) {
      return `<span class="text-gain"><span class="kpi-arrow">▲</span> +${num.toFixed(2)}%</span>`;
    } else if (num < -0.0001) {
      return `<span class="text-drop"><span class="kpi-arrow">▼</span> ${num.toFixed(2)}%</span>`;
    } else {
      return `<span class="text-flat">0.00%</span>`;
    }
  }

  function formatKpiDeltaSub(absVal, label) {
    if (absVal === null || absVal === undefined || isNaN(absVal)) {
      return `<span class="kpi-context">${label}</span>`;
    }
    const num = Number(absVal);
    const sign = num > 0.0001 ? "+" : num < -0.0001 ? "-" : "";
    const formatted = `${sign}₱${Math.abs(num).toFixed(2)}`;
    return `<span class="kpi-highlight font-mono">${formatted}</span> <span class="kpi-context">${label}</span>`;
  }

  // ========================================================================
  // 2. Category Indices Plotly Chart
  // ========================================================================

  function initCategoryChart() {
    const container = document.getElementById("category-index-plot");
    if (!container || typeof Plotly === "undefined") return;
    if (!categoryChartInitialized) {
      Plotly.newPlot(
        container,
        categoryChartPayload.data,
        categoryChartPayload.layout,
        { responsive: true, displayModeBar: "hover", displaylogo: false }
      );
      categoryChartInitialized = true;
    } else {
      Plotly.Plots.resize(container);
    }
  }

  // ========================================================================
  // 3. Commodity Detail View & Time-Series Chart Controller
  // ========================================================================

  function sliceTimeframe(arr, count) {
    if (!arr || count === null || count >= arr.length) return arr;
    return arr.slice(arr.length - count);
  }

  function getTimeframeCount(tf) {
    switch (tf) {
      case "1M":
        return 30;
      case "3M":
        return 90;
      case "6M":
        return 180;
      case "1Y":
        return 365;
      default:
        return null;
    }
  }

  function updateCommodityDetail(commName) {
    const meta = commodityMetadata[commName];
    const ts = timeseriesData[commName];
    if (!meta || !ts) return;

    activeCommodity = commName;

    // Unified Header & Chart Card Header updates
    const navTitleEl = document.getElementById("navbar-view-title");
    const bcGroupEl = document.getElementById("breadcrumb-group");
    const bcCommEl = document.getElementById("breadcrumb-commodity");
    const chartTitleEl = document.getElementById("commodity-chart-title");

    if (activeView === "commodity") {
      if (navTitleEl) navTitleEl.textContent = commName;
    }
    if (bcGroupEl) bcGroupEl.textContent = meta.commodity_group;
    if (bcCommEl) bcCommEl.textContent = commName;
    if (chartTitleEl) {
      chartTitleEl.textContent = `Daily Prevailing Retail Price, 7-Day SMA & 30-Day SMA — ${commName} (₱ / ${meta.unit})`;
    }

    // Update 7 Unified KPI Strip cards
    const boxPrice = document.getElementById("box-price");
    const boxUnit = document.getElementById("box-unit");
    const boxDod = document.getElementById("box-dod");
    const boxDodSub = document.getElementById("box-dod-sub");
    const boxWow = document.getElementById("box-wow");
    const boxWowSub = document.getElementById("box-wow-sub");
    const boxMom = document.getElementById("box-mom");
    const boxMomSub = document.getElementById("box-mom-sub");
    const boxSma7 = document.getElementById("box-sma7");
    const boxSma30 = document.getElementById("box-sma30");
    const boxVol = document.getElementById("box-vol");
    const boxCv = document.getElementById("box-cv");

    if (boxPrice) boxPrice.textContent = meta.price !== null ? `₱${meta.price.toFixed(2)}` : "—";
    if (boxUnit) {
      boxUnit.innerHTML = `<span class="kpi-context">Retailing per</span> <span class="kpi-highlight">${meta.unit}</span>`;
    }

    if (boxDod) boxDod.innerHTML = formatKpiHeadlineChange(meta.dod_pct);
    if (boxDodSub) boxDodSub.innerHTML = formatKpiDeltaSub(meta.dod_abs, "vs yesterday");

    if (boxWow) boxWow.innerHTML = formatKpiHeadlineChange(meta.wow_pct);
    if (boxWowSub) boxWowSub.innerHTML = formatKpiDeltaSub(meta.wow_abs, "vs 7d ago");

    if (boxMom) boxMom.innerHTML = formatKpiHeadlineChange(meta.mom_pct);
    if (boxMomSub) boxMomSub.innerHTML = formatKpiDeltaSub(meta.mom_abs, "vs 30d ago");

    if (boxSma7) boxSma7.textContent = meta.sma_7d !== null ? `₱${meta.sma_7d.toFixed(2)}` : "—";
    if (boxSma30) boxSma30.textContent = meta.sma_30d !== null ? `₱${meta.sma_30d.toFixed(2)}` : "—";
    if (boxVol) boxVol.textContent = meta.volatility_30d !== null ? `₱${meta.volatility_30d.toFixed(2)}` : "—";
    if (boxCv) {
      boxCv.innerHTML =
        meta.cv_30d !== null
          ? `<span class="kpi-context">CV:</span> <span class="kpi-highlight font-mono">${(meta.cv_30d * 100).toFixed(2)}%</span>`
          : `<span class="kpi-context">CV: —</span>`;
    }

    // Synchronize Quick Switch dropdown
    const picker = document.getElementById("commodity-picker");
    if (picker && picker.value !== commName) {
      picker.value = commName;
    }

    renderCommodityChart(commName);
  }

  function renderCommodityChart(commName) {
    const container = document.getElementById("commodity-trend-plot");
    if (!container || typeof Plotly === "undefined") return;

    const meta = commodityMetadata[commName];
    const ts = timeseriesData[commName];
    if (!meta || !ts) return;

    const sliceCount = getTimeframeCount(activeTimeframe);
    const dates = sliceTimeframe(ts.dates, sliceCount);
    const prices = sliceTimeframe(ts.prices, sliceCount);
    const sma7 = sliceTimeframe(ts.sma_7d, sliceCount);
    const sma30 = sliceTimeframe(ts.sma_30d, sliceCount);

    const traces = [
      {
        x: dates,
        y: prices,
        mode: "lines",
        name: "Prevailing Retail Price",
        line: { color: "#166534", width: 2.4 },
        hovertemplate:
          "%{x|%b %d, %Y}<br><b>Price:</b> ₱%{y:.2f} / " + meta.unit + "<extra></extra>",
      },
      {
        x: dates,
        y: sma7,
        mode: "lines",
        name: "7-Day SMA",
        line: { color: "#d97706", width: 1.8, dash: "dash" },
        hovertemplate: "%{x|%b %d, %Y}<br><b>7d SMA:</b> ₱%{y:.2f}<extra></extra>",
      },
      {
        x: dates,
        y: sma30,
        mode: "lines",
        name: "30-Day SMA",
        line: { color: "#64748b", width: 1.5, dash: "dot" },
        hovertemplate: "%{x|%b %d, %Y}<br><b>30d SMA:</b> ₱%{y:.2f}<extra></extra>",
      },
    ];

    const layout = {
      template: "plotly_white",
      hovermode: "x unified",
      margin: { l: 55, r: 20, t: 30, b: 40 },
      xaxis: {
        title: "",
        showgrid: true,
        gridcolor: "#f1f5f9",
        zeroline: false,
      },
      yaxis: {
        title: `Price (₱ / ${meta.unit})`,
        showgrid: true,
        gridcolor: "#f1f5f9",
        tickprefix: "₱",
        zeroline: false,
      },
      legend: {
        orientation: "h",
        yanchor: "bottom",
        y: 1.02,
        xanchor: "right",
        x: 1,
        font: { size: 11 },
      },
    };

    Plotly.react(container, traces, layout, {
      responsive: true,
      displayModeBar: "hover",
      displaylogo: false,
    });
  }

  // ========================================================================
  // 4. Sidebar View Router & URL Hash Synchronization
  // ========================================================================

  function activateView(viewName, commName, updateHash = true) {
    activeView = viewName;

    // 1. Toggle .view-panel sections
    document.querySelectorAll(".view-panel").forEach((panel) => {
      panel.classList.remove("active");
    });
    const targetPanel = document.getElementById(`view-${viewName}`);
    if (targetPanel) {
      targetPanel.classList.add("active");
    }

    // 2. Highlight top-level system nav buttons
    document.querySelectorAll(".sidebar-nav-btn").forEach((btn) => {
      if (btn.getAttribute("data-view") === viewName) {
        btn.classList.add("active");
      } else {
        btn.classList.remove("active");
      }
    });

    // 3. Highlight commodity tree button
    document.querySelectorAll(".sidebar-commodity-btn").forEach((btn) => {
      const btnComm = btn.getAttribute("data-commodity");
      if (viewName === "commodity" && btnComm === commName) {
        btn.classList.add("active");
        // Ensure parent group is expanded
        const parentGroup = btn.closest(".sidebar-group");
        if (parentGroup) {
          parentGroup.classList.add("expanded");
        }
      } else {
        btn.classList.remove("active");
      }
    });

    // 4. Close mobile drawer if open
    const sidebar = document.getElementById("dashboard-sidebar");
    if (sidebar) {
      sidebar.classList.remove("mobile-open");
    }

    // 5. Update Unified Navbar heading and controls
    const tagEl = document.getElementById("navbar-view-tag");
    const crumbTrailEl = document.getElementById("navbar-commodity-breadcrumb");
    const commControlsEl = document.getElementById("navbar-commodity-controls");
    const titleEl = document.getElementById("navbar-view-title");

    const isCommodityView = viewName === "commodity";
    if (tagEl) tagEl.style.display = isCommodityView ? "none" : "";
    if (crumbTrailEl) crumbTrailEl.style.display = isCommodityView ? "flex" : "none";
    if (commControlsEl) commControlsEl.style.display = isCommodityView ? "flex" : "none";

    if (titleEl) {
      if (viewName === "overview") {
        if (tagEl) tagEl.textContent = "Executive Summary";
        titleEl.textContent = "Market Overview";
      } else if (viewName === "census") {
        if (tagEl) tagEl.textContent = "Cross-Sectional Price Register";
        titleEl.textContent = "Full Price Census";
      } else if (viewName === "governance") {
        if (tagEl) tagEl.textContent = "Transparency & Provenance";
        titleEl.textContent = "Data Governance & Methodology";
      }
    }

    // 6. Render view-specific charts
    if (viewName === "overview") {
      initCategoryChart();
      if (updateHash) {
        history.replaceState(null, "", "#overview");
      }
    } else if (viewName === "commodity") {
      const targetComm = commName && commodityMetadata[commName] ? commName : activeCommodity;
      updateCommodityDetail(targetComm);
      const slug = commodityMetadata[targetComm]?.slug || "";
      if (updateHash && slug) {
        history.replaceState(null, "", `#commodity/${slug}`);
      }
    } else if (viewName === "census") {
      if (updateHash) {
        history.replaceState(null, "", "#census");
      }
    } else if (viewName === "governance") {
      if (updateHash) {
        history.replaceState(null, "", "#governance");
      }
    }

    const workspace = document.querySelector(".main-workspace");
    if (workspace) {
      workspace.scrollTo({ top: 0, behavior: "smooth" });
    }
    window.scrollTo({ top: 0, behavior: "smooth" });
  }

  function handleHashRoute() {
    const hash = window.location.hash.replace(/^#/, "").trim();
    if (!hash || hash === "overview") {
      activateView("overview", null, false);
    } else if (hash === "census") {
      activateView("census", null, false);
    } else if (hash === "governance") {
      activateView("governance", null, false);
    } else if (hash.startsWith("commodity/")) {
      const slug = hash.slice("commodity/".length);
      const comm = slugToCommodity[slug] || defaultCommodity;
      activateView("commodity", comm, false);
    } else {
      activateView("overview", null, false);
    }
  }

  // ========================================================================
  // 5. Event Listeners Setup
  // ========================================================================

  function bindEvents() {
    // Top-level system view buttons
    document.querySelectorAll("[data-nav-view]").forEach((btn) => {
      btn.addEventListener("click", () => {
        const view = btn.getAttribute("data-nav-view");
        activateView(view, null, true);
      });
    });

    // Collapsible category group headers in sidebar
    document.querySelectorAll(".sidebar-group-toggle").forEach((toggle) => {
      toggle.addEventListener("click", () => {
        const group = toggle.closest(".sidebar-group");
        if (group) {
          group.classList.toggle("expanded");
        }
      });
    });

    // Expand / Collapse All Groups button
    const expandAllBtn = document.getElementById("btn-expand-all-groups");
    if (expandAllBtn) {
      let allExpanded = false;
      expandAllBtn.addEventListener("click", () => {
        allExpanded = !allExpanded;
        document.querySelectorAll(".sidebar-group").forEach((grp) => {
          if (allExpanded) {
            grp.classList.add("expanded");
          } else {
            grp.classList.remove("expanded");
          }
        });
        expandAllBtn.textContent = allExpanded ? "Collapse All" : "Expand All";
      });
    }

    // Individual commodity buttons in sidebar
    document.querySelectorAll(".sidebar-commodity-btn").forEach((btn) => {
      btn.addEventListener("click", () => {
        const comm = btn.getAttribute("data-commodity");
        activateView("commodity", comm, true);
      });
    });

    // Fallback dropdown selector (if used directly)
    const picker = document.getElementById("commodity-picker");
    if (picker) {
      picker.addEventListener("change", (e) => {
        activateView("commodity", e.target.value, true);
      });
    }

    // Sidebar Quick Search Filter
    const sidebarSearch = document.getElementById("sidebar-commodity-search");
    if (sidebarSearch) {
      sidebarSearch.addEventListener("input", () => {
        const q = sidebarSearch.value.trim().toLowerCase();
        document.querySelectorAll(".sidebar-group").forEach((grp) => {
          const grpName = (grp.getAttribute("data-group-name") || "").toLowerCase();
          const itemBtns = grp.querySelectorAll(".sidebar-commodity-btn");
          let matchedInGroup = 0;

          itemBtns.forEach((btn) => {
            const cName = (btn.getAttribute("data-commodity") || "").toLowerCase();
            if (q === "" || cName.includes(q) || grpName.includes(q)) {
              btn.parentElement.style.display = "";
              matchedInGroup++;
            } else {
              btn.parentElement.style.display = "none";
            }
          });

          if (q === "") {
            grp.style.display = "";
          } else if (matchedInGroup > 0) {
            grp.style.display = "";
            grp.classList.add("expanded");
          } else {
            grp.style.display = "none";
          }
        });
      });
    }

    // Navbar Quick Search Filter
    const navSearch = document.getElementById("navbar-quick-search");
    if (navSearch) {
      navSearch.addEventListener("input", () => {
        const q = navSearch.value;
        if (sidebarSearch) {
          sidebarSearch.value = q;
          sidebarSearch.dispatchEvent(new Event("input"));
        }
        const masterSearch = document.getElementById("table-search-input");
        if (masterSearch && activeView === "census") {
          masterSearch.value = q;
          masterSearch.dispatchEvent(new Event("input"));
        }
      });
    }
    document.querySelectorAll(".btn-timeframe").forEach((btn) => {
      btn.addEventListener("click", () => {
        document.querySelectorAll(".btn-timeframe").forEach((b) => b.classList.remove("active"));
        btn.classList.add("active");
        activeTimeframe = btn.getAttribute("data-timeframe") || "ALL";
        renderCommodityChart(activeCommodity);
      });
    });

    // Previous / Next Commodity Stepper Buttons
    const prevBtn = document.getElementById("btn-prev-commodity");
    const nextBtn = document.getElementById("btn-next-commodity");
    if (prevBtn) {
      prevBtn.addEventListener("click", () => {
        const idx = commodityList.indexOf(activeCommodity);
        const prevIdx = (idx - 1 + commodityList.length) % commodityList.length;
        activateView("commodity", commodityList[prevIdx], true);
      });
    }
    if (nextBtn) {
      nextBtn.addEventListener("click", () => {
        const idx = commodityList.indexOf(activeCommodity);
        const nextIdx = (idx + 1) % commodityList.length;
        activateView("commodity", commodityList[nextIdx], true);
      });
    }

    // Clickable rows in Top Movers and Master Census Table
    document.querySelectorAll("tr[data-target-commodity]").forEach((row) => {
      row.addEventListener("click", () => {
        const comm = row.getAttribute("data-target-commodity");
        if (comm) {
          activateView("commodity", comm, true);
        }
      });
    });

    // Mobile sidebar toggle button
    const mobileToggle = document.getElementById("btn-sidebar-toggle");
    const sidebar = document.getElementById("dashboard-sidebar");
    if (mobileToggle && sidebar) {
      mobileToggle.addEventListener("click", () => {
        sidebar.classList.toggle("mobile-open");
      });
    }

    // Master Table Search & Category Filter
    const searchInput = document.getElementById("table-search-input");
    const filterButtons = document.querySelectorAll("#category-filter-buttons .btn-filter");
    const tableRows = document.querySelectorAll("#master-price-table tbody tr");
    const rowCounter = document.getElementById("table-row-counter");
    let activeCategory = "ALL";

    function filterTable() {
      const query = searchInput ? searchInput.value.trim().toLowerCase() : "";
      let visibleCount = 0;

      tableRows.forEach((row) => {
        const comm = row.getAttribute("data-commodity") || "";
        const grp = row.getAttribute("data-group") || "";
        const matchesCategory = activeCategory === "ALL" || grp === activeCategory;
        const matchesSearch =
          query === "" || comm.includes(query) || grp.toLowerCase().includes(query);

        if (matchesCategory && matchesSearch) {
          row.style.display = "";
          visibleCount++;
        } else {
          row.style.display = "none";
        }
      });

      if (rowCounter) {
        rowCounter.textContent = `Showing ${visibleCount} of ${tableRows.length} commodities`;
      }
    }

    if (searchInput) {
      searchInput.addEventListener("input", filterTable);
    }

    filterButtons.forEach((btn) => {
      btn.addEventListener("click", () => {
        filterButtons.forEach((b) => b.classList.remove("active"));
        btn.classList.add("active");
        activeCategory = btn.getAttribute("data-category") || "ALL";
        filterTable();
      });
    });

    // Column Sorting for Master Census Table
    const tableHeaders = document.querySelectorAll("#master-price-table th.sortable");
    let sortColumn = null;
    let sortAsc = true;

    tableHeaders.forEach((th) => {
      th.addEventListener("click", () => {
        const colKey = th.getAttribute("data-sort");
        if (sortColumn === colKey) {
          sortAsc = !sortAsc;
        } else {
          sortColumn = colKey;
          sortAsc = true;
        }

        tableHeaders.forEach((h) => {
          const text = h.textContent.replace(/[▲▼⇅]/g, "").trim();
          if (h === th) {
            h.textContent = `${text} ${sortAsc ? "▲" : "▼"}`;
          } else {
            h.textContent = `${text} ⇅`;
          }
        });

        const tbody = document.querySelector("#master-price-table tbody");
        const rowsArray = Array.from(tableRows);

        rowsArray.sort((a, b) => {
          if (colKey === "commodity") {
            const valA = a.querySelector(".commodity-cell").textContent.trim();
            const valB = b.querySelector(".commodity-cell").textContent.trim();
            return sortAsc ? valA.localeCompare(valB) : valB.localeCompare(valA);
          } else if (colKey === "group") {
            const valA = a.getAttribute("data-group") || "";
            const valB = b.getAttribute("data-group") || "";
            return sortAsc ? valA.localeCompare(valB) : valB.localeCompare(valA);
          } else {
            const colIndex = Array.from(th.parentNode.children).indexOf(th);
            const tdA = a.children[colIndex];
            const tdB = b.children[colIndex];
            const valA = parseFloat(tdA.getAttribute("data-val") || "0");
            const valB = parseFloat(tdB.getAttribute("data-val") || "0");
            return sortAsc ? valA - valB : valB - valA;
          }
        });

        rowsArray.forEach((r) => tbody.appendChild(r));
      });
    });

    window.addEventListener("hashchange", handleHashRoute);
  }

  // Initialize on DOMContentLoaded
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", () => {
      bindEvents();
      handleHashRoute();
    });
  } else {
    bindEvents();
    handleHashRoute();
  }
})();
