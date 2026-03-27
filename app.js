const TODAY = new Date().toISOString().slice(0, 10);

const MODULES = {
  medical: {
    id: "medical",
    tabLabel: "医药资讯优先级系统",
    kicker: "Medical News",
    title: "医药资讯优先级系统",
    description: "保留原有资讯优先级工作流，继续用于资讯更新建议、人工复核和导出。当前默认使用内置模拟数据，后续可直接替换为本地 Excel，或再扩展为线上接口。",
    sourceName: "本地模拟数据 / 医药资讯",
    sourceMeta: "当前加载的是拆分后的医药资讯独立数据集。后续如切换到 Excel 或接口，只影响医药资讯模块，不会影响 AACR 模块。",
    datasetName: "医药资讯优先级",
    rawRows: Array.isArray(window.MEDICAL_DATA) ? window.MEDICAL_DATA : [],
    columns: [
      {
        label: "药品",
        render: row => `
          <button class="drug-btn" onclick="openModal('${escapeJs(row.__id)}')">${escapeHtml(row.drugName || "-")}</button>
          <div class="subtext">${escapeHtml(row.institution || "-")}</div>
        `
      },
      { label: "总分", render: row => scorePill(row.totalScore) },
      { label: "优先级", render: row => priorityPill(getPriorityBucket(row.totalScore)) },
      { label: "优先更新建议", render: row => `<div class="summary-cell">${escapeHtml(row.recommendation || "-")}</div>` },
      { label: "资讯标题", render: row => linkCell(row.newsUrl, row.newsTitle) },
      { label: "新增日期", render: row => `<span class="mono">${escapeHtml(row.addDate || "-")}</span>` },
      { label: "更新状态", render: row => medicalStatusCell(row) }
    ]
  },
  aacr: {
    id: "aacr",
    tabLabel: "AACR 会议管线优先级系统",
    kicker: "AACR Pipeline",
    title: "AACR 会议相关管线优先级系统",
    description: "专门用于 AACR 摘要中的肿瘤管线优先级排序。数据与医药资讯完全分开，单独维护原始摘要、公司、靶点、技术类型、分项得分与摘要链接。",
    sourceName: "本地 Excel 抽取结果 / AACR",
    sourceMeta: "当前默认载入你提供的 AACR 评分结果示例。后续继续导入本地 Excel 时，只覆盖 AACR 模块自身数据。",
    datasetName: "AACR 管线优先级",
    rawRows: Array.isArray(window.AACR_DATA) ? window.AACR_DATA : [],
    columns: [
      {
        label: "管线",
        render: row => `
          ${row.url
            ? `<a class="title-link" href="${escapeHtml(row.url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(row.drug_name || "-")}</a>`
            : `<div>${escapeHtml(row.drug_name || "-")}</div>`}
        `
      },
      {
        label: "公司",
        render: row => `
          <div class="compact-text">${escapeHtml(row.company || "-")}</div>
          <div class="subtext compact-text">${escapeHtml(row.source_org_type || "-")}</div>
        `
      },
      {
        label: "靶点",
        render: row => `
          <div class="compact-text">${escapeHtml(row.target_meta || "-")}</div>
          <div class="subtext compact-text">${escapeHtml(row.target || "-")}</div>
        `
      },
      {
        label: "作用机制",
        render: row => `
          <div class="compact-text">${escapeHtml(row.MOA_meta || "-")}</div>
          <div class="subtext compact-text">${escapeHtml(row.modality || "-")}</div>
        `
      },
      {
        label: "总分",
        render: row => `<button class="drug-btn" onclick="openModal('${escapeJs(row.__id)}')">${scorePill(row.total_score)}</button>`
      },
      {
        label: "简述",
        render: row => `
          <div class="summary-cell" title="${escapeHtml(row.brief_reason || "-")}">
            ${escapeHtml(row.brief_reason || "-")}
          </div>
        `
      },
      {
        label: "ID",
        render: row => `
          <div class="id-cell">
            <button class="copy-id-btn" onclick="copyAacrId('${escapeJs(row.id || "")}', event)">${escapeHtml(row.id || "-")}</button>
          </div>
        `
      }
    ]
  }
};

const state = {
  activeModule: "medical",
  page: 1,
  perPage: 12,
  filters: {},
  filteredRows: [],
  selectedId: null
};

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function escapeJs(value) {
  return String(value ?? "").replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function scorePill(score) {
  const bucket = getPriorityBucket(score);
  return `<span class="score-pill ${bucket.className}">${formatScore(score)}</span>`;
}

function priorityPill(bucket) {
  const labelMap = { high: "高优先", mid: "中优先", low: "低优先" };
  return `<span class="priority-pill ${bucket.className}">${labelMap[bucket.key]}</span>`;
}

function formatScore(score) {
  if (score === null || score === undefined || score === "") return "-";
  const num = Number(score);
  return Number.isFinite(num) ? num.toFixed(num % 1 === 0 ? 0 : 1) : escapeHtml(score);
}

function getPriorityBucket(score) {
  const num = Number(score) || 0;
  if (num >= 75) return { key: "high", className: "pill-high" };
  if (num >= 50) return { key: "mid", className: "pill-mid" };
  return { key: "low", className: "pill-low" };
}

function toSafeArray(value) {
  return Array.isArray(value) ? value : [];
}

function normalizeMedicalRows(rows) {
  return toSafeArray(rows).map((row, index) => ({
    ...row,
    __id: `medical-${row.id ?? index + 1}`,
    __module: "medical",
    totalScore: Number(row.totalScore ?? row.total_score ?? 0),
    addDate: row.addDate || row.date || "",
    updateStatusText: /已|done/i.test(String(row.updateStatus || "")) ? "已更新" : "未更新",
    dimensions: Array.isArray(row.dimensions) ? row.dimensions : []
  }));
}

function normalizeAacrRows(rows) {
  return toSafeArray(rows).map((row, index) => ({
    ...row,
    __id: `aacr-${row.id ?? row.row_number ?? index + 1}`,
    __module: "aacr",
    total_score: Number(row.total_score ?? 0),
    institution_score: Number(row.institution_score ?? 0),
    track_score: Number(row.track_score ?? 0),
    milestone_score: Number(row.milestone_score ?? 0),
    field_heat_score: Number(row.field_heat_score ?? 0),
    bonus_score: Number(row.bonus_score ?? 0),
    source_org_type: row.source_org_type || row.org_type || "",
    target_meta: row.target_meta || row.target || "",
    MOA_meta: row.MOA_meta || row.modality || ""
  })).sort((a, b) => (Number(b.total_score) || 0) - (Number(a.total_score) || 0));
}

MODULES.medical.rows = normalizeMedicalRows(MODULES.medical.rawRows);
MODULES.aacr.rows = normalizeAacrRows(MODULES.aacr.rawRows);

function getActiveModule() {
  return MODULES[state.activeModule];
}

function getActiveRows() {
  return getActiveModule().rows;
}

function buildTabs() {
  const el = document.getElementById("moduleTabs");
  el.innerHTML = Object.values(MODULES).map(module => `
    <button class="module-tab ${module.id === state.activeModule ? "active" : ""}" onclick="switchModule('${module.id}')">
      ${module.tabLabel}
    </button>
  `).join("");
}

function buildSidebar() {
  const module = getActiveModule();
  const subtitle = module.id === "medical" ? "医药资讯模块筛选" : "AACR 模块筛选";
  document.getElementById("sidebarSubtitle").textContent = subtitle;

  const filters = state.filters[module.id] || getDefaultFilters(module.id);
  const content = [];

  content.push(`
    <div class="filter-group">
      <div class="filter-group-title">优先级</div>
      <div class="checkbox-list">
        ${priorityCheckbox("high", "高优先", filters.priority.high)}
        ${priorityCheckbox("mid", "中优先", filters.priority.mid)}
        ${priorityCheckbox("low", "低优先", filters.priority.low)}
      </div>
    </div>
  `);

  content.push(`
    <div class="filter-group">
      <div class="filter-group-title">总分范围</div>
      <div class="score-inputs">
        <input class="score-input" id="scoreMin" type="number" min="0" max="100" value="${filters.scoreMin}" oninput="applyFilters()">
        <input class="score-input" id="scoreMax" type="number" min="0" max="100" value="${filters.scoreMax}" oninput="applyFilters()">
      </div>
    </div>
  `);

  if (module.id === "medical") {
    content.push(`
      <div class="filter-group">
        <div class="filter-group-title">新增日期</div>
        <div class="date-inputs">
          <input class="date-input" id="dateFrom" type="date" value="${filters.dateFrom}" onchange="applyFilters()">
          <input class="date-input" id="dateTo" type="date" value="${filters.dateTo}" onchange="applyFilters()">
        </div>
      </div>
    `);
    content.push(`
      <div class="filter-group">
        <div class="filter-group-title">更新状态</div>
        <div class="checkbox-list">
          <label class="cb-item"><input type="checkbox" id="statusPending" ${filters.status.pending ? "checked" : ""} onchange="applyFilters()"><span class="dot dot-pending"></span>未更新</label>
          <label class="cb-item"><input type="checkbox" id="statusDone" ${filters.status.done ? "checked" : ""} onchange="applyFilters()"><span class="dot dot-updated"></span>已更新</label>
        </div>
      </div>
    `);
  } else {
    const modalityOptions = getDistinctValues(getActiveRows().map(row => row.MOA_meta)).slice(0, 50);
    content.push(`
      <div class="filter-group">
        <div class="filter-group-title">Company Type</div>
        <input class="search-input" id="aacrCompanyType" type="text" value="${escapeHtml(filters.companyType || "")}" placeholder="e.g. company / university / hospital" oninput="applyFilters()">
      </div>
    `);
    content.push(`
      <div class="filter-group">
        <div class="filter-group-title">作用机制</div>
        <select class="select-input" id="modalitySelect" onchange="applyFilters()">
          <option value="">全部作用机制</option>
          ${modalityOptions.map(v => `<option value="${escapeHtml(v)}" ${filters.modality === v ? "selected" : ""}>${escapeHtml(v)}</option>`).join("")}
        </select>
      </div>
    `);
    content.push(`
      <div class="filter-group">
        <div class="filter-group-title">关键词检索</div>
        <input class="search-input" id="aacrKeyword" type="text" value="${escapeHtml(filters.keyword)}" placeholder="检索药物名 / 公司 / 靶点 / 标题" oninput="applyFilters()">
      </div>
    `);
  }

  document.getElementById("sidebarContent").innerHTML = content.join("");
}

function priorityCheckbox(key, label, checked) {
  const dotClass = key === "high" ? "dot-high" : key === "mid" ? "dot-mid" : "dot-low";
  return `<label class="cb-item"><input type="checkbox" id="priority-${key}" ${checked ? "checked" : ""} onchange="applyFilters()"><span class="dot ${dotClass}"></span>${label}</label>`;
}

function getDefaultFilters(moduleId) {
  if (moduleId === "medical") {
    return {
      priority: { high: true, mid: true, low: true },
      scoreMin: 0,
      scoreMax: 100,
      dateFrom: "",
      dateTo: "",
      status: { pending: true, done: true }
    };
  }
  return {
    priority: { high: true, mid: true, low: true },
    scoreMin: 0,
    scoreMax: 100,
    keyword: "",
    companyType: "",
    company: "",
    targetMeta: "",
    moaMeta: ""
  };
}

function switchModule(moduleId) {
  state.activeModule = moduleId;
  state.page = 1;
  state.selectedId = null;
  if (!state.filters[moduleId]) state.filters[moduleId] = getDefaultFilters(moduleId);
  renderAll();
  closeModal();
}

function readFiltersFromDom() {
  const module = getActiveModule();
  const base = {
    priority: {
      high: document.getElementById("priority-high")?.checked ?? true,
      mid: document.getElementById("priority-mid")?.checked ?? true,
      low: document.getElementById("priority-low")?.checked ?? true
    },
    scoreMin: Number(document.getElementById("scoreMin")?.value || 0),
    scoreMax: Number(document.getElementById("scoreMax")?.value || 100)
  };

  if (module.id === "medical") {
    return {
      ...base,
      dateFrom: document.getElementById("dateFrom")?.value || "",
      dateTo: document.getElementById("dateTo")?.value || "",
      status: {
        pending: document.getElementById("statusPending")?.checked ?? true,
        done: document.getElementById("statusDone")?.checked ?? true
      }
    };
  }

  return {
    ...base,
    keyword: (document.getElementById("aacrKeyword")?.value || "").trim().toLowerCase(),
    companyType: document.getElementById("aacrCompanyType")?.value || "",
    company: document.getElementById("headCompanyFilter")?.value || "",
    targetMeta: document.getElementById("headTargetFilter")?.value || "",
    moaMeta: document.getElementById("headMoaFilter")?.value || "",
    pipeline: document.getElementById("headPipelineFilter")?.value || ""
  };
}

function applyFilters() {
  state.filters[state.activeModule] = readFiltersFromDom();
  state.page = 1;
  renderTableSection();
  renderStats();
}

function applyFiltersOnEnter(event) {
  if (event.key !== "Enter") return;
  event.preventDefault();
  applyFilters();
}

function resetFilters() {
  state.filters[state.activeModule] = getDefaultFilters(state.activeModule);
  state.page = 1;
  renderAll();
}

function getFilteredRows() {
  const module = getActiveModule();
  const filters = state.filters[module.id] || getDefaultFilters(module.id);
  const rows = module.rows;

  return rows.filter(row => {
    const bucket = getPriorityBucket(module.id === "medical" ? row.totalScore : row.total_score).key;
    if (!filters.priority[bucket]) return false;

    const score = module.id === "medical" ? row.totalScore : row.total_score;
    if (score < filters.scoreMin || score > filters.scoreMax) return false;

    if (module.id === "medical") {
      if (filters.dateFrom && String(row.addDate || "") < filters.dateFrom) return false;
      if (filters.dateTo && String(row.addDate || "") > filters.dateTo) return false;
      if (!filters.status.pending && row.updateStatusText === "未更新") return false;
      if (!filters.status.done && row.updateStatusText === "已更新") return false;
      return true;
    }

    if (filters.companyType && !String(row.source_org_type || "").toLowerCase().includes(filters.companyType.toLowerCase())) return false;
    if (filters.company && !String(row.company || "").toLowerCase().includes(filters.company.toLowerCase())) return false;
    if (filters.targetMeta && !String(row.target_meta || "").toLowerCase().includes(filters.targetMeta.toLowerCase())) return false;
    if (filters.moaMeta && !String(row.MOA_meta || "").toLowerCase().includes(filters.moaMeta.toLowerCase())) return false;
    if (filters.pipeline && !String(row.drug_name || "").toLowerCase().includes(filters.pipeline.toLowerCase())) return false;
    if (filters.keyword) {
      const haystack = [row.drug_name, row.company, row.target, row.title, row.brief_reason].join(" ").toLowerCase();
      if (!haystack.includes(filters.keyword)) return false;
    }
    return true;
  });
}

function renderHero() {
  const module = getActiveModule();
  const heroSection = document.getElementById("heroSection");
  heroSection.style.display = module.id === "aacr" ? "none" : "grid";
  document.getElementById("panelKicker").textContent = module.kicker;
  document.getElementById("pageTitle").textContent = module.title;
  document.getElementById("pageDesc").textContent = module.description;
  document.getElementById("sourceName").textContent = module.sourceName;
  document.getElementById("sourceMeta").textContent = module.sourceMeta;
  document.getElementById("tableTitle").textContent = `${module.title}总览`;
  document.getElementById("tableSubtitle").textContent = module.id === "medical"
    ? "医药资讯模块与 AACR 模块完全分离，当前只展示医药资讯模块的数据。"
    : "";
}

function renderStats() {
  const module = getActiveModule();
  const rows = getFilteredRows();
  const statsBar = document.getElementById("statsBar");

  if (module.id === "medical") {
    const high = rows.filter(row => row.totalScore >= 75).length;
    const mid = rows.filter(row => row.totalScore >= 50 && row.totalScore < 75).length;
    const today = rows.filter(row => row.addDate === TODAY).length;
    const pending = rows.filter(row => row.updateStatusText === "未更新").length;
    statsBar.innerHTML = [
      statCard("高优先级", high, "high"),
      statCard("中优先级", mid, "mid"),
      statCard("今日新增", today, "accent"),
      statCard("待更新条目", pending, "green")
    ].join("");
  } else {
    const high = rows.filter(row => row.total_score >= 75).length;
    const mid = rows.filter(row => row.total_score >= 50 && row.total_score < 75).length;
    const company = rows.filter(row => String(row.source_org_type || "").includes("公司")).length;
    const university = rows.filter(row => String(row.source_org_type || "").includes("大学")).length;
    statsBar.innerHTML = [
      statCard("高优先级管线", high, "high"),
      statCard("中优先级管线", mid, "mid"),
      statCard("公司来源", company, "accent"),
      statCard("大学来源", university, "green")
    ].join("");
  }
}

function statCard(label, value, tone) {
  return `
    <div class="stat-card">
      <div class="stat-label">${label}</div>
      <div class="stat-value ${tone}">${value}</div>
    </div>
  `;
}

function renderTableSection() {
  const module = getActiveModule();
  const rows = getFilteredRows();
  state.filteredRows = rows;

  document.getElementById("tableHead").innerHTML = module.id === "aacr"
    ? renderAacrTableHead()
    : `<tr>${module.columns.map(col => `<th>${col.label}</th>`).join("")}</tr>`;

  const start = (state.page - 1) * state.perPage;
  const pageRows = rows.slice(start, start + state.perPage);
  const body = document.getElementById("tableBody");

  if (!pageRows.length) {
    body.innerHTML = `<tr><td colspan="${module.columns.length}"><div class="empty-state">当前筛选条件下没有匹配数据。</div></td></tr>`;
  } else {
    body.innerHTML = pageRows.map(row => `
      <tr>${module.columns.map(col => `<td>${col.render(row)}</td>`).join("")}</tr>
    `).join("");
  }

  renderPagination(rows.length);
}

function renderAacrTableHead() {
  const filters = state.filters.aacr || getDefaultFilters("aacr");

  const makeSearch = (id, placeholder, value) => `
    <div style="margin-top:6px;">
      <input class="search-input" id="${id}" type="search" value="${escapeHtml(value || "")}" placeholder="${placeholder}" style="padding:6px 8px;font-size:12px;min-width:110px;max-width:160px;" onkeydown="applyFiltersOnEnter(event)">
    </div>
  `;

  return `
    <tr>
      <th>管线${makeSearch("headPipelineFilter", "搜管线", filters.pipeline || "")}</th>
      <th>公司${makeSearch("headCompanyFilter", "搜公司", filters.company || "")}</th>
      <th>靶点${makeSearch("headTargetFilter", "搜靶点", filters.targetMeta || "")}</th>
      <th>作用机制${makeSearch("headMoaFilter", "搜机制", filters.moaMeta || "")}</th>
      <th>总分</th>
      <th>简述</th>
      <th>ID</th>
    </tr>
  `;
}

function renderPagination(totalRows) {
  const totalPages = Math.max(1, Math.ceil(totalRows / state.perPage));
  if (state.page > totalPages) state.page = totalPages;
  document.getElementById("pageInfo").textContent = `共 ${totalRows} 条，当前第 ${state.page} / ${totalPages} 页`;

  const buttons = [];
  buttons.push(`<button class="page-btn" onclick="goPage(${state.page - 1})" ${state.page === 1 ? "disabled" : ""}>‹</button>`);

  const pages = [];
  for (let i = 1; i <= totalPages; i++) pages.push(i);
  const displayPages = totalPages <= 7
    ? pages
    : [...new Set([1, 2, state.page - 1, state.page, state.page + 1, totalPages - 1, totalPages].filter(p => p >= 1 && p <= totalPages))].sort((a, b) => a - b);

  let prev = 0;
  displayPages.forEach(page => {
    if (page - prev > 1) buttons.push(`<button class="page-btn" disabled>…</button>`);
    buttons.push(`<button class="page-btn ${page === state.page ? "active" : ""}" onclick="goPage(${page})">${page}</button>`);
    prev = page;
  });

  buttons.push(`<button class="page-btn" onclick="goPage(${state.page + 1})" ${state.page === totalPages ? "disabled" : ""}>›</button>`);
  document.getElementById("pageBtns").innerHTML = buttons.join("");
}

function goPage(page) {
  const totalPages = Math.max(1, Math.ceil(state.filteredRows.length / state.perPage));
  if (page < 1 || page > totalPages) return;
  state.page = page;
  renderTableSection();
}

function linkCell(url, title) {
  const safeTitle = escapeHtml(title || "-");
  if (!url) return `<div class="summary-cell">${safeTitle}</div>`;
  return `<a class="title-link" href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer" title="${safeTitle}">${safeTitle}</a>`;
}

function medicalStatusCell(row) {
  const done = row.updateStatusText === "已更新";
  return `<button class="status-btn ${done ? "done" : "pending"}" onclick="toggleMedicalStatus('${escapeJs(row.__id)}', event)">${done ? "已更新" : "未更新"}</button>`;
}

function toggleMedicalStatus(id, event) {
  event.stopPropagation();
  const rows = MODULES.medical.rows;
  const item = rows.find(row => row.__id === id);
  if (!item) return;
  item.updateStatusText = item.updateStatusText === "已更新" ? "未更新" : "已更新";
  renderTableSection();
  renderStats();
  if (state.selectedId === id) openModal(id);
}

function openModal(id) {
  const row = getActiveRows().find(item => item.__id === id);
  if (!row) return;
  state.selectedId = id;

  const module = getActiveModule();
  document.getElementById("modalTitle").textContent = module.id === "medical" ? (row.drugName || "-") : (row.drug_name || "-");
  document.getElementById("modalSubtitle").textContent = module.id === "medical"
    ? `${row.institution || "-"} · ${row.updateStatusText}`
    : `${row.company || "-"} · ${row.modality || "-"} · ${formatScore(row.total_score)} 分`;

  if (module.id === "medical") {
    renderMedicalModal(row);
  } else {
    renderAacrModal(row);
  }

  document.getElementById("modalOverlay").classList.add("open");
  requestAnimationFrame(() => {
    document.querySelectorAll(".metric-fill").forEach(el => {
      el.style.width = `${el.dataset.width}%`;
    });
  });
}

function renderMedicalModal(row) {
  document.getElementById("modalLeft").innerHTML = `
    <div class="section">
      <div class="section-title">基础信息</div>
      <div class="info-grid">
        ${infoRow("药品名称", row.drugName)}
        ${infoRow("药品别名", row.drugAlias)}
        ${infoRow("靶点", row.target)}
        ${infoRow("药理类型", row.pharmacology)}
        ${infoRow("研发机构", row.institution)}
        ${infoRow("药品类别", row.drugCategory)}
        ${infoRow("新增日期", row.addDate)}
      </div>
    </div>
    <div class="section">
      <div class="section-title">更新提示</div>
      <div class="message-box">${escapeHtml(row.updateHint || "暂无")}</div>
    </div>
    <div class="section">
      <div class="section-title">优先更新建议</div>
      <div class="reason-box">${escapeHtml(row.recommendation || "暂无")}</div>
    </div>
    <div class="section">
      <div class="section-title">资讯链接</div>
      <div class="message-box">${row.newsUrl ? `<a class="title-link" href="${escapeHtml(row.newsUrl)}" target="_blank" rel="noopener noreferrer">${escapeHtml(row.newsTitle || row.newsUrl)}</a>` : "暂无链接"}</div>
    </div>
  `;

  const bucket = getPriorityBucket(row.totalScore);
  const dimensions = Array.isArray(row.dimensions) ? row.dimensions : [];
  const blocks = dimensions.length
    ? dimensions.map(dim => metricBlock(dim.name, dim.score, dim.weight || "", dim.reason || "", dim.isBonus)).join("")
    : `<div class="message-box">当前导入的数据没有维度拆解字段，后续如 Excel 中补充分项得分，可直接在此展示。</div>`;

  document.getElementById("modalRight").innerHTML = `
    <div class="score-block">
      <div class="score-num" style="color:${bucket.key === "high" ? "var(--high)" : bucket.key === "mid" ? "var(--mid)" : "var(--low)"}">${formatScore(row.totalScore)}</div>
      <div>
        <div>${priorityPill(bucket)}</div>
        <div class="score-caption" style="margin-top:10px;">加权总分</div>
      </div>
    </div>
    <div class="section">
      <div class="section-title">维度拆解</div>
      ${blocks}
    </div>
  `;
}

function renderAacrModal(row) {
  document.getElementById("modalLeft").innerHTML = `
    <div class="section">
      <div class="section-title">基础信息</div>
      <div class="info-grid">
        ${infoRow("药物名称", row.drug_name)}
        ${infoRow("公司", row.company)}
        ${infoRow("机构类型", row.source_org_type)}
        ${infoRow("靶点", row.target)}
        ${infoRow("技术类型", row.modality)}
      </div>
    </div>
    <div class="section">
      <div class="section-title">AACR 摘要标题</div>
      <div class="message-box">${row.url ? `<a class="title-link" href="${escapeHtml(row.url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(row.title || row.url)}</a>` : escapeHtml(row.title || "暂无")}</div>
    </div>
    <div class="section">
      <div class="section-title">优先级简述</div>
      <div class="reason-box">${escapeHtml(row.brief_reason || "暂无")}</div>
    </div>
  `;

  const bucket = getPriorityBucket(row.total_score);
  document.getElementById("modalRight").innerHTML = `
    <div class="score-block">
      <div class="score-num" style="color:${bucket.key === "high" ? "var(--high)" : bucket.key === "mid" ? "var(--mid)" : "var(--low)"}">${formatScore(row.total_score)}</div>
      <div>
        <div>${priorityPill(bucket)}</div>
        <div class="score-caption" style="margin-top:10px;">AACR 综合优先级总分</div>
      </div>
    </div>
    <div class="section">
      <div class="section-title">分项得分</div>
      ${metricBlock("机构重要性", row.institution_score, "", "机构类型和商业化潜力对优先级的贡献。")}
      ${metricBlock("赛道重要性", row.track_score, "", "当前靶点 / 技术赛道的关注度与价值。")}
      ${metricBlock("里程碑重要性", row.milestone_score, "", "摘要中的前临床 / 临床推进信号。")}
      ${metricBlock("领域热度", row.field_heat_score, "", "该方向在肿瘤研发中的整体热度。")}
      ${metricBlock("加分项", row.bonus_score, "额外", "额外创新点或特殊亮点。", true)}
    </div>
    <div class="section">
      <div class="section-title">摘要正文</div>
      <div class="abstract-box">${escapeHtml(row.abstract_text || "暂无摘要")}</div>
    </div>
  `;
}

function metricBlock(name, score, weight, reason, isBonus = false) {
  const num = Number(score) || 0;
  const color = num >= 85 ? "#16a34a" : num >= 70 ? "#2563eb" : num >= 50 ? "#d97706" : "#64748b";
  return `
    <div class="metric-item">
      <div class="metric-head">
        <div class="metric-name">${escapeHtml(name)}</div>
        <div class="metric-meta">
          ${weight ? `<span class="metric-tag">${escapeHtml(weight)}</span>` : ""}
          <span class="metric-score" style="color:${color}">${formatScore(num)}</span>
        </div>
      </div>
      <div class="metric-track">
        <div class="metric-fill" data-width="${Math.max(0, Math.min(isBonus ? num * 10 : num, 100))}" style="background:${color}"></div>
      </div>
      <div class="metric-reason">${escapeHtml(reason || "暂无说明")}</div>
    </div>
  `;
}

function infoRow(key, value) {
  return `<div class="info-row"><div class="info-key">${escapeHtml(key)}</div><div class="info-val">${escapeHtml(value || "-")}</div></div>`;
}

function closeModal() {
  document.getElementById("modalOverlay").classList.remove("open");
  document.querySelectorAll(".metric-fill").forEach(el => {
    el.style.width = "0";
  });
}

function handleOverlayClick(event) {
  if (event.target.id === "modalOverlay") closeModal();
}

function exportCurrentRows() {
  const module = getActiveModule();
  const rows = state.filteredRows;
  if (!rows.length) return;

  const exportRows = module.id === "medical"
    ? rows.map(row => ({
        药品名称: row.drugName,
        药品别名: row.drugAlias,
        靶点: row.target,
        药理类型: row.pharmacology,
        研发机构: row.institution,
        总分: row.totalScore,
        优先级: priorityLabelFromScore(row.totalScore),
        优先更新建议: row.recommendation,
        资讯标题: row.newsTitle,
        资讯链接: row.newsUrl,
        资讯来源: row.newsSource,
        新增日期: row.addDate,
        更新状态: row.updateStatusText
      }))
    : rows.map(row => ({
        drug_name: row.drug_name,
        company: row.company,
        source_org_type: row.source_org_type,
        target: row.target,
        modality: row.modality,
        total_score: row.total_score,
        importance_level: priorityLabelFromScore(row.total_score),
        institution_score: row.institution_score,
        track_score: row.track_score,
        milestone_score: row.milestone_score,
        field_heat_score: row.field_heat_score,
        bonus_score: row.bonus_score,
        brief_reason: row.brief_reason,
        title: row.title,
        url: row.url
      }));

  const ws = XLSX.utils.json_to_sheet(exportRows);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, module.datasetName);
  XLSX.writeFile(wb, `${module.datasetName}_${TODAY}.xlsx`);
}

function priorityLabelFromScore(score) {
  const bucket = getPriorityBucket(score).key;
  if (bucket === "high") return "高优先";
  if (bucket === "mid") return "中优先";
  return "低优先";
}

function triggerImport() {
  const targetId = state.activeModule === "medical" ? "medicalFileInput" : "aacrFileInput";
  document.getElementById(targetId).click();
}

function handleModuleImport(event, moduleId) {
  const file = event.target.files?.[0];
  if (!file) return;

  const reader = new FileReader();
  reader.onload = e => {
    const data = new Uint8Array(e.target.result);
    const workbook = XLSX.read(data, { type: "array" });
    const rows = moduleId === "medical" ? parseMedicalWorkbook(workbook) : parseAacrWorkbook(workbook);
    if (!rows.length) {
      alert("未识别到可用数据，请确认 Excel 表头是否符合当前模块的字段要求。");
      event.target.value = "";
      return;
    }

    if (moduleId === "medical") {
      MODULES.medical.rows = normalizeMedicalRows(rows);
      MODULES.medical.sourceName = `本地 Excel / ${file.name}`;
      MODULES.medical.sourceMeta = "当前数据来自本地导入的医药资讯 Excel。AACR 模块未受影响。";
    } else {
      MODULES.aacr.rows = normalizeAacrRows(rows);
      MODULES.aacr.sourceName = `本地 Excel / ${file.name}`;
      MODULES.aacr.sourceMeta = "当前数据来自本地导入的 AACR Excel。医药资讯模块未受影响。";
    }

    if (state.activeModule === moduleId) {
      resetFilters();
    }
    event.target.value = "";
  };
  reader.readAsArrayBuffer(file);
}

function parseMedicalWorkbook(workbook) {
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  const rows = XLSX.utils.sheet_to_json(sheet, { defval: "" });
  return rows.map((row, index) => ({
    id: row.id || row.ID || index + 1,
    drugName: row.drugName || row["药品名称"] || row["drug_name"] || "",
    drugAlias: row.drugAlias || row["药品别名"] || "",
    target: row.target || row["靶点"] || "",
    pharmacology: row.pharmacology || row["药理类型"] || "",
    institution: row.institution || row["研发机构"] || row.company || "",
    drugCategory: row.drugCategory || row["药品类别"] || "",
    updateHint: row.updateHint || row["更新提示"] || "",
    totalScore: row.totalScore || row["总分"] || row.total_score || 0,
    recommendation: row.recommendation || row["优先更新建议"] || row["brief_reason"] || "",
    newsTitle: row.newsTitle || row["资讯标题"] || row.title || "",
    newsUrl: row.newsUrl || row["资讯链接"] || row.url || "",
    newsSource: row.newsSource || row["资讯来源"] || "",
    addDate: row.addDate || row["新增日期"] || row.date || "",
    updateStatus: row.updateStatus || row["更新状态"] || "未更新",
    dimensions: []
  }));
}

function parseAacrWorkbook(workbook) {
  const sheetName = workbook.SheetNames.includes("grok_result") ? "grok_result" : workbook.SheetNames[0];
  const sheet = workbook.Sheets[sheetName];
  return XLSX.utils.sheet_to_json(sheet, { defval: "" }).map(row => ({
    drug_name: row.drug_name || row["药物名称"] || "",
    company: row.company || row["公司"] || "",
    org_type: row.org_type || row["机构类型"] || "",
    target: row.target || row["靶点"] || "",
    modality: row.modality || row["技术类型"] || "",
    target_meta: row.target_meta || row["target_meta"] || row["靶点标准化"] || row.target || row["靶点"] || "",
    MOA_meta: row.MOA_meta || row["MOA_meta"] || row["作用机制"] || row.modality || row["技术类型"] || "",
    importance_level: row.importance_level || row["优先级"] || "",
    total_score: row.total_score || row["总分"] || 0,
    institution_score: row.institution_score || row["机构重要性"] || 0,
    track_score: row.track_score || row["赛道重要性"] || 0,
    milestone_score: row.milestone_score || row["里程碑重要性"] || 0,
    field_heat_score: row.field_heat_score || row["领域热度"] || 0,
    bonus_score: row.bonus_score || row["加分项"] || 0,
    brief_reason: row.brief_reason || row["简述"] || "",
    url: row.url || row["链接"] || "",
    id: row.id || row._id || row["ID"] || "",
    source_drug_name: row.source_drug_name || row["原始药名"] || "",
    title: row.title || row["摘要标题"] || "",
    abstract_text: row.abstract_text || row["摘要正文"] || "",
    source_company: row.source_company || row["原始公司"] || "",
    source_org_type: row.source_org_type || row["原始机构类型"] || row["source_org_type"] || row["机构类型"] || row.org_type || "",
    row_number: row.row_number || row["行号"] || ""
  }));
}

function copyAacrId(id, event) {
  event.stopPropagation();
  const value = String(id || "").trim();
  if (!value) return;
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(value).catch(() => fallbackCopyText(value));
    return;
  }
  fallbackCopyText(value);
}

function fallbackCopyText(value) {
  const input = document.createElement("textarea");
  input.value = value;
  input.setAttribute("readonly", "");
  input.style.position = "fixed";
  input.style.opacity = "0";
  document.body.appendChild(input);
  input.select();
  document.execCommand("copy");
  document.body.removeChild(input);
}

function getDistinctValues(values) {
  return [...new Set(values.filter(Boolean).map(v => String(v).trim()).filter(Boolean))].sort((a, b) => a.localeCompare(b));
}

function exportCurrentRows() {
  const module = getActiveModule();
  const rows = state.filteredRows;
  if (!rows.length) return;

  const exportRows = module.id === "medical"
    ? rows.map(row => ({
        drug_name: row.drugName,
        drug_alias: row.drugAlias,
        target: row.target,
        pharmacology: row.pharmacology,
        institution: row.institution,
        total_score: row.totalScore,
        priority: priorityLabelFromScore(row.totalScore),
        recommendation: row.recommendation,
        news_title: row.newsTitle,
        news_url: row.newsUrl,
        news_source: row.newsSource,
        add_date: row.addDate,
        update_status: row.updateStatusText
      }))
    : rows.map(row => ({
        drug_name: row.drug_name,
        company: row.company,
        source_org_type: row.source_org_type,
        target: row.target,
        modality: row.modality,
        total_score: row.total_score,
        importance_level: priorityLabelFromScore(row.total_score),
        institution_score: row.institution_score,
        track_score: row.track_score,
        milestone_score: row.milestone_score,
        field_heat_score: row.field_heat_score,
        bonus_score: row.bonus_score,
        brief_reason: row.brief_reason,
        title: row.title,
        url: row.url
      }));

  if (typeof window.XLSX !== "undefined") {
    const ws = XLSX.utils.json_to_sheet(exportRows);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, module.datasetName);
    XLSX.writeFile(wb, `${module.datasetName}_${TODAY}.xlsx`);
    return;
  }

  downloadTextFile(`${module.datasetName}_${TODAY}.csv`, toCsv(exportRows), "text/csv;charset=utf-8;");
}

function triggerImport() {
  const targetId = state.activeModule === "medical" ? "medicalFileInput" : "aacrFileInput";
  document.getElementById(targetId).click();
}

function handleModuleImport(event, moduleId) {
  const file = event.target.files?.[0];
  if (!file) return;

  const extension = (file.name.split(".").pop() || "").toLowerCase();

  if (extension === "csv") {
    const reader = new FileReader();
    reader.onload = e => {
      const rows = parseCsvText(String(e.target?.result || ""));
      applyImportedRows(moduleId, rows, file.name, "CSV");
      event.target.value = "";
    };
    reader.readAsText(file, "utf-8");
    return;
  }

  if (typeof window.XLSX === "undefined") {
    alert("当前纯静态版本未内置 Excel 解析库。请改为导入 CSV，或使用带 XLSX 库的版本。");
    event.target.value = "";
    return;
  }

  const reader = new FileReader();
  reader.onload = e => {
    const data = new Uint8Array(e.target.result);
    const workbook = XLSX.read(data, { type: "array" });
    const rows = moduleId === "medical" ? parseMedicalWorkbook(workbook) : parseAacrWorkbook(workbook);
    applyImportedRows(moduleId, rows, file.name, "Excel");
    event.target.value = "";
  };
  reader.readAsArrayBuffer(file);
}

function parseMedicalWorkbook(workbook) {
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  const rows = XLSX.utils.sheet_to_json(sheet, { defval: "" });
  return parseMedicalRows(rows);
}

function parseAacrWorkbook(workbook) {
  const sheetName = workbook.SheetNames.includes("grok_result") ? "grok_result" : workbook.SheetNames[0];
  const sheet = workbook.Sheets[sheetName];
  const rows = XLSX.utils.sheet_to_json(sheet, { defval: "" });
  return parseAacrRows(rows);
}

function applyImportedRows(moduleId, rawRows, fileName, fileTypeLabel) {
  const rows = moduleId === "medical" ? parseMedicalRows(rawRows) : parseAacrRows(rawRows);
  if (!rows.length) {
    alert("未识别到可用数据，请确认文件表头是否符合当前模块要求。");
    return;
  }

  if (moduleId === "medical") {
    MODULES.medical.rows = normalizeMedicalRows(rows);
    MODULES.medical.sourceName = `本地${fileTypeLabel} / ${fileName}`;
    MODULES.medical.sourceMeta = `当前数据来自本地导入的医药资讯${fileTypeLabel}文件。AACR 模块未受影响。`;
  } else {
    MODULES.aacr.rows = normalizeAacrRows(rows);
    MODULES.aacr.sourceName = `本地${fileTypeLabel} / ${fileName}`;
    MODULES.aacr.sourceMeta = `当前数据来自本地导入的 AACR ${fileTypeLabel}文件。医药资讯模块未受影响。`;
  }

  if (state.activeModule === moduleId) resetFilters();
}

function parseMedicalRows(rows) {
  return rows.map((row, index) => ({
    id: row.id || row.ID || index + 1,
    drugName: row.drugName || row["药品名称"] || row.drug_name || "",
    drugAlias: row.drugAlias || row["药品别名"] || "",
    target: row.target || row["靶点"] || "",
    pharmacology: row.pharmacology || row["药理类型"] || "",
    institution: row.institution || row["研发机构"] || row.company || "",
    drugCategory: row.drugCategory || row["药品类别"] || "",
    updateHint: row.updateHint || row["更新提示"] || "",
    totalScore: row.totalScore || row["总分"] || row.total_score || 0,
    recommendation: row.recommendation || row["优先更新建议"] || row.brief_reason || "",
    newsTitle: row.newsTitle || row["资讯标题"] || row.title || "",
    newsUrl: row.newsUrl || row["资讯链接"] || row.url || "",
    newsSource: row.newsSource || row["资讯来源"] || "",
    addDate: row.addDate || row["新增日期"] || row.date || "",
    updateStatus: row.updateStatus || row["更新状态"] || "未更新",
    dimensions: []
  }));
}

function parseAacrRows(rows) {
  return rows.map(row => ({
    drug_name: row.drug_name || row["药物名称"] || "",
    company: row.company || row["公司"] || "",
    org_type: row.org_type || row["机构类型"] || "",
    target: row.target || row["靶点"] || "",
    modality: row.modality || row["技术类型"] || "",
    target_meta: row.target_meta || row["target_meta"] || row["靶点标准化"] || row.target || row["靶点"] || "",
    MOA_meta: row.MOA_meta || row["MOA_meta"] || row["作用机制"] || row.modality || row["技术类型"] || "",
    importance_level: row.importance_level || row["优先级"] || "",
    total_score: row.total_score || row["总分"] || 0,
    institution_score: row.institution_score || row["机构重要性"] || 0,
    track_score: row.track_score || row["赛道重要性"] || 0,
    milestone_score: row.milestone_score || row["里程碑重要性"] || 0,
    field_heat_score: row.field_heat_score || row["领域热度"] || 0,
    bonus_score: row.bonus_score || row["加分项"] || 0,
    brief_reason: row.brief_reason || row["简述"] || "",
    url: row.url || row["链接"] || "",
    id: row.id || row._id || row["ID"] || "",
    source_drug_name: row.source_drug_name || row["原始药名"] || "",
    title: row.title || row["摘要标题"] || "",
    abstract_text: row.abstract_text || row["摘要正文"] || "",
    source_company: row.source_company || row["原始公司"] || "",
    source_org_type: row.source_org_type || row["原始机构类型"] || row["source_org_type"] || row["机构类型"] || row.org_type || "",
    row_number: row.row_number || row["行号"] || ""
  }));
}

function parseCsvText(text) {
  const normalized = String(text || "").replace(/^\uFEFF/, "");
  const rows = [];
  let field = "";
  let row = [];
  let inQuotes = false;

  for (let i = 0; i < normalized.length; i += 1) {
    const char = normalized[i];
    const next = normalized[i + 1];

    if (char === "\"") {
      if (inQuotes && next === "\"") {
        field += "\"";
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === "," && !inQuotes) {
      row.push(field);
      field = "";
      continue;
    }

    if ((char === "\n" || char === "\r") && !inQuotes) {
      if (char === "\r" && next === "\n") i += 1;
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
      continue;
    }

    field += char;
  }

  if (field.length || row.length) {
    row.push(field);
    rows.push(row);
  }

  if (!rows.length) return [];
  const [header, ...body] = rows;
  return body
    .filter(cells => cells.some(cell => String(cell).trim() !== ""))
    .map(cells => Object.fromEntries(header.map((key, index) => [String(key || "").trim(), cells[index] ?? ""])));
}

function toCsv(rows) {
  if (!rows.length) return "";
  const headers = [...new Set(rows.flatMap(row => Object.keys(row)))];
  const escapeCell = value => {
    const text = String(value ?? "");
    return /[",\r\n]/.test(text) ? `"${text.replace(/"/g, "\"\"")}"` : text;
  };

  return [
    headers.join(","),
    ...rows.map(row => headers.map(header => escapeCell(row[header])).join(","))
  ].join("\r\n");
}

function downloadTextFile(fileName, content, mimeType) {
  const blob = new Blob([content], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = fileName;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

function renderAll() {
  buildTabs();
  buildSidebar();
  renderHero();
  renderStats();
  renderTableSection();
}

document.addEventListener("keydown", event => {
  if (event.key === "Escape") closeModal();
});

state.filters.medical = getDefaultFilters("medical");
state.filters.aacr = getDefaultFilters("aacr");
renderAll();
