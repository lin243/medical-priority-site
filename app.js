const TODAY = new Date().toISOString().slice(0, 10);
const DEFAULT_MEDICAL_CSV = "./npnews_daily_table_latest_scored.csv";

const MODULES = {
  medical: {
    id: "medical",
    tabLabel: "医药资讯优先级系统",
    title: "医药资讯优先级系统",
    kicker: "Medical News",
    description: "默认读取同目录的 scored CSV，后续导入文件也按同一格式解析。",
    sourceName: "本地 CSV / 医药资讯",
    sourceMeta: "当前默认读取 scored CSV。",
    datasetName: "medical_news_priority",
    rows: []
  },
  aacr: {
    id: "aacr",
    tabLabel: "AACR 管线优先级系统",
    title: "AACR 管线优先级系统",
    kicker: "AACR Pipeline",
    description: "AACR 模块保持独立，继续使用当前内嵌数据。",
    sourceName: "本地嵌入数据 / AACR",
    sourceMeta: "当前默认读取页面内嵌 AACR 数据。",
    datasetName: "aacr_pipeline_priority",
    rows: []
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

function esc(v) {
  return String(v ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}

function escJs(v) {
  return String(v ?? "").replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function scoreText(v) {
  if (v === "" || v === null || v === undefined) return "-";
  const n = Number(v);
  return Number.isFinite(n) ? n.toFixed(n % 1 === 0 ? 0 : 1) : String(v);
}

function bucket(score) {
  const n = num(score);
  if (n >= 75) return { key: "high", cls: "pill-high" };
  if (n >= 50) return { key: "mid", cls: "pill-mid" };
  return { key: "low", cls: "pill-low" };
}

function scorePill(score) {
  const b = bucket(score);
  return `<span class="score-pill ${b.cls}">${esc(scoreText(score))}</span>`;
}

function parseMetaDisplay(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  const parts = raw.split(" | ").map(s => s.trim()).filter(Boolean);
  const texts = [];
  for (const part of parts) {
    try {
      const item = JSON.parse(part);
      texts.push(String(item?.text || "").trim() || part);
    } catch {
      texts.push(part);
    }
  }
  return [...new Set(texts.filter(Boolean))].join(" | ");
}

function parseDimensions(rawJson) {
  if (!rawJson) return [];
  try {
    const parsed = JSON.parse(rawJson);
    const details = Array.isArray(parsed["维度打分详情"]) ? parsed["维度打分详情"] : [];
    return details.map(item => ({
      name: item["维度"] || "",
      score: item["分数"] || "",
      reason: item["理由"] || ""
    }));
  } catch {
    return [];
  }
}

function parseUpdateTags(label) {
  return String(label || "").split(";").map(item => item.trim()).filter(Boolean);
}

function normalizeMedicalRows(rows) {
  return rows.map((row, index) => ({
    ...row,
    __id: `medical-${row.id_news || index + 1}`,
    idNews: String(row.id_news || "").trim(),
    drugDisplay: parseMetaDisplay(row.drug_name_meta) || String(row.drug_name_original || "").trim(),
    drugOriginal: String(row.drug_name_original || "").trim(),
    drugMetaDisplay: parseMetaDisplay(row.drug_name_meta),
    drugSynonymMetaDisplay: parseMetaDisplay(row.drug_synonym_meta),
    targetMetaDisplay: parseMetaDisplay(row.target_meta),
    moaTargetDisplay: parseMetaDisplay(row.MOA_target),
    companyDisplay: String(row.baseline_company || "").trim(),
    totalScore: num(row.llm_total_score),
    updateContent: String(row.label || "").trim(),
    updateTags: parseUpdateTags(row.label),
    newsTitle: String(row.title_news || "").trim(),
    newsUrl: String(row.url_news || "").trim(),
    sourceNews: String(row.source_news || "").trim(),
    dateOnly: String(row.time || "").trim().slice(0, 10),
    updateStatusText: String(row.update_status || row.updateStatus || "").trim() || "未更新",
    baselineName: String(row.baseline_name || "").trim(),
    baselineTarget: String(row.baseline_target || "").trim(),
    baselineMoa: String(row.baseline_MOA || "").trim(),
    baselineCompany: String(row.baseline_company || "").trim(),
    dimensions: parseDimensions(row.llm_raw_json),
    llmRawJson: String(row.llm_raw_json || "").trim()
  })).sort((a, b) => num(b.totalScore) - num(a.totalScore));
}

function normalizeAacrRows(rows) {
  return rows.map((row, index) => ({
    ...row,
    __id: `aacr-${row.id || index + 1}`,
    total_score: num(row.total_score),
    institution_score: num(row.institution_score),
    track_score: num(row.track_score),
    milestone_score: num(row.milestone_score),
    field_heat_score: num(row.field_heat_score),
    bonus_score: num(row.bonus_score),
    source_org_type: row.source_org_type || row.org_type || "",
    target_meta: row.target_meta || row.target || "",
    MOA_meta: row.MOA_meta || row.modality || ""
  })).sort((a, b) => num(b.total_score) - num(a.total_score));
}

function parseCsvText(text) {
  const rows = [];
  const input = String(text || "").replace(/^\uFEFF/, "");
  let field = "";
  let row = [];
  let inQuotes = false;
  for (let i = 0; i < input.length; i += 1) {
    const ch = input[i];
    const next = input[i + 1];
    if (ch === "\"") {
      if (inQuotes && next === "\"") {
        field += "\"";
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (ch === "," && !inQuotes) {
      row.push(field);
      field = "";
      continue;
    }
    if ((ch === "\n" || ch === "\r") && !inQuotes) {
      if (ch === "\r" && next === "\n") i += 1;
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
      continue;
    }
    field += ch;
  }
  if (field.length || row.length) {
    row.push(field);
    rows.push(row);
  }
  if (!rows.length) return [];
  const [header, ...body] = rows;
  return body.filter(cells => cells.some(cell => String(cell).trim() !== "")).map(cells => {
    const item = {};
    header.forEach((key, index) => {
      item[String(key || "").trim()] = cells[index] ?? "";
    });
    return item;
  });
}

function toCsv(rows) {
  if (!rows.length) return "";
  const headers = [...new Set(rows.flatMap(row => Object.keys(row)))];
  const encode = value => {
    const text = String(value ?? "");
    return /[",\r\n]/.test(text) ? `"${text.replace(/"/g, "\"\"")}"` : text;
  };
  return [headers.join(","), ...rows.map(row => headers.map(key => encode(row[key])).join(","))].join("\r\n");
}

function downloadTextFile(name, text, mime) {
  const blob = new Blob([text], { type: mime });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = name;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

function getDefaultFilters(moduleId) {
  return moduleId === "medical"
    ? {
        priority: { high: true, mid: true, low: true },
        scoreMin: 0,
        scoreMax: 100,
        dateFrom: "",
        dateTo: "",
        status: { pending: true, done: true },
        keyword: "",
        drugKeyword: "",
        updateType: ""
      }
    : {
        priority: { high: true, mid: true, low: true },
        scoreMin: 0,
        scoreMax: 100,
        keyword: "",
        drugKeyword: "",
        companyType: "",
        pipeline: "",
        company: "",
        targetMeta: "",
        moaMeta: ""
      };
}

function activeModule() {
  return MODULES[state.activeModule];
}

function readFiltersFromDom() {
  const base = {
    priority: {
      high: document.getElementById("priority-high")?.checked ?? true,
      mid: document.getElementById("priority-mid")?.checked ?? true,
      low: document.getElementById("priority-low")?.checked ?? true
    },
    scoreMin: num(document.getElementById("scoreMin")?.value || 0),
    scoreMax: num(document.getElementById("scoreMax")?.value || 100),
    keyword: (document.getElementById("keywordInput")?.value || "").trim().toLowerCase()
  };
  if (state.activeModule !== "medical") {
    return {
      ...base,
      companyType: (document.getElementById("companyTypeInput")?.value || "").trim().toLowerCase(),
      pipeline: (document.getElementById("headPipelineFilter")?.value || "").trim().toLowerCase(),
      company: (document.getElementById("headCompanyFilter")?.value || "").trim().toLowerCase(),
      targetMeta: (document.getElementById("headTargetFilter")?.value || "").trim().toLowerCase(),
      moaMeta: (document.getElementById("headMoaFilter")?.value || "").trim().toLowerCase()
    };
  }
  return {
    ...base,
    drugKeyword: (document.getElementById("headDrugFilter")?.value || "").trim().toLowerCase(),
    dateFrom: document.getElementById("dateFrom")?.value || "",
    dateTo: document.getElementById("dateTo")?.value || "",
    status: {
      pending: document.getElementById("statusPending")?.checked ?? true,
      done: document.getElementById("statusDone")?.checked ?? true
    },
    updateType: document.getElementById("headUpdateTypeFilter")?.value || "",
  };
}

function filteredRows() {
  const module = activeModule();
  const filters = state.filters[module.id] || getDefaultFilters(module.id);
  return module.rows.filter(row => {
    const score = module.id === "medical" ? row.totalScore : row.total_score;
    if (!filters.priority[bucket(score).key]) return false;
    if (score < filters.scoreMin || score > filters.scoreMax) return false;
    if (module.id === "medical") {
      if (filters.dateFrom && row.dateOnly < filters.dateFrom) return false;
      if (filters.dateTo && row.dateOnly > filters.dateTo) return false;
      if (!filters.status.pending && row.updateStatusText === "未更新") return false;
      if (!filters.status.done && row.updateStatusText === "已更新") return false;
      if (filters.updateType) {
        const matcherByType = {
          drug: tag => tag === "【药品】",
          drug_synonym: tag => tag === "【新增药品异名】",
          mismatch: tag => tag === "【药品和药品异名不一致】",
          target: tag => tag.startsWith("【更新或新增靶点:"),
          moa: tag => tag === "【药理类型】"
        };
        const matcher = matcherByType[filters.updateType];
        if (matcher && !row.updateTags.some(tag => matcher(tag))) return false;
      }
    }
    if (module.id === "medical" && filters.drugKeyword) {
      const drugHaystack = [row.drugDisplay, row.drugOriginal].join(" ").toLowerCase();
      if (!drugHaystack.includes(filters.drugKeyword)) return false;
    }
    if (filters.keyword) {
      const haystack = module.id === "medical"
        ? [row.drugDisplay, row.companyDisplay, row.updateContent, row.newsTitle, row.idNews].join(" ").toLowerCase()
        : [row.drug_name, row.company, row.target, row.title, row.id].join(" ").toLowerCase();
      if (!haystack.includes(filters.keyword)) return false;
    }
    if (module.id !== "medical") {
      if (filters.companyType && !String(row.source_org_type || "").toLowerCase().includes(filters.companyType)) return false;
      if (filters.pipeline && !String(row.drug_name || "").toLowerCase().includes(filters.pipeline)) return false;
      if (filters.company && !String(row.company || "").toLowerCase().includes(filters.company)) return false;
      if (filters.targetMeta && !String(row.target_meta || "").toLowerCase().includes(filters.targetMeta)) return false;
      if (filters.moaMeta && !String(row.MOA_meta || "").toLowerCase().includes(filters.moaMeta)) return false;
    }
    return true;
  });
}

function buildTabs() {
  document.getElementById("moduleTabs").innerHTML = Object.values(MODULES).map(module => `
    <button class="module-tab ${module.id === state.activeModule ? "active" : ""}" onclick="switchModule('${module.id}')">${module.tabLabel}</button>
  `).join("");
}

function buildSidebar() {
  const module = activeModule();
  const f = state.filters[module.id] || getDefaultFilters(module.id);
  document.getElementById("sidebarSubtitle").textContent = module.id === "medical" ? "医药资讯模块" : "AACR 模块";
  document.getElementById("sidebarContent").innerHTML = `
    <div class="filter-group">
      <div class="filter-group-title">优先级</div>
      <div class="checkbox-list">
        ${cb("high", "高优先", f.priority.high)}
        ${cb("mid", "中优先", f.priority.mid)}
        ${cb("low", "低优先", f.priority.low)}
      </div>
    </div>
    <div class="filter-group">
      <div class="filter-group-title">总分范围</div>
      <div class="score-inputs">
        <input class="score-input" id="scoreMin" type="number" min="0" max="100" value="${f.scoreMin}" oninput="applyFilters()">
        <input class="score-input" id="scoreMax" type="number" min="0" max="100" value="${f.scoreMax}" oninput="applyFilters()">
      </div>
    </div>
    ${module.id !== "medical" ? `
      <div class="filter-group">
        <div class="filter-group-title">公司类型</div>
        <input
          class="search-input"
          id="companyTypeInput"
          type="search"
          value="${esc(f.companyType || "")}"
          placeholder="搜索公司类型"
          onkeydown="handleSubmitSearch(event)">
      </div>
    ` : ""}
    ${module.id === "medical" ? `
      <div class="filter-group">
        <div class="filter-group-title">日期</div>
        <div class="date-inputs">
          <input class="date-input" id="dateFrom" type="date" value="${f.dateFrom}" onchange="applyFilters()">
          <input class="date-input" id="dateTo" type="date" value="${f.dateTo}" onchange="applyFilters()">
        </div>
      </div>
      <div class="filter-group">
        <div class="filter-group-title">更新状态</div>
        <div class="checkbox-list">
          <label class="cb-item"><input type="checkbox" id="statusPending" ${f.status.pending ? "checked" : ""} onchange="applyFilters()"><span class="dot dot-pending"></span>未更新</label>
          <label class="cb-item"><input type="checkbox" id="statusDone" ${f.status.done ? "checked" : ""} onchange="applyFilters()"><span class="dot dot-updated"></span>已更新</label>
        </div>
      </div>
    ` : ""}
    <div class="filter-group">
      <div class="filter-group-title">关键词</div>
      <input class="search-input" id="keywordInput" type="text" value="${esc(f.keyword || "")}" placeholder="搜索药品、标题、公司、ID" oninput="applyFilters()">
    </div>
  `;
}

function cb(key, label, checked) {
  const dot = key === "high" ? "dot-high" : key === "mid" ? "dot-mid" : "dot-low";
  return `<label class="cb-item"><input type="checkbox" id="priority-${key}" ${checked ? "checked" : ""} onchange="applyFilters()"><span class="dot ${dot}"></span>${label}</label>`;
}

function renderHero() {
  const module = activeModule();
  document.getElementById("heroSection").style.display = "none";
  document.getElementById("panelKicker").textContent = module.kicker;
  document.getElementById("pageTitle").textContent = module.title;
  document.getElementById("pageDesc").textContent = module.description;
  document.getElementById("sourceName").textContent = module.sourceName;
  document.getElementById("sourceMeta").textContent = module.sourceMeta;
  document.getElementById("tableTitle").textContent = `${module.title}总览`;
  document.getElementById("tableSubtitle").textContent = module.id === "medical" ? "主表字段直接来自 scored CSV。" : "AACR 模块保持独立。";
}

function renderStats() {
  const module = activeModule();
  const rows = filteredRows();
  const stats = module.id === "medical"
    ? [
        ["高优先", rows.filter(r => r.totalScore >= 75).length, "high"],
        ["中优先", rows.filter(r => r.totalScore >= 50 && r.totalScore < 75).length, "mid"],
        ["今日新增", rows.filter(r => r.dateOnly === TODAY).length, "accent"],
        ["待更新", rows.filter(r => r.updateStatusText === "未更新").length, "green"]
      ]
    : [
        ["高优先", rows.filter(r => r.total_score >= 75).length, "high"],
        ["中优先", rows.filter(r => r.total_score >= 50 && r.total_score < 75).length, "mid"],
        ["公司来源", rows.filter(r => String(r.source_org_type || "").includes("公司")).length, "accent"],
        ["大学来源", rows.filter(r => String(r.source_org_type || "").includes("大学")).length, "green"]
      ];
  document.getElementById("statsBar").innerHTML = stats.map(([label, value, tone]) => `
    <div class="stat-card"><div class="stat-label">${label}</div><div class="stat-value ${tone}">${value}</div></div>
  `).join("");
}

function renderTableSection() {
  const module = activeModule();
  const rows = filteredRows();
  state.filteredRows = rows;
  const columns = module.id === "medical"
    ? [
        ["药品", row => `${esc(row.drugDisplay || "-")}<div class="subtext">${esc(row.companyDisplay || "")}</div>`],
        ["总分", row => `<button class="drug-btn" onclick="openModal('${escJs(row.__id)}')">${scorePill(row.totalScore)}</button>`],
        ["更新内容", row => `<div class="summary-cell">${esc(row.updateContent || "-")}</div>`],
        ["资讯标题", row => row.newsUrl ? `<a class="title-link" href="${esc(row.newsUrl)}" target="_blank" rel="noopener noreferrer">${esc(row.newsTitle || "-")}</a>` : esc(row.newsTitle || "-")],
        ["日期", row => `<span class="mono">${esc(row.dateOnly || "-")}</span>`],
        ["ID", row => `<div class="id-cell"><button class="copy-id-btn" onclick="copyText('${escJs(row.idNews || "")}', event)">${esc(row.idNews || "-")}</button></div>`],
        ["更新状态", row => statusCell(row)]
      ]
    : [
        ["管线", row => row.url ? `<a class="title-link" href="${esc(row.url)}" target="_blank" rel="noopener noreferrer">${esc(row.drug_name || "-")}</a>` : esc(row.drug_name || "-")],
        ["公司", row => `<div class="compact-text">${esc(row.company || "-")}</div><div class="subtext compact-text">${esc(row.source_org_type || "-")}</div>`],
        ["靶点", row => `<div class="compact-text">${esc(row.target_meta || "-")}</div><div class="subtext compact-text">${esc(row.target || "-")}</div>`],
        ["作用机制", row => `<div class="compact-text">${esc(row.MOA_meta || "-")}</div><div class="subtext compact-text">${esc(row.modality || "-")}</div>`],
        ["总分", row => `<button class="drug-btn" onclick="openModal('${escJs(row.__id)}')">${scorePill(row.total_score)}</button>`],
        ["简述", row => `<div class="summary-cell">${esc(row.brief_reason || "-")}</div>`],
        ["ID", row => `<div class="id-cell"><button class="copy-id-btn" onclick="copyText('${escJs(row.id || "")}', event)">${esc(row.id || "-")}</button></div>`]
      ];

  document.getElementById("tableHead").innerHTML = module.id === "medical"
    ? renderMedicalTableHead(columns)
    : renderAacrTableHead();
  const table = document.querySelector(".table-scroll table");
  if (table) table.className = module.id === "medical" ? "table-medical" : "table-aacr";
  const start = (state.page - 1) * state.perPage;
  const pageRows = rows.slice(start, start + state.perPage);
  document.getElementById("tableBody").innerHTML = pageRows.length
    ? pageRows.map(row => `<tr>${columns.map(([, render]) => `<td>${render(row)}</td>`).join("")}</tr>`).join("")
    : `<tr><td colspan="${columns.length}"><div class="empty-state">当前筛选条件下没有匹配数据。</div></td></tr>`;
  renderPagination(rows.length);
}

function renderMedicalTableHead(columns) {
  const filters = state.filters.medical || getDefaultFilters("medical");
  const searchBox = (id, placeholder, value) => `
    <div style="margin-top:6px;">
      <input
        class="search-input"
        id="${id}"
        type="search"
        value="${esc(value || "")}"
        placeholder="${placeholder}"
        style="padding:6px 8px;font-size:12px;min-width:110px;max-width:160px;"
        onkeydown="handleSubmitSearch(event)">
    </div>
  `;
  return `<tr>${columns.map(([label], index) => {
    if (index === 0) return `<th>${label}${searchBox("headDrugFilter", "搜索药品", filters.drugKeyword)}</th>`;
    if (index !== 2) return `<th>${label}</th>`;
    return `<th>${label}
      <div style="margin-top:6px;">
        <select
          id="headUpdateTypeFilter"
          class="select-input"
          style="padding:6px 8px;font-size:12px;min-width:150px;"
          onchange="applyFilters()">
          <option value="" ${filters.updateType === "" ? "selected" : ""}>全部</option>
          <option value="drug" ${filters.updateType === "drug" ? "selected" : ""}>新增药品</option>
          <option value="drug_synonym" ${filters.updateType === "drug_synonym" ? "selected" : ""}>新增药品异名</option>
          <option value="mismatch" ${filters.updateType === "mismatch" ? "selected" : ""}>药品和药品异名不一致</option>
          <option value="target" ${filters.updateType === "target" ? "selected" : ""}>更新靶点</option>
          <option value="moa" ${filters.updateType === "moa" ? "selected" : ""}>更新药理类型</option>
        </select>
      </div>
    </th>`;
  }).join("")}</tr>`;
}

function renderAacrTableHead() {
  const filters = state.filters.aacr || getDefaultFilters("aacr");
  const searchBox = (id, placeholder, value) => `
    <div style="margin-top:6px;">
      <input
        class="search-input"
        id="${id}"
        type="search"
        value="${esc(value || "")}"
        placeholder="${placeholder}"
        style="padding:6px 8px;font-size:12px;min-width:110px;max-width:160px;"
        onkeydown="handleSubmitSearch(event)">
    </div>
  `;

  return `
    <tr>
      <th>管线${searchBox("headPipelineFilter", "搜索管线", filters.pipeline)}</th>
      <th>公司${searchBox("headCompanyFilter", "搜索公司", filters.company)}</th>
      <th>靶点${searchBox("headTargetFilter", "搜索靶点", filters.targetMeta)}</th>
      <th>作用机制${searchBox("headMoaFilter", "搜索机制", filters.moaMeta)}</th>
      <th>总分</th>
      <th>简述</th>
      <th>ID</th>
    </tr>
  `;
}

function statusCell(row) {
  const done = row.updateStatusText === "已更新";
  return `<button class="ghost-btn" onclick="toggleMedicalStatus('${escJs(row.__id)}', event)" style="padding:6px 10px;border-color:${done ? '#16a34a' : '#94a3b8'};color:${done ? '#166534' : '#475569'};background:${done ? '#dcfce7' : '#f1f5f9'};">${esc(row.updateStatusText)}</button>`;
}

function renderPagination(total) {
  const totalPages = Math.max(1, Math.ceil(total / state.perPage));
  if (state.page > totalPages) state.page = totalPages;
  document.getElementById("pageInfo").textContent = `第 ${state.page} / ${totalPages} 页，共 ${total} 条`;
  document.getElementById("pageBtns").innerHTML = `
    <button class="ghost-btn" ${state.page === 1 ? "disabled" : ""} onclick="goToPage(${state.page - 1})">上一页</button>
    <button class="ghost-btn" ${state.page === totalPages ? "disabled" : ""} onclick="goToPage(${state.page + 1})">下一页</button>
  `;
}

function goToPage(page) {
  const totalPages = Math.max(1, Math.ceil(state.filteredRows.length / state.perPage));
  state.page = Math.max(1, Math.min(totalPages, page));
  renderTableSection();
}

function applyFilters() {
  state.filters[state.activeModule] = readFiltersFromDom();
  state.page = 1;
  renderStats();
  renderTableSection();
}

function resetFilters() {
  state.filters[state.activeModule] = getDefaultFilters(state.activeModule);
  state.page = 1;
  renderAll();
}

function switchModule(moduleId) {
  state.activeModule = moduleId;
  state.page = 1;
  closeModal();
  renderAll();
}

function openModal(rowId) {
  const row = activeModule().rows.find(item => item.__id === rowId);
  if (!row) return;
  document.getElementById("modalOverlay").classList.add("open");
  document.getElementById("modalTitle").textContent = state.activeModule === "medical" ? (row.drugDisplay || "药品详情") : (row.drug_name || "管线详情");
  document.getElementById("modalSubtitle").textContent = state.activeModule === "medical" ? (row.newsTitle || row.idNews || "") : (row.title || row.id || "");
  document.getElementById("modalLeft").innerHTML = state.activeModule === "medical"
    ? `<div class="section">
         <div class="section-title">来源信息</div>
         ${infoRow("药品原名", row.drugOriginal)}
         ${infoRow("药品异名", row.drugMetaDisplay)}
         ${infoRow("药品异名meta", row.drugSynonymMetaDisplay)}
         ${infoRow("靶点", row.targetMetaDisplay)}
         ${infoRow("药理类型", row.moaTargetDisplay)}
         ${infoRow("来源", row.sourceNews)}
       </div>
       <div class="section">
         <div class="section-title">药品基线信息</div>
         ${infoRow("药品名称", row.baselineName)}
         ${infoRow("靶点", row.baselineTarget)}
         ${infoRow("药理类型", row.baselineMoa)}
         ${infoRow("机构", row.baselineCompany)}
       </div>`
    : `<div class="section">
         ${infoRow("药品", row.drug_name)}
         ${infoRow("公司", row.company)}
         ${infoRow("靶点", row.target)}
         ${infoRow("机制", row.modality)}
         ${infoRow("总分", scoreText(row.total_score))}
         ${infoRow("ID", row.id)}
       </div>
       <div class="section">
         <div class="section-title">????</div>
         <div class="abstract-box modal-abstract-box">${esc(row.abstract_text || "-")}</div>
       </div>`;
  document.getElementById("modalRight").innerHTML = state.activeModule === "medical"
    ? `<div class="section"><div class="section-title">维度详情</div>${row.dimensions.length ? row.dimensions.map(item => metric(item)).join("") : '<div class="empty-state">当前记录没有维度明细。</div>'}</div>`
    : `<div class="section"><div class="section-title">评分详情</div>${metric({ name: "机构重要性", score: row.institution_score, reason: "机构类型和商业化潜力" })}${metric({ name: "赛道重要性", score: row.track_score, reason: "靶点和技术赛道热度" })}${metric({ name: "里程碑重要性", score: row.milestone_score, reason: "研发进度和事件强度" })}${metric({ name: "领域热度", score: row.field_heat_score, reason: "方向整体热度" })}${metric({ name: "加分项", score: row.bonus_score, reason: row.brief_reason })}</div>`;
}

function infoRow(label, value) {
  return `<div class="info-row"><div class="info-key">${esc(label)}</div><div class="info-val">${esc(value || "-")}</div></div>`;
}

function metric(item) {
  return `<div class="metric-item"><div class="metric-head"><div class="metric-name">${esc(item.name)}</div><div class="metric-score">${esc(scoreText(item.score))}</div></div><div class="metric-track"><div class="metric-fill" style="width:${Math.max(0, Math.min(num(item.score), 100))}%"></div></div><div class="metric-reason">${esc(item.reason || "-")}</div></div>`;
}

function handleSubmitSearch(event) {
  if (event.key !== "Enter") return;
  event.preventDefault();
  applyFilters();
}

function closeModal() {
  document.getElementById("modalOverlay").classList.remove("open");
}

function handleOverlayClick(event) {
  if (event.target.id === "modalOverlay") closeModal();
}

function copyText(value, event) {
  event?.stopPropagation();
  const text = String(value || "").trim();
  if (!text) return;
  if (navigator.clipboard?.writeText) {
    navigator.clipboard.writeText(text).catch(() => fallbackCopy(text));
  } else {
    fallbackCopy(text);
  }
}

function toggleMedicalStatus(rowId, event) {
  event?.stopPropagation();
  if (state.activeModule !== "medical") return;
  const row = MODULES.medical.rows.find(item => item.__id === rowId);
  if (!row) return;
  row.updateStatusText = row.updateStatusText === "已更新" ? "未更新" : "已更新";
  renderStats();
  renderTableSection();
}

function fallbackCopy(text) {
  const input = document.createElement("textarea");
  input.value = text;
  input.setAttribute("readonly", "");
  input.style.position = "fixed";
  input.style.opacity = "0";
  document.body.appendChild(input);
  input.select();
  document.execCommand("copy");
  document.body.removeChild(input);
}

function exportCurrentRows() {
  if (!state.filteredRows.length) return;
  const rows = state.activeModule === "medical"
    ? state.filteredRows.map(row => ({
        id_news: row.idNews,
        drug_name: row.drugDisplay,
        baseline_company: row.companyDisplay,
        llm_total_score: row.totalScore,
        label: row.updateContent,
        title_news: row.newsTitle,
        url_news: row.newsUrl,
        time: row.dateOnly,
        update_status: row.updateStatusText
      }))
    : state.filteredRows;
  downloadTextFile(`${activeModule().datasetName}_${TODAY}.csv`, toCsv(rows), "text/csv;charset=utf-8;");
}

function triggerImport() {
  document.getElementById(state.activeModule === "medical" ? "medicalFileInput" : "aacrFileInput").click();
}

function handleModuleImport(event, moduleId) {
  const file = event.target.files?.[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = e => {
    const rows = parseCsvText(String(e.target?.result || ""));
    if (moduleId === "medical") {
      MODULES.medical.rows = normalizeMedicalRows(rows);
      MODULES.medical.sourceName = `本地 CSV / ${file.name}`;
      MODULES.medical.sourceMeta = "当前数据来自你导入的医药资讯 CSV。";
    } else {
      MODULES.aacr.rows = normalizeAacrRows(rows);
      MODULES.aacr.sourceName = `本地 CSV / ${file.name}`;
      MODULES.aacr.sourceMeta = "当前数据来自你导入的 AACR CSV。";
    }
    resetFilters();
  };
  reader.readAsText(file, "utf-8");
  event.target.value = "";
}

async function loadDefaultMedicalCsv() {
  try {
    const response = await fetch(DEFAULT_MEDICAL_CSV, { cache: "no-store" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const text = await response.text();
    MODULES.medical.rows = normalizeMedicalRows(parseCsvText(text));
    MODULES.medical.sourceName = "本地 CSV / npnews_daily_table_latest_scored.csv";
    MODULES.medical.sourceMeta = "当前页面默认读取同目录 scored CSV。";
  } catch (error) {
    console.error(error);
    MODULES.medical.rows = [];
    MODULES.medical.sourceName = "默认 CSV 加载失败";
    MODULES.medical.sourceMeta = "请启动本地静态服务器，或手动导入同格式 CSV。";
  }
}

function renderAll() {
  buildTabs();
  buildSidebar();
  renderHero();
  renderStats();
  renderTableSection();
}

async function init() {
  MODULES.aacr.rows = normalizeAacrRows(Array.isArray(window.AACR_DATA) ? window.AACR_DATA : []);
  state.filters.medical = getDefaultFilters("medical");
  state.filters.aacr = getDefaultFilters("aacr");
  await loadDefaultMedicalCsv();
  renderAll();
}

document.addEventListener("keydown", event => {
  if (event.key === "Escape") closeModal();
});

init();
