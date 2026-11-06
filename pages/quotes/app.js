(() => {
  "use strict";

  // 面板会在 </body> 前注入 bridge SDK，可能晚于本脚本，因此延迟解析。
  let bridge = null;
  const resolveBridge = () => window.AstrBotPluginView || window.AstrBotPluginPage || null;
  async function waitForBridge(timeout = 5000) {
    const deadline = Date.now() + timeout;
    let found = resolveBridge();
    while (!found && Date.now() < deadline) {
      await new Promise((resolve) => window.setTimeout(resolve, 60));
      found = resolveBridge();
    }
    return found;
  }

  const state = {
    pool: "normal",
    session: "all",
    query: "",
    page: 1,
    pageSize: 12,
    total: 0,
    totalPages: 1,
    loading: false,
    items: [],
    styleDefaults: null,
    styleSaved: null,
    draft: null,
    previewQuoteId: "",
    previewPool: "normal",
  };

  const $ = (id) => document.getElementById(id);

  function toast(message, kind = "info") {
    const node = $("toast");
    node.textContent = message;
    node.className = `toast show ${kind}`;
    window.clearTimeout(toast._timer);
    toast._timer = window.setTimeout(() => {
      node.className = "toast";
    }, 2600);
  }

  function escapeHtml(text) {
    return String(text == null ? "" : text)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function params(extra = {}) {
    return Object.assign(
      {
        pool: state.pool,
        session: state.session,
        query: state.query,
        page: state.page,
        page_size: state.pageSize,
      },
      extra,
    );
  }

  async function callGet(endpoint, query) {
    if (!bridge) throw new Error("插件页面桥接不可用，请从 AstrBot 面板的插件页面打开本页。");
    return bridge.apiGet(endpoint, query);
  }

  async function callPost(endpoint, body) {
    if (!bridge) throw new Error("插件页面桥接不可用，请从 AstrBot 面板的插件页面打开本页。");
    return bridge.apiPost(endpoint, body);
  }

  function debounce(fn, delay) {
    let timer = null;
    return (...args) => {
      window.clearTimeout(timer);
      timer = window.setTimeout(() => fn(...args), delay);
    };
  }

  // ------------------------------------------------------------------ 列表
  function setCounts(pools) {
    const normal = (pools && pools.normal && pools.normal.total) || 0;
    const nsfw = (pools && pools.nsfw && pools.nsfw.total) || 0;
    $("count-normal").textContent = String(normal);
    $("count-nsfw").textContent = String(nsfw);
    $("subtitle").textContent = `普通池 ${normal} 条 · 涩图池 ${nsfw} 条 · 两池互不可见`;
    return { normal, nsfw };
  }

  function fillSessions(sessions) {
    const select = $("session-select");
    const current = state.session;
    select.replaceChildren();
    const all = document.createElement("option");
    all.value = "all";
    all.textContent = "全部会话";
    select.append(all);
    (sessions || []).forEach((item) => {
      const option = document.createElement("option");
      option.value = item.session;
      option.textContent = `${item.label}（${item.count}）`;
      select.append(option);
    });
    const exists = [...select.options].some((option) => option.value === current);
    select.value = exists ? current : "all";
    state.session = select.value;
  }

  function renderEmpty() {
    const empty = document.createElement("div");
    empty.className = "empty";
    const hint =
      state.pool === "nsfw"
        ? "这个池还空着，先在群里用「上传涩图」保存一条吧。"
        : "这个池还没有语录，先用「上传」保存一条吧。";
    empty.innerHTML = `<div class="empty-icon">🗂️</div><p>${escapeHtml(
      state.query ? `没有匹配「${state.query}」的语录` : hint,
    )}</p>`;
    $("list").replaceChildren(empty);
  }

  function card(item) {
    const node = document.createElement("article");
    node.className = "card";
    node.dataset.id = item.id;

    const thumbs = (item.thumbs || [])
      .map((src, index) => `<img src="${src}" alt="图片 ${index + 1}" loading="lazy">`)
      .join("");
    const more =
      item.image_count > (item.thumbs || []).length
        ? `<span class="more">+${item.image_count - (item.thumbs || []).length}</span>`
        : "";

    node.innerHTML = `
      <div class="card-head">
        <span class="index">#${item.index}</span>
        <span class="badge${item.badge === "聊天记录" ? " forward" : ""}">${escapeHtml(item.badge)}</span>
        <span class="session">${escapeHtml(item.session_label)}</span>
      </div>
      ${thumbs ? `<div class="thumbs">${thumbs}${more}</div>` : ""}
      <div class="card-title">${escapeHtml(item.name)} <span class="qq">${escapeHtml(item.qq)}</span></div>
      <p class="text">${escapeHtml(item.text || "（无文字内容）")}</p>
      <div class="card-foot">
        <span class="time">${escapeHtml(item.time_label)}</span>
        <span class="card-actions">
          <button class="mini preview" type="button" title="渲染成图片预览">预览</button>
          <button class="mini copy" type="button" title="复制文字">复制</button>
          <button class="delete" type="button">删除</button>
        </span>
      </div>
    `;

    node.querySelector(".preview").addEventListener("click", () => openPreview(item));
    node.querySelector(".copy").addEventListener("click", () => copyText(item.text || ""));
    node.querySelector(".delete").addEventListener("click", () => remove(item, node));
    return node;
  }

  async function copyText(text) {
    try {
      await navigator.clipboard.writeText(text);
      toast("已复制文字", "ok");
    } catch (error) {
      toast("复制失败，请手动选择文本", "error");
    }
  }

  async function remove(item, node) {
    const label = `${item.name}（#${item.index}）`;
    if (!window.confirm(`确认删除这条语录吗？\n${label}`)) return;
    const button = node.querySelector(".delete");
    button.disabled = true;
    button.textContent = "删除中…";
    try {
      await callPost("page/delete", { pool: state.pool, quote_id: item.id });
      node.classList.add("removing");
      toast("已删除", "ok");
      await refresh(true);
    } catch (error) {
      button.disabled = false;
      button.textContent = "删除";
      toast(`删除失败：${error && error.message ? error.message : error}`, "error");
    }
  }

  function renderPage(data) {
    state.total = data.total || 0;
    state.totalPages = data.total_pages || 1;
    state.page = data.page || 1;
    state.items = data.items || [];
    $("page-info").textContent = `${state.page} / ${state.totalPages} · 共 ${state.total} 条`;
    $("prev").disabled = state.page <= 1;
    $("next").disabled = state.page >= state.totalPages;

    if (!state.items.length) {
      renderEmpty();
      return;
    }
    $("list").replaceChildren(...state.items.map(card));
  }

  async function loadList() {
    const data = await callGet("page/list", params());
    renderPage(data || {});
  }

  async function refresh(keepPage = false) {
    if (state.loading) return;
    state.loading = true;
    if (!keepPage) state.page = 1;
    $("subtitle").textContent = "读取中…";
    try {
      const overview = (await callGet("page/overview")) || {};
      const counts = setCounts(overview.pools);
      fillSessions(overview.pools && overview.pools[state.pool] ? overview.pools[state.pool].sessions : []);
      await loadList();
      if (state.pool === "nsfw" && counts.nsfw === 0) {
        $("subtitle").textContent = `普通池 ${counts.normal} 条 · 涩图池 0 条 · 两池互不可见`;
      }
    } catch (error) {
      $("subtitle").textContent = "读取失败";
      $("list").replaceChildren();
      const node = document.createElement("div");
      node.className = "empty";
      node.innerHTML = `<div class="empty-icon">⚠️</div><p>${escapeHtml(
        error && error.message ? error.message : String(error),
      )}</p>`;
      $("list").append(node);
      toast("读取失败，请查看插件日志", "error");
    } finally {
      state.loading = false;
    }
  }

  function switchPool(pool) {
    if (state.pool === pool) return;
    state.pool = pool;
    state.page = 1;
    state.session = "all";
    document.querySelectorAll(".tab").forEach((tab) => {
      tab.classList.toggle("active", tab.dataset.pool === pool);
    });
    refresh();
    if (isDrawerOpen()) scheduleLivePreview(true);
  }

  // ------------------------------------------------------------------ 渲染预览
  function openModal() {
    $("preview-modal").hidden = false;
    document.body.classList.add("modal-open");
  }

  function closeModal() {
    $("preview-modal").hidden = true;
    document.body.classList.remove("modal-open");
  }

  async function renderQuoteImage(quoteId, pool, style) {
    const body = { pool: pool || state.pool };
    if (quoteId) body.quote_id = quoteId;
    if (style) body.style = style;
    return callPost("page/render", body);
  }

  async function openPreview(item) {
    state.previewQuoteId = item ? item.id : "";
    state.previewPool = state.pool;
    $("preview-title").textContent = item ? `渲染预览 · ${item.name}（#${item.index}）` : "渲染预览";
    $("preview-text").textContent = item ? item.text || "" : "";
    $("preview-img").hidden = true;
    $("preview-loading").hidden = false;
    $("preview-loading").textContent = "渲染中…";
    openModal();
    try {
      const data = await renderQuoteImage(state.previewQuoteId, state.previewPool, isDrawerOpen() ? draftStyle() : null);
      $("preview-img").src = data.image;
      $("preview-img").hidden = false;
    } catch (error) {
      $("preview-loading").textContent = `渲染失败：${error && error.message ? error.message : error}`;
    } finally {
      $("preview-loading").hidden = $("preview-img").hidden ? false : true;
    }
  }

  // ------------------------------------------------------------------ 样式抽屉
  const styleDraftDefaults = () => Object.assign({}, state.styleDefaults || {});

  function isDrawerOpen() {
    return !$("style-drawer").hidden;
  }

  function setColorPair(colorInput, textInput, value) {
    const color = normalizeHex(value);
    if (color) colorInput.value = color;
    textInput.value = value || color || "";
  }

  function normalizeHex(value) {
    const text = String(value || "").trim();
    if (/^#[0-9a-fA-F]{6}$/.test(text)) return text.toLowerCase();
    if (/^#[0-9a-fA-F]{3}$/.test(text)) {
      return `#${text[1]}${text[1]}${text[2]}${text[2]}${text[3]}${text[3]}`.toLowerCase();
    }
    return "";
  }

  function applyStyleToControls(style) {
    state.draft = Object.assign(styleDraftDefaults(), style || {});
    const draft = state.draft;
    setColorPair($("style-bg-color"), $("style-bg-color-text"), draft.bg_color || "#000000");
    setColorPair($("style-text-color"), $("style-text-color-text"), draft.text_color || "#ffffff");
    $("style-font-scale").value = draft.font_scale || 100;
    $("style-font-scale-value").textContent = `${draft.font_scale || 100}%`;
    $("style-overlay").value = draft.bg_image_overlay == null ? 45 : draft.bg_image_overlay;
    $("style-overlay-value").textContent = `${$("style-overlay").value}%`;
    $("style-width").value = draft.width || 1280;
    $("style-height").value = draft.height || 427;
    $("style-font-family").value = draft.font_family || "";
    const bg = draft.bg_image || "";
    $("style-bg-url").value = bg.startsWith("data:") ? "（已选择本地图片，尚未保存）" : bg;
    $("style-bg-url").disabled = bg.startsWith("data:");
  }

  function draftStyle() {
    const draft = Object.assign({}, state.draft || styleDraftDefaults());
    draft.bg_color = $("style-bg-color-text").value.trim() || draft.bg_color;
    draft.text_color = $("style-text-color-text").value.trim() || draft.text_color;
    draft.font_scale = Number($("style-font-scale").value) || 100;
    draft.bg_image_overlay = Number($("style-overlay").value);
    draft.width = Number($("style-width").value) || draft.width;
    draft.height = Number($("style-height").value) || draft.height;
    draft.font_family = $("style-font-family").value.trim() || draft.font_family;
    return draft;
  }

  async function livePreview() {
    if (!isDrawerOpen()) return;
    const hint = $("style-preview-hint");
    const img = $("style-preview-img");
    hint.hidden = false;
    hint.textContent = "渲染中…";
    const startedAt = Date.now();
    const ticker = window.setInterval(() => {
      hint.textContent = `渲染中… ${Math.round((Date.now() - startedAt) / 1000)}s`;
    }, 900);
    try {
      const quoteId = state.previewQuoteId && state.previewPool === state.pool ? state.previewQuoteId : "";
      const data = await Promise.race([
        renderQuoteImage(quoteId, state.pool, draftStyle()),
        new Promise((_resolve, reject) =>
          window.setTimeout(() => reject(new Error("渲染请求超时（60s）")), 60000),
        ),
      ]);
      if (!data || !data.image) throw new Error("渲染接口没有返回图片数据");
      img.src = data.image;
      img.hidden = false;
      hint.hidden = true;
    } catch (error) {
      img.hidden = true;
      hint.hidden = false;
      hint.textContent = `预览失败：${error && error.message ? error.message : error}`;
    } finally {
      window.clearInterval(ticker);
    }
  }

  const scheduleLivePreview = debounce(livePreview, 380);

  async function openDrawer() {
    $("style-drawer").hidden = false;
    document.body.classList.add("drawer-open");
    if (!state.styleSaved) {
      try {
        const data = (await callGet("page/style")) || {};
        state.styleDefaults = data.defaults || {};
        state.styleSaved = data.style || {};
      } catch (error) {
        toast(`读取样式失败：${error && error.message ? error.message : error}`, "error");
        state.styleDefaults = state.styleDefaults || {};
      }
    }
    applyStyleToControls(state.styleSaved || styleDraftDefaults());
    scheduleLivePreview();
  }

  function closeDrawer() {
    $("style-drawer").hidden = true;
    document.body.classList.remove("drawer-open");
  }

  function readImageFile(file) {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onerror = () => reject(new Error("读取图片失败"));
      reader.onload = () => {
        const img = new Image();
        img.onerror = () => reject(new Error("不是有效的图片文件"));
        img.onload = () => {
          const maxWidth = 1600;
          const ratio = img.width > maxWidth ? maxWidth / img.width : 1;
          const canvas = document.createElement("canvas");
          canvas.width = Math.round(img.width * ratio);
          canvas.height = Math.round(img.height * ratio);
          const context = canvas.getContext("2d");
          context.drawImage(img, 0, 0, canvas.width, canvas.height);
          resolve(canvas.toDataURL("image/jpeg", 0.86));
        };
        img.src = reader.result;
      };
      reader.readAsDataURL(file);
    });
  }

  async function saveStyle() {
    const button = $("style-save");
    button.disabled = true;
    button.textContent = "保存中…";
    try {
      const data = await callPost("page/style_save", { style: draftStyle() });
      state.styleSaved = data.style || draftStyle();
      applyStyleToControls(state.styleSaved);
      toast(
        data.saved
          ? `样式已保存${data.cache_cleared ? `，已清理 ${data.cache_cleared} 个渲染缓存` : ""}`
          : "样式已应用（配置文件未写入，重启后可能回退）",
        data.saved ? "ok" : "warn",
      );
      scheduleLivePreview();
    } catch (error) {
      toast(`保存失败：${error && error.message ? error.message : error}`, "error");
    } finally {
      button.disabled = false;
      button.textContent = "保存样式";
    }
  }

  function resetStyle() {
    applyStyleToControls(state.styleDefaults || {});
    scheduleLivePreview();
    toast("已恢复默认值（记得点保存）", "info");
  }

  async function clearRenderCache() {
    try {
      const data = await callPost("page/cache_clear", {});
      toast(`已清空渲染缓存 ${data.removed || 0} 个文件`, "ok");
    } catch (error) {
      toast(`清空失败：${error && error.message ? error.message : error}`, "error");
    }
  }

  // ------------------------------------------------------------------ 事件
  function bind() {
    document.querySelectorAll(".tab").forEach((tab) => {
      tab.addEventListener("click", () => switchPool(tab.dataset.pool));
    });
    $("session-select").addEventListener("change", (event) => {
      state.session = event.target.value;
      state.page = 1;
      loadList().catch((error) => toast(String(error), "error"));
    });
    $("page-size").addEventListener("change", (event) => {
      state.pageSize = Number(event.target.value) || 12;
      state.page = 1;
      loadList().catch((error) => toast(String(error), "error"));
    });
    let timer = null;
    $("search-input").addEventListener("input", (event) => {
      window.clearTimeout(timer);
      timer = window.setTimeout(() => {
        state.query = event.target.value.trim();
        state.page = 1;
        loadList().catch((error) => toast(String(error), "error"));
      }, 260);
    });
    $("prev").addEventListener("click", () => {
      if (state.page <= 1) return;
      state.page -= 1;
      loadList().catch((error) => toast(String(error), "error"));
    });
    $("next").addEventListener("click", () => {
      if (state.page >= state.totalPages) return;
      state.page += 1;
      loadList().catch((error) => toast(String(error), "error"));
    });
    $("refresh").addEventListener("click", () => refresh(true));

    // 预览弹窗
    $("preview-close").addEventListener("click", closeModal);
    $("preview-modal").addEventListener("click", (event) => {
      if (event.target === $("preview-modal")) closeModal();
    });
    $("preview-reload").addEventListener("click", () => {
      const item = state.items.find((entry) => entry.id === state.previewQuoteId);
      openPreview(item || null);
    });
    $("preview-download").addEventListener("click", () => {
      const src = $("preview-img").src;
      if (!src) return;
      const link = document.createElement("a");
      link.href = src;
      link.download = `quote-${state.previewQuoteId || "preview"}.jpg`;
      link.click();
    });

    // 样式抽屉
    $("open-style").addEventListener("click", () => (isDrawerOpen() ? closeDrawer() : openDrawer()));
    $("style-close").addEventListener("click", closeDrawer);
    $("style-save").addEventListener("click", saveStyle);
    $("style-reset").addEventListener("click", resetStyle);
    $("style-clear-cache").addEventListener("click", clearRenderCache);

    $("style-bg-color").addEventListener("input", (event) => {
      $("style-bg-color-text").value = event.target.value;
      scheduleLivePreview();
    });
    $("style-bg-color-text").addEventListener("input", (event) => {
      const hex = normalizeHex(event.target.value);
      if (hex) $("style-bg-color").value = hex;
      scheduleLivePreview();
    });
    $("style-text-color").addEventListener("input", (event) => {
      $("style-text-color-text").value = event.target.value;
      scheduleLivePreview();
    });
    $("style-text-color-text").addEventListener("input", (event) => {
      const hex = normalizeHex(event.target.value);
      if (hex) $("style-text-color").value = hex;
      scheduleLivePreview();
    });
    $("style-font-scale").addEventListener("input", (event) => {
      $("style-font-scale-value").textContent = `${event.target.value}%`;
      scheduleLivePreview();
    });
    $("style-overlay").addEventListener("input", (event) => {
      $("style-overlay-value").textContent = `${event.target.value}%`;
      scheduleLivePreview();
    });
    ["style-width", "style-height", "style-font-family"].forEach((id) => {
      $(id).addEventListener("input", scheduleLivePreview);
    });
    $("style-bg-url").addEventListener("input", (event) => {
      if (!state.draft) return;
      state.draft.bg_image = event.target.value.trim();
      scheduleLivePreview();
    });
    $("style-bg-clear").addEventListener("click", () => {
      if (!state.draft) return;
      state.draft.bg_image = "";
      $("style-bg-url").disabled = false;
      $("style-bg-url").value = "";
      $("style-bg-file").value = "";
      scheduleLivePreview();
    });
    $("style-bg-file").addEventListener("change", async (event) => {
      const file = event.target.files && event.target.files[0];
      if (!file) return;
      try {
        const dataUrl = await readImageFile(file);
        if (!state.draft) state.draft = styleDraftDefaults();
        state.draft.bg_image = dataUrl;
        $("style-bg-url").value = "（已选择本地图片，尚未保存）";
        $("style-bg-url").disabled = true;
        scheduleLivePreview();
      } catch (error) {
        toast(error.message || "读取图片失败", "error");
      }
    });

    document.addEventListener("keydown", (event) => {
      if (event.key !== "Escape") return;
      if (!$("preview-modal").hidden) closeModal();
      else if (isDrawerOpen()) closeDrawer();
    });
  }

  async function boot() {
    bind();
    bridge = await waitForBridge();
    if (!bridge) {
      $("subtitle").textContent = "桥接不可用";
      toast("请从 AstrBot 面板进入插件页面", "error");
      return;
    }
    try {
      if (typeof bridge.ready === "function") {
        await Promise.race([
          bridge.ready(),
          new Promise((resolve) => window.setTimeout(resolve, 1500)),
        ]);
      }
    } catch (error) {
      console.warn("bridge.ready failed", error);
    }
    refresh();
    if (/(?:^|[?&#])open=style/.test(window.location.href)) {
      openDrawer();
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
