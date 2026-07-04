/* Bubble Detector — renders data/data.json into the tape + panel cards. */
(function () {
  "use strict";

  var PANEL_TAGS = { us: "US-EQ", india: "IN-EQ", tech: "US-TECH", fno: "DERIV" };
  var GLYPH = { red: "▲", amber: "◆", green: "●", na: "○" };

  // ---- theme ----
  var root = document.documentElement;
  var saved = localStorage.getItem("theme");
  if (saved) root.setAttribute("data-theme", saved);
  document.getElementById("theme-toggle").addEventListener("click", function () {
    var dark = root.getAttribute("data-theme") === "dark" ||
      (!root.getAttribute("data-theme") &&
        !window.matchMedia("(prefers-color-scheme: light)").matches);
    var next = dark ? "light" : "dark";
    root.setAttribute("data-theme", next);
    localStorage.setItem("theme", next);
  });

  document.getElementById("year").textContent = new Date().getFullYear();

  // ---- helpers ----
  function el(tag, cls, text) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text != null) e.textContent = text;
    return e;
  }

  function relTime(iso) {
    if (!iso) return "";
    var mins = Math.round((Date.now() - new Date(iso).getTime()) / 60000);
    if (mins < 2) return "just now";
    if (mins < 90) return mins + " min ago";
    var hrs = Math.round(mins / 60);
    if (hrs < 36) return hrs + " hours ago";
    return Math.round(hrs / 24) + " days ago";
  }

  function fmtValue(v) {
    if (v == null) return "—";
    if (Math.abs(v) >= 100) return String(Math.round(v));
    return String(v);
  }

  // ---- sparkline ----
  var tooltip = document.getElementById("tooltip");

  function sparkline(spark, status, label) {
    var wrap = el("div", "spark-wrap");
    if (!spark || spark.length < 2) return null;
    var W = 300, H = 56, PAD = 4;
    var vals = spark.map(function (p) { return p[1]; });
    var min = Math.min.apply(null, vals), max = Math.max.apply(null, vals);
    var span = (max - min) || 1;
    function x(i) { return PAD + (W - 2 * PAD) * i / (spark.length - 1); }
    function y(v) { return H - PAD - (H - 2 * PAD) * (v - min) / span; }

    var svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    svg.setAttribute("viewBox", "0 0 " + W + " " + H);
    svg.setAttribute("preserveAspectRatio", "none");
    svg.setAttribute("role", "img");
    svg.setAttribute("aria-label", label + " — 2-year trend, latest " + spark[spark.length - 1][1]);

    var d = spark.map(function (p, i) {
      return (i ? "L" : "M") + x(i).toFixed(1) + " " + y(p[1]).toFixed(1);
    }).join(" ");
    var path = document.createElementNS(svg.namespaceURI, "path");
    path.setAttribute("d", d);
    path.setAttribute("class", "spark-line");
    path.setAttribute("vector-effect", "non-scaling-stroke");
    svg.appendChild(path);

    var dot = document.createElementNS(svg.namespaceURI, "circle");
    dot.setAttribute("cx", x(spark.length - 1));
    dot.setAttribute("cy", y(spark[spark.length - 1][1]));
    dot.setAttribute("r", 3.5);
    dot.setAttribute("class", "spark-dot-" + status);
    svg.appendChild(dot);

    var cross = document.createElementNS(svg.namespaceURI, "line");
    cross.setAttribute("class", "spark-cross");
    cross.setAttribute("y1", PAD);
    cross.setAttribute("y2", H - PAD);
    cross.style.display = "none";
    svg.appendChild(cross);

    svg.addEventListener("mousemove", function (ev) {
      var rect = svg.getBoundingClientRect();
      var i = Math.round((ev.clientX - rect.left) / rect.width * (spark.length - 1));
      i = Math.max(0, Math.min(spark.length - 1, i));
      cross.style.display = "";
      cross.setAttribute("x1", x(i));
      cross.setAttribute("x2", x(i));
      tooltip.hidden = false;
      tooltip.textContent = spark[i][0] + " · " + spark[i][1];
      tooltip.style.left = (rect.left + x(i) / W * rect.width) + "px";
      tooltip.style.top = (rect.top) + "px";
    });
    svg.addEventListener("mouseleave", function () {
      cross.style.display = "none";
      tooltip.hidden = true;
    });

    wrap.appendChild(svg);
    return wrap;
  }

  // ---- card ----
  function card(ind) {
    var c = el("article", "card");
    c.id = "card-" + ind.id;

    var name = el("div", "card-name", ind.name);
    if (ind.stale) name.appendChild(el("span", "stale-badge", "STALE"));
    c.appendChild(name);

    var row = el("div", "card-value-row");
    row.appendChild(el("span", "card-value", fmtValue(ind.value)));
    if (ind.unit) row.appendChild(el("span", "card-unit", ind.unit));
    row.appendChild(el("span", "chip chip-" + ind.status,
      GLYPH[ind.status] + " " + ind.statusLabel));
    c.appendChild(row);

    if (ind.context) c.appendChild(el("div", "card-context", ind.context));

    var sp = sparkline(ind.spark, ind.status, ind.name);
    if (sp) c.appendChild(sp);

    c.appendChild(el("div", "card-explainer", ind.explainer));
    c.appendChild(el("div", "card-source", "source: " + ind.source));
    return c;
  }

  // ---- tape ----
  function buildTape(panels) {
    var tape = document.getElementById("tape");
    var n = 0;
    panels.forEach(function (p) {
      var group = el("div", "tape-group");
      group.appendChild(el("div", "tape-group-label", PANEL_TAGS[p.id] || p.title));
      var cells = el("div", "tape-cells");
      p.indicators.forEach(function (ind) {
        var cell = el("button", "tape-cell cell-" + ind.status, GLYPH[ind.status]);
        cell.style.animationDelay = (n++ * 30) + "ms";
        cell.title = ind.name + " — " + ind.statusLabel;
        cell.setAttribute("aria-label", cell.title);
        cell.addEventListener("click", function () {
          var target = document.getElementById("card-" + ind.id);
          target.scrollIntoView({ block: "center" });
          target.classList.add("flash");
          setTimeout(function () { target.classList.remove("flash"); }, 1600);
        });
        cells.appendChild(cell);
      });
      group.appendChild(cells);
      tape.appendChild(group);
    });
  }

  // ---- panels ----
  function buildPanels(panels) {
    var mainEl = document.getElementById("panels");
    panels.forEach(function (p) {
      var s = el("section", "panel-section");
      var head = el("div", "panel-head");
      head.appendChild(el("h2", "panel-title", p.title));
      head.appendChild(el("span", "panel-tag mono", PANEL_TAGS[p.id] || ""));
      var verdict = el("div", "panel-verdict");
      var counts = el("span", "panel-counts");
      counts.innerHTML =
        '<span class="lg-red">' + p.summary.red + "▲</span> · " +
        '<span class="lg-amber">' + p.summary.amber + "◆</span> · " +
        '<span class="lg-green">' + p.summary.green + "●</span>";
      verdict.appendChild(counts);
      verdict.appendChild(el("span", "verdict-text", p.summary.verdict));
      head.appendChild(verdict);
      s.appendChild(head);
      s.appendChild(el("p", "panel-subtitle", p.subtitle));

      var grid = el("div", "cards");
      p.indicators.forEach(function (ind) { grid.appendChild(card(ind)); });
      s.appendChild(grid);
      mainEl.appendChild(s);
    });
  }

  // ---- boot ----
  fetch("data/data.json?" + Date.now())
    .then(function (r) {
      if (!r.ok) throw new Error("HTTP " + r.status);
      return r.json();
    })
    .then(function (data) {
      document.getElementById("updated").textContent =
        "updated " + relTime(data.generated_at);
      buildTape(data.panels);
      buildPanels(data.panels);
    })
    .catch(function (e) {
      var mainEl = document.getElementById("panels");
      var err = el("p", "error-block",
        "Could not load data/data.json (" + e.message + "). " +
        "Run scripts/fetch.py to generate it, or wait for the next scheduled update.");
      mainEl.appendChild(err);
    });
})();
