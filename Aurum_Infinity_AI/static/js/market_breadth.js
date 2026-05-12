(function () {
  const dataNode = document.getElementById("market-breadth-data");
  if (!dataNode) return;

  const payload = JSON.parse(dataNode.textContent || "{}");
  const series = payload.series || [];
  const proxies = payload.proxies || {};

  function number(value) {
    return Number.isFinite(Number(value)) ? Number(value) : null;
  }

  function buildLine(points, xKey, yKey, width, height, yMin, yMax) {
    if (!points.length || yMax === yMin) return "";
    return points.map((point, index) => {
      const x = points.length === 1 ? width / 2 : (index / (points.length - 1)) * width;
      const value = number(point[yKey]);
      const y = value === null ? height : height - ((value - yMin) / (yMax - yMin)) * height;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    }).join(" ");
  }

  function renderChart(targetId, lines, options) {
    const target = document.getElementById(targetId);
    if (!target) return;

    const width = 1000;
    const height = options.height || 320;
    const values = [];
    lines.forEach((line) => {
      line.points.forEach((point) => {
        const value = number(point[line.yKey]);
        if (value !== null) values.push(value);
      });
    });

    if (!values.length) {
      target.innerHTML = `<div class="breadth-empty">暫無資料</div>`;
      return;
    }

    let min = Math.min(...values);
    let max = Math.max(...values);
    const pad = (max - min || 1) * 0.12;
    min -= pad;
    max += pad;

    const grid = [0.25, 0.5, 0.75].map((ratio) => {
      const y = height * ratio;
      return `<line x1="0" y1="${y}" x2="${width}" y2="${y}" stroke="rgba(31,29,24,0.08)" />`;
    }).join("");

    const paths = lines.map((line) => {
      const points = buildLine(line.points, "date", line.yKey, width, height, min, max);
      if (!points) return "";
      return `<polyline points="${points}" fill="none" stroke="${line.color}" stroke-width="${line.width || 3}" stroke-linecap="round" stroke-linejoin="round" />`;
    }).join("");

    const firstDate = series[0] ? series[0].date : "";
    const lastDate = series[series.length - 1] ? series[series.length - 1].date : "";
    target.innerHTML = `
      <svg viewBox="0 0 ${width} ${height + 34}" preserveAspectRatio="none" aria-hidden="true">
        <rect x="0" y="0" width="${width}" height="${height}" rx="18" fill="rgba(255,255,255,0.28)" />
        ${grid}
        ${paths}
        <text x="0" y="${height + 24}" fill="#766b5f" font-size="18">${firstDate}</text>
        <text x="${width}" y="${height + 24}" text-anchor="end" fill="#766b5f" font-size="18">${lastDate}</text>
      </svg>
    `;
  }

  const equalWeight = series.map((item) => ({
    date: item.date,
    value: item.equal_weight_index,
  }));
  const spy = (proxies.SPY && proxies.SPY.points || []).map((item) => ({
    date: item.date,
    value: item.normalized,
  }));
  const dia = (proxies.DIA && proxies.DIA.points || []).map((item) => ({
    date: item.date,
    value: item.normalized,
  }));

  renderChart("breadth-index-chart", [
    { points: equalWeight, yKey: "value", color: "#1d4f8f", width: 4 },
    { points: spy, yKey: "value", color: "#c38b16", width: 3 },
    { points: dia, yKey: "value", color: "#168c66", width: 3 },
  ], { height: 320 });

  renderChart("breadth-participation-chart", [
    { points: series, yKey: "advancers_pct", color: "#1d4f8f", width: 4 },
    { points: series, yKey: "above50_pct", color: "#c38b16", width: 3 },
    { points: series, yKey: "above200_pct", color: "#168c66", width: 3 },
  ], { height: 240 });

  renderChart("breadth-ad-chart", [
    { points: series, yKey: "advance_decline_line", color: "#1d4f8f", width: 4 },
  ], { height: 240 });
})();
