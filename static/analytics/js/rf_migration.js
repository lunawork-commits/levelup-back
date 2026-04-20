(function () {
  const svg = document.getElementById('flow-svg');
  if (!svg || !FLOWS.length) return;

  const BAR_W   = 18;
  const GAP     = 6;
  const PADDING = 24;
  const LABEL_W = 160;

  const fromTotals = {}, toTotals = {};
  const fromMeta   = {}, toMeta   = {};

  FLOWS.forEach(f => {
    fromTotals[f.from_code] = (fromTotals[f.from_code] || 0) + f.count;
    toTotals[f.to_code]     = (toTotals[f.to_code]     || 0) + f.count;
    fromMeta[f.from_code]   = { name: f.from_name, emoji: f.from_emoji, color: f.from_color };
    toMeta[f.to_code]       = { name: f.to_name,   emoji: f.to_emoji,   color: f.to_color   };
  });

  const fromList = Object.entries(fromTotals).sort((a, b) => b[1] - a[1]);
  const toList   = Object.entries(toTotals).sort((a, b) => b[1] - a[1]);

  const totalFlow  = fromList.reduce((s, [, v]) => s + v, 0);
  const maxSegments = Math.max(fromList.length, toList.length);

  const W     = Math.min(Math.max(svg.parentElement.clientWidth || 700, 500), 900);
  const LEFT  = LABEL_W;
  const RIGHT = W - LABEL_W - BAR_W;
  const USABLE_H = Math.max(maxSegments * 52, 240);
  const H        = USABLE_H + 2 * PADDING;

  svg.setAttribute('width',  W);
  svg.setAttribute('height', H);
  svg.setAttribute('viewBox', `0 0 ${W} ${H}`);

  const scale = (USABLE_H - GAP * (fromList.length - 1)) / totalFlow;

  function layoutBars(list) {
    let y = PADDING;
    return list.map(([code, count]) => {
      const h    = Math.max(14, count * scale);
      const yMid = y + h / 2;
      const pos  = { y, h, yMid, count };
      y += h + GAP;
      return [code, pos];
    });
  }

  const fromPos = Object.fromEntries(layoutBars(fromList));
  const toPos   = Object.fromEntries(layoutBars(toList));

  const fromOff = Object.fromEntries(fromList.map(([c]) => [c, 0]));
  const toOff   = Object.fromEntries(toList.map(([c]) => [c, 0]));

  let markup = '';

  FLOWS.forEach(f => {
    const fp = fromPos[f.from_code];
    const tp = toPos[f.to_code];
    if (!fp || !tp) return;

    const fh = Math.max(1, f.count * scale);
    const x1 = LEFT + BAR_W;
    const x2 = RIGHT;
    const mx  = (x1 + x2) / 2;

    const y1t = fp.y + fromOff[f.from_code];
    const y1b = y1t + fh;
    const y2t = tp.y + toOff[f.to_code];
    const y2b = y2t + fh;

    fromOff[f.from_code] += fh;
    toOff[f.to_code]     += fh;

    const col = f.from_color || '#94a3b8';
    const d = [
      `M ${x1} ${y1t}`,
      `C ${mx} ${y1t}, ${mx} ${y2t}, ${x2} ${y2t}`,
      `L ${x2} ${y2b}`,
      `C ${mx} ${y2b}, ${mx} ${y1b}, ${x1} ${y1b}`,
      'Z',
    ].join(' ');

    markup += `<path d="${d}" fill="${col}38" stroke="${col}88" stroke-width="0.5">
      <title>${f.from_name} → ${f.to_name}: ${f.count} гостей</title>
    </path>`;
  });

  fromList.forEach(([code, count]) => {
    const p    = fromPos[code];
    const meta = fromMeta[code];
    const col  = meta.color || '#94a3b8';

    markup += `<rect x="${LEFT}" y="${p.y}" width="${BAR_W}" height="${p.h}"
      fill="${col}" rx="3" ry="3"/>`;

    const yt = p.yMid;
    markup += `
      <text x="${LEFT - 8}" y="${yt - 5}" text-anchor="end"
        font-size="12" fill="#1e293b" font-weight="600"
        style="paint-order:stroke" stroke="#fff" stroke-width="3">${meta.emoji} ${meta.name}</text>
      <text x="${LEFT - 8}" y="${yt + 10}" text-anchor="end"
        font-size="10" fill="#94a3b8">${count} гостей</text>
    `;
  });

  toList.forEach(([code, count]) => {
    const p    = toPos[code];
    const meta = toMeta[code];
    const col  = meta.color || '#94a3b8';

    markup += `<rect x="${RIGHT}" y="${p.y}" width="${BAR_W}" height="${p.h}"
      fill="${col}" rx="3" ry="3"/>`;

    const yt = p.yMid;
    markup += `
      <text x="${RIGHT + BAR_W + 8}" y="${yt - 5}" text-anchor="start"
        font-size="12" fill="#1e293b" font-weight="600"
        style="paint-order:stroke" stroke="#fff" stroke-width="3">${meta.emoji} ${meta.name}</text>
      <text x="${RIGHT + BAR_W + 8}" y="${yt + 10}" text-anchor="start"
        font-size="10" fill="#94a3b8">${count} гостей</text>
    `;
  });

  markup += `
    <text x="${LEFT + BAR_W / 2}" y="${PADDING - 8}" text-anchor="middle"
      font-size="11" fill="#94a3b8" font-weight="600" letter-spacing="1">ИЗ</text>
    <text x="${RIGHT + BAR_W / 2}" y="${PADDING - 8}" text-anchor="middle"
      font-size="11" fill="#94a3b8" font-weight="600" letter-spacing="1">В</text>
  `;

  svg.innerHTML = markup;
})();

function recalcRF() {
  const btn    = document.getElementById('btn-recalc');
  const status = document.getElementById('recalc-status');

  btn.disabled = true;
  btn.textContent = '⏳ Считаем...';
  status.style.display = 'inline';
  status.textContent   = 'Идёт пересчёт...';

  const body = new URLSearchParams();
  body.set('mode', ACTIVE_MODE);
  if (BRANCH_IDS) body.set('branch_ids', BRANCH_IDS);

  fetch('/api/v1/analytics/rf/recalculate/', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'X-CSRFToken': getCookie('csrftoken') },
    body: body.toString(),
  })
    .then(r => r.json())
    .then(data => {
      if (data.detail || data.non_field_errors) {
        status.textContent = '⚠ ' + (data.detail || JSON.stringify(data));
      } else {
        status.textContent = `✓ Готово: обновлено ${data.updated}, создано ${data.created}, миграций ${data.migrations} (${data.duration_ms} мс)`;
        setTimeout(() => location.reload(), 1500);
      }
    })
    .catch(() => { status.textContent = '⚠ Ошибка соединения'; })
    .finally(() => {
      btn.disabled = false;
      btn.textContent = '🔄 Пересчитать';
    });
}

function getCookie(name) {
  const m = document.cookie.match('(?:^|;)\\s*' + name + '=([^;]*)');
  return m ? decodeURIComponent(m[1]) : '';
}
