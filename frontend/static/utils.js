/**
 * DomQ — утилиты фронтенда
 * Форматирование, H3-цвета, геокодирование, общие DOM-хелперы.
 */

// ─────────────────────────────────────────────
// Форматирование
// ─────────────────────────────────────────────

/** Форматировать рубли: 10 200 000 → «10 200 000 ₽» */
export function fmtRub(value) {
  if (value == null) return '—';
  return Number(value).toLocaleString('ru-RU') + ' ₽';
}

/** Форматировать цену/м²: 191000 → «191 000 ₽/м²» */
export function fmtPpm2(value) {
  if (value == null) return '—';
  return Number(value).toLocaleString('ru-RU') + ' ₽/м²';
}

/** Форматировать площадь: 56.3 → «56.3 м²» */
export function fmtArea(value) {
  if (value == null) return '—';
  const v = Number(value);
  return (Number.isInteger(v) ? v : v.toFixed(1)) + ' м²';
}

/** Сокращение тысяч: 196500 → «196.5 т₽» */
export function fmtKilo(value) {
  if (value == null) return '—';
  return (value / 1000).toFixed(0) + ' т₽';
}

/** ISO-дата → «13 апр. 2026» */
export function fmtDate(isoStr) {
  if (!isoStr) return '—';
  return new Date(isoStr).toLocaleDateString('ru-RU', { day: 'numeric', month: 'short', year: 'numeric' });
}

/** Склонение: pluralize(3, ['объявление','объявления','объявлений']) */
export function pluralize(n, forms) {
  const mod10 = n % 10, mod100 = n % 100;
  if (mod100 >= 11 && mod100 <= 19) return `${n} ${forms[2]}`;
  if (mod10 === 1) return `${n} ${forms[0]}`;
  if (mod10 >= 2 && mod10 <= 4) return `${n} ${forms[1]}`;
  return `${n} ${forms[2]}`;
}

// ─────────────────────────────────────────────
// H3-тепловая карта: цвет по цене
// ─────────────────────────────────────────────

/** Минимальная и максимальная цены для нормализации градиента */
const PRICE_MIN = 100_000;
const PRICE_MAX = 400_000;

/**
 * Возвращает hex-цвет (#rrggbb) по нормализованному значению t ∈ [0, 1].
 * 7 опорных точек со сдвигом в зелёную сторону:
 * зелёный → светло-зелёный → жёлто-зелёный → жёлтый → оранжевый → красный → тёмно-красный
 * t=0..0.5 — зелёная зона, t=0.5 — жёлтый, t=0.5..1 — тёплая зона.
 * @param {number} t
 */
export function tToColor(t) {
  const s = Math.max(0, Math.min(1, t));
  const stops = [
    [0x4A, 0xDE, 0x80],  // #4ADE80 зелёный
    [0x86, 0xEF, 0xAC],  // #86EFAC светло-зелёный
    [0xD9, 0xF9, 0x9D],  // #D9F99D жёлто-зелёный
    [0xFA, 0xCC, 0x15],  // #FACC15 жёлтый
    [0xF9, 0x73, 0x16],  // #F97316 оранжевый
    [0xEF, 0x44, 0x44],  // #EF4444 красный
    [0x7F, 0x1D, 0x1D],  // #7F1D1D тёмно-красный
  ];
  const seg = stops.length - 1;
  const pos = s * seg;
  const i = Math.min(Math.floor(pos), seg - 1);
  const u = pos - i;
  const [r1, g1, b1] = stops[i];
  const [r2, g2, b2] = stops[i + 1];
  const r = Math.round(r1 + (r2 - r1) * u);
  const g = Math.round(g1 + (g2 - g1) * u);
  const b = Math.round(b1 + (b2 - b1) * u);
  return `#${r.toString(16).padStart(2,'0')}${g.toString(16).padStart(2,'0')}${b.toString(16).padStart(2,'0')}`;
}

/**
 * Возвращает hex-цвет (#rrggbb) по нормализованному t ∈ [0,1]: светло-голубой → тёмно-синий.
 * @param {number} t
 */
export function tToColorCount(t) {
  const s = Math.max(0, Math.min(1, t));
  const stops = [
    [0xE0, 0xF2, 0xFE],  // #E0F2FE очень светлый голубой
    [0x7D, 0xD3, 0xFC],  // #7DD3FC голубой
    [0x38, 0xBD, 0xF8],  // #38BDF8 небесный
    [0x02, 0x84, 0xC7],  // #0284C7 синий
    [0x1E, 0x40, 0xAF],  // #1E40AF тёмно-синий
    [0x1E, 0x3A, 0x8A],  // #1E3A8A глубокий синий
  ];
  const seg = stops.length - 1;
  const pos = s * seg;
  const i = Math.min(Math.floor(pos), seg - 1);
  const u = pos - i;
  const [r1, g1, b1] = stops[i];
  const [r2, g2, b2] = stops[i + 1];
  const r = Math.round(r1 + (r2 - r1) * u);
  const g = Math.round(g1 + (g2 - g1) * u);
  const b = Math.round(b1 + (b2 - b1) * u);
  return `#${r.toString(16).padStart(2,'0')}${g.toString(16).padStart(2,'0')}${b.toString(16).padStart(2,'0')}`;
}

/**
 * Возвращает hex-цвет по медианной цене/м² с линейным масштабированием.
 * @param {number} pricePerM2
 * @param {number} [min]
 * @param {number} [max]
 */
export function priceToColor(pricePerM2, min = PRICE_MIN, max = PRICE_MAX) {
  const range = max - min || 1;
  return tToColor((pricePerM2 - min) / range);
}

// ─────────────────────────────────────────────
// Геокодирование через Nominatim
// ─────────────────────────────────────────────

/**
 * Geocode адреса через Nominatim (OpenStreetMap, без ключа).
 * @param {string} address
 * @returns {Promise<{ lat: number, lon: number, display_name: string } | null>}
 */
export async function geocodeAddress(address) {
  const url = new URL('https://nominatim.openstreetmap.org/search');
  url.searchParams.set('q', address + ', Москва');
  url.searchParams.set('format', 'jsonv2');
  url.searchParams.set('limit', '1');
  url.searchParams.set('accept-language', 'ru');

  const res = await fetch(url.toString(), {
    headers: { 'User-Agent': 'DomQ/1.0 (thesis project)' },
  });
  if (!res.ok) return null;
  const data = await res.json();
  if (!data.length) return null;
  return { lat: parseFloat(data[0].lat), lon: parseFloat(data[0].lon), display_name: data[0].display_name };
}

// ─────────────────────────────────────────────
// URL-параметры (для кросс-страничной навигации)
// ─────────────────────────────────────────────

/** Получить query-параметр из текущего URL */
export function getParam(name) {
  return new URLSearchParams(window.location.search).get(name);
}

/** Перейти на страницу с query-параметрами */
export function navigateTo(page, params = {}) {
  const url = new URL(page, window.location.href);
  Object.entries(params).forEach(([k, v]) => {
    if (v !== null && v !== undefined) url.searchParams.set(k, v);
  });
  window.location.href = url.toString();
}

// ─────────────────────────────────────────────
// DOM-хелперы
// ─────────────────────────────────────────────

/** Показать/скрыть элемент */
export function setVisible(el, visible) {
  if (typeof el === 'string') el = document.getElementById(el);
  if (!el) return;
  el.style.display = visible ? '' : 'none';
}

/** Установить текст элемента */
export function setText(el, text) {
  if (typeof el === 'string') el = document.getElementById(el);
  if (el) el.textContent = text ?? '—';
}

/**
 * Простой спиннер — заменяет innerHTML кнопки на анимацию,
 * возвращает функцию восстановления.
 * @param {HTMLElement} btn
 */
export function startSpinner(btn) {
  const original = btn.innerHTML;
  btn.disabled = true;
  btn.innerHTML = '<span class="domq-spinner"></span>';
  return () => { btn.innerHTML = original; btn.disabled = false; };
}

// ─────────────────────────────────────────────
// Toast-уведомления
// ─────────────────────────────────────────────

/** Показать toast-сообщение в правом нижнем углу */
export function showToast(message, type = 'info', durationMs = 4000) {
  let container = document.getElementById('domq-toast-container');
  if (!container) {
    container = document.createElement('div');
    container.id = 'domq-toast-container';
    Object.assign(container.style, {
      position: 'fixed', bottom: '24px', right: '24px',
      display: 'flex', flexDirection: 'column', gap: '8px', zIndex: '99999',
    });
    document.body.appendChild(container);
  }

  const colors = { info: '#2563EB', error: '#EF4444', success: '#16A34A', warn: '#D97706' };
  const icons  = { info: '&#x2139;', error: '&#x2715;', success: '&#x2713;', warn: '&#x26A0;' };

  const toast = document.createElement('div');
  Object.assign(toast.style, {
    background: '#fff', border: `1.5px solid ${colors[type]}`,
    borderRadius: '10px', padding: '12px 16px', maxWidth: '320px',
    boxShadow: '0 4px 20px rgba(0,0,0,.12)', display: 'flex', gap: '10px',
    alignItems: 'flex-start', fontSize: '0.875rem', color: '#0F172A',
    opacity: '0', transform: 'translateX(16px)', transition: 'all .2s ease',
  });
  toast.innerHTML = `<span style="color:${colors[type]};font-size:1rem">${icons[type]}</span>${message}`;
  container.appendChild(toast);

  requestAnimationFrame(() => {
    toast.style.opacity = '1';
    toast.style.transform = 'translateX(0)';
  });

  setTimeout(() => {
    toast.style.opacity = '0';
    toast.style.transform = 'translateX(16px)';
    setTimeout(() => toast.remove(), 220);
  }, durationMs);
}

// ─────────────────────────────────────────────
// CSS для спиннера (вставляется один раз)
// ─────────────────────────────────────────────
(function injectSpinnerCss() {
  if (document.getElementById('domq-utils-css')) return;
  const s = document.createElement('style');
  s.id = 'domq-utils-css';
  s.textContent = `
    .domq-spinner {
      display: inline-block; width: 16px; height: 16px;
      border: 2px solid rgba(255,255,255,.4);
      border-top-color: #fff; border-radius: 50%;
      animation: domq-spin .6s linear infinite;
      vertical-align: middle;
    }
    @keyframes domq-spin { to { transform: rotate(360deg); } }
  `;
  document.head.appendChild(s);
})();
