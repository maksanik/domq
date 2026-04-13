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
 * Возвращает hex-цвет (#rrggbb) по медианной цене/м²
 * Градиент: зелёный (дёшево) → жёлтый → красный (дорого)
 * @param {number} pricePerM2
 */
export function priceToColor(pricePerM2) {
  const t = Math.max(0, Math.min(1, (pricePerM2 - PRICE_MIN) / (PRICE_MAX - PRICE_MIN)));
  let r, g, b;
  if (t < 0.5) {
    // зелёный (#4ADE80) → жёлтый (#FACC15)
    const s = t * 2;
    r = Math.round(0x4A + (0xFA - 0x4A) * s);
    g = Math.round(0xDE + (0xCC - 0xDE) * s);
    b = Math.round(0x80 + (0x15 - 0x80) * s);
  } else {
    // жёлтый (#FACC15) → красный (#EF4444)
    const s = (t - 0.5) * 2;
    r = Math.round(0xFA + (0xEF - 0xFA) * s);
    g = Math.round(0xCC + (0x44 - 0xCC) * s);
    b = Math.round(0x15 + (0x44 - 0x15) * s);
  }
  return `#${r.toString(16).padStart(2,'0')}${g.toString(16).padStart(2,'0')}${b.toString(16).padStart(2,'0')}`;
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
