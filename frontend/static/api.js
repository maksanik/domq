/**
 * DomQ — API client
 * Обёртка над REST API FastAPI-бэкенда.
 * Базовый URL берётся из window.API_BASE или window.location.origin.
 */

const API_BASE = window.API_BASE ?? '';

/** @param {string} path @param {Record<string,any>} params */
async function apiFetch(path, params = {}) {
  const url = new URL(API_BASE + path, window.location.href);
  Object.entries(params).forEach(([k, v]) => {
    if (v !== null && v !== undefined && v !== '') url.searchParams.set(k, v);
  });
  const res = await fetch(url.toString());
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`HTTP ${res.status}: ${text}`);
  }
  return res.json();
}

// ─────────────────────────────────────────────
// GET /listings
// ─────────────────────────────────────────────
/**
 * @param {{
 *   rooms?: number,
 *   min_price?: number, max_price?: number,
 *   min_area?: number,  max_area?: number,
 *   h3_index?: string,
 *   is_hot_deal?: boolean,
 *   sort_by?: 'price'|'price_per_m2'|'discount_percent'|'area_total',
 *   sort_order?: 'asc'|'desc',
 *   limit?: number, offset?: number
 * }} filters
 * @returns {Promise<{ total: number, items: ListingItem[] }>}
 */
export function getListings(filters = {}) {
  return apiFetch('/listings', filters);
}

// ─────────────────────────────────────────────
// GET /listings/buildings
// ─────────────────────────────────────────────
/**
 * @param {{ rooms?: number }} filters
 * @returns {Promise<{ items: BuildingPin[] }>}
 */
export function getBuildingPins(filters = {}) {
  return apiFetch('/listings/buildings', filters);
}

// ─────────────────────────────────────────────
// GET /listings/:id
// ─────────────────────────────────────────────
/** @param {number} id @returns {Promise<ListingItem>} */
export function getListing(id) {
  return apiFetch(`/listings/${id}`);
}

// ─────────────────────────────────────────────
// GET /h3-stats/map
// ─────────────────────────────────────────────
/**
 * @param {{ rooms?: number }} params
 * @returns {Promise<H3StatMapItem[]>}
 */
export function getH3StatsMap(params = {}) {
  return apiFetch('/h3-stats/map', params);
}

// ─────────────────────────────────────────────
// GET /h3-stats
// ─────────────────────────────────────────────
/**
 * @param {{ h3_index: string, rooms?: number }} params
 * @returns {Promise<H3StatDetail>}
 */
export function getH3Stats(params) {
  return apiFetch('/h3-stats', params);
}

// ─────────────────────────────────────────────
// POST /predict-price
// ─────────────────────────────────────────────
/**
 * @param {{ latitude: number, longitude: number, area_total: number,
 *           rooms: number, floor: number, floors_total: number }} body
 * @returns {Promise<PredictResult>}
 */
export async function predictPrice(body) {
  const res = await fetch(API_BASE + '/predict-price', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

// ─────────────────────────────────────────────
// GET /health
// ─────────────────────────────────────────────
/** @returns {Promise<{ status: string, db: string, last_scraped_at: string }>} */
export function getHealth() {
  return apiFetch('/health');
}

// ─────────────────────────────────────────────
// JSDoc types (для IDE-подсказок без TS)
// ─────────────────────────────────────────────
/**
 * @typedef {{
 *   id: number, external_id: string, url: string,
 *   price: number, price_per_m2: number,
 *   rooms: number, area_total: number,
 *   floor: number, floors_total: number,
 *   address: string, latitude: number, longitude: number,
 *   h3_index: string, is_active: boolean,
 *   is_hot_deal: boolean, discount_percent: number|null,
 *   first_seen_at: string, last_seen_at: string
 * }} ListingItem
 *
 * @typedef {{
 *   h3_index: string, rooms: number,
 *   median_price_per_m2: number, listings_count: number
 * }} H3StatMapItem
 *
 * @typedef {{
 *   h3_index: string, rooms: number|null,
 *   price_stats: {
 *     h3_index: string, rooms: number,
 *     median_price_per_m2: number|null, avg_price_per_m2: number|null,
 *     listings_count: number, calculated_at: string|null
 *   } | null,
 *   liquidity: { avg_days_on_market: number|null, median_days: number|null } | null,
 *   price_history: { date: string, median_price_per_m2: number|null }[]
 * }} H3StatDetail
 *
 * @typedef {{
 *   predicted_price: number, price_per_m2_used: number,
 *   h3_index: string, listings_in_cell: number,
 *   method: string, note: string|null
 * }} PredictResult
 *
 * @typedef {{
 *   building_id: number, address: string,
 *   latitude: number, longitude: number,
 *   h3_index: string, listings_count: number
 * }} BuildingPin
 */
