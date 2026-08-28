// Build a compact VRR bus lookup index from a gtfs.de static dataset.
// Streams the (multi-GB) stop_times once. Output: trip_id -> { line, origin,
// dest, scheduledDeparture, serviceDate }. Only bus trips (route_type 3) that
// run on the target date AND touch a stop inside the VRR bounding box.
//
// Usage: node build-vrr-index.mjs <gtfsDir> <outFile> [YYYY-MM-DD]
import { createReadStream } from 'node:fs';
import { writeFile } from 'node:fs/promises';
import { createInterface } from 'node:readline';

const [gtfsDir, outFile, dateArg] = process.argv.slice(2);
if (!gtfsDir || !outFile) {
  console.error('usage: build-vrr-index.mjs <gtfsDir> <outFile> [YYYY-MM-DD]');
  process.exit(1);
}

// VRR bounding box (generous: Kleve/Duisburg/Essen/Dortmund/Düsseldorf/Wuppertal/Hagen).
const BBOX = { latMin: 51.0, latMax: 51.75, lonMin: 6.0, lonMax: 7.7 };
const TZ = 'Europe/Berlin';

// Offset (seconds) of Europe/Berlin from UTC at the given date (handles CET/CEST).
function berlinOffsetSeconds(y, m, d) {
  const utc = new Date(Date.UTC(y, m - 1, d, 12, 0, 0));
  const fmt = new Intl.DateTimeFormat('en-US', {
    timeZone: TZ, hour12: false, year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit',
  });
  const parts = Object.fromEntries(fmt.formatToParts(utc).map((p) => [p.type, p.value]));
  const asUtc = Date.UTC(Number(parts.year), Number(parts.month) - 1, Number(parts.day), Number(parts.hour), Number(parts.minute), Number(parts.second));
  return Math.round((asUtc - utc.getTime()) / 1000);
}

function targetDate() {
  if (dateArg) {
    const [y, m, d] = dateArg.split('-').map(Number);
    return { y, m, d };
  }
  const parts = Object.fromEntries(
    new Intl.DateTimeFormat('en-CA', { timeZone: TZ, year: 'numeric', month: '2-digit', day: '2-digit' })
      .formatToParts(new Date()).map((p) => [p.type, p.value]),
  );
  return { y: Number(parts.year), m: Number(parts.month), d: Number(parts.day) };
}

// Minimal CSV line splitter (handles double-quoted fields with commas).
function splitCsv(line) {
  const out = [];
  let cur = '';
  let q = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (q) {
      if (c === '"') {
        if (line[i + 1] === '"') { cur += '"'; i++; } else q = false;
      } else cur += c;
    } else if (c === '"') q = true;
    else if (c === ',') { out.push(cur); cur = ''; }
    else cur += c;
  }
  out.push(cur);
  return out;
}

async function* rows(file) {
  const rl = createInterface({ input: createReadStream(`${gtfsDir}/${file}`, 'utf8'), crlfDelay: Infinity });
  let header = null;
  for await (const line of rl) {
    if (line === '') continue;
    const cols = splitCsv(line);
    if (!header) { header = cols; continue; }
    const rec = {};
    for (let i = 0; i < header.length; i++) rec[header[i]] = cols[i];
    yield rec;
  }
}

const { y, m, d } = targetDate();
const serviceDate = `${y}-${String(m).padStart(2, '0')}-${String(d).padStart(2, '0')}`;
const serviceDateInt = Number(`${y}${String(m).padStart(2, '0')}${String(d).padStart(2, '0')}`);
const offset = berlinOffsetSeconds(y, m, d);
const localMidnightMs = Date.UTC(y, m - 1, d, 0, 0, 0) - offset * 1000;
const weekday = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'][new Date(Date.UTC(y, m - 1, d)).getUTCDay()];

console.error(`target ${serviceDate} (${weekday}), Berlin offset ${offset}s`);

// 1. Bus routes.
const busRoutes = new Map(); // route_id -> line label
for await (const r of rows('routes.txt')) {
  if (r.route_type !== '3') continue;
  busRoutes.set(r.route_id, (r.route_short_name || r.route_long_name || '').trim());
}
console.error(`bus routes: ${busRoutes.size}`);

// 2. Stops + VRR flag.
const stops = new Map(); // stop_id -> { name, inVrr }
for await (const s of rows('stops.txt')) {
  const lat = Number(s.stop_lat);
  const lon = Number(s.stop_lon);
  const inVrr = lat >= BBOX.latMin && lat <= BBOX.latMax && lon >= BBOX.lonMin && lon <= BBOX.lonMax;
  stops.set(s.stop_id, { name: s.stop_name, inVrr });
}
console.error(`stops: ${stops.size}`);

// 3. Services active on the target date (calendar + calendar_dates exceptions).
const active = new Set();
try {
  for await (const c of rows('calendar.txt')) {
    const from = Number(c.start_date);
    const to = Number(c.end_date);
    if (serviceDateInt >= from && serviceDateInt <= to && c[weekday] === '1') active.add(c.service_id);
  }
} catch { /* calendar.txt may be absent */ }
for await (const cd of rows('calendar_dates.txt')) {
  if (Number(cd.date) !== serviceDateInt) continue;
  if (cd.exception_type === '1') active.add(cd.service_id);
  else if (cd.exception_type === '2') active.delete(cd.service_id);
}
console.error(`active services today: ${active.size}`);

// 4. Candidate trips: bus route + active service.
const candidates = new Map(); // trip_id -> { line }
for await (const t of rows('trips.txt')) {
  if (!active.has(t.service_id)) continue;
  const line = busRoutes.get(t.route_id);
  if (line === undefined) continue;
  candidates.set(t.trip_id, { line });
}
console.error(`candidate bus trips today: ${candidates.size}`);

// 5. Stream stop_times: first/last stop per candidate + VRR touch.
// Columns: trip_id,arrival_time,departure_time,stop_id,stop_sequence,... (first 5 are comma-safe).
const agg = new Map(); // trip_id -> { minSeq, maxSeq, originStop, originDep, destStop, vrr }
let seen = 0;
{
  const rl = createInterface({ input: createReadStream(`${gtfsDir}/stop_times.txt`, 'utf8'), crlfDelay: Infinity });
  let first = true;
  for await (const line of rl) {
    if (first) { first = false; continue; }
    if (line === '') continue;
    // Fast split; only need the first 5 columns, which never contain commas.
    let p = 0;
    const c1 = line.indexOf(','); const tripId = line.slice(0, c1);
    if (!candidates.has(tripId)) continue;
    const c2 = line.indexOf(',', c1 + 1); const arr = line.slice(c1 + 1, c2);
    const c3 = line.indexOf(',', c2 + 1); const dep = line.slice(c2 + 1, c3);
    const c4 = line.indexOf(',', c3 + 1); const stopId = line.slice(c3 + 1, c4);
    const c5 = line.indexOf(',', c4 + 1); const seq = Number(line.slice(c4 + 1, c5 === -1 ? line.length : c5));
    p = arr; // (silence lint)
    let a = agg.get(tripId);
    if (!a) { a = { minSeq: Infinity, maxSeq: -Infinity, originStop: null, originDep: null, destStop: null, vrr: false }; agg.set(tripId, a); }
    const st = stops.get(stopId);
    if (st?.inVrr) a.vrr = true;
    if (seq <= a.minSeq) { a.minSeq = seq; a.originStop = stopId; a.originDep = dep; }
    if (seq >= a.maxSeq) { a.maxSeq = seq; a.destStop = stopId; }
    if ((++seen % 20_000_000) === 0) console.error(`  stop_times scanned ~${seen / 1_000_000}M, matched trips ${agg.size}`);
  }
}
console.error(`stop_times scanned; matched trips ${agg.size}`);

// 6. Build the VRR index.
function toIso(depTime) {
  const [hh, mm, ss] = depTime.split(':').map(Number);
  const sec = hh * 3600 + mm * 60 + (ss || 0);
  return new Date(localMidnightMs + sec * 1000).toISOString();
}

const trips = {};
let kept = 0;
for (const [tripId, a] of agg) {
  if (!a.vrr) continue;
  const line = candidates.get(tripId).line;
  const origin = a.originStop ? stops.get(a.originStop)?.name ?? null : null;
  const dest = a.destStop ? stops.get(a.destStop)?.name ?? null : null;
  let scheduledDeparture = null;
  try { if (a.originDep) scheduledDeparture = toIso(a.originDep); } catch { /* skip bad time */ }
  trips[tripId] = { line, origin, dest, scheduledDeparture };
  kept++;
}

const out = { serviceDate, tz: TZ, generatedAt: new Date().toISOString(), tripCount: kept, trips };
await writeFile(outFile, JSON.stringify(out));
console.error(`VRR bus trips kept: ${kept} -> ${outFile}`);
