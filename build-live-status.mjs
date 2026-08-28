// Fetch the free gtfs.de GTFS-RT feed and project it onto the VRR bus index,
// producing a slim problem-oriented live feed (CANCELED + DELAYED only).
// Usage: node build-live-status.mjs <vrrIndex.json> <outFile>
import { readFile, writeFile } from 'node:fs/promises';
import bindings from 'gtfs-realtime-bindings';

const { transit_realtime: rt } = bindings;
const CANCELED = rt.TripDescriptor.ScheduleRelationship.CANCELED;
const RT_URL = process.env.RT_URL || 'https://realtime.gtfs.de/realtime-free.pb';
const DELAY_THRESHOLD = 60;

const [indexFile, outFile] = process.argv.slice(2);
if (!indexFile || !outFile) { console.error('usage: build-live-status.mjs <index> <out>'); process.exit(1); }

function toNum(v) {
  if (v == null) return null;
  if (typeof v === 'number') return v;
  if (typeof v.toNumber === 'function') return v.toNumber();
  const n = Number(v); return Number.isNaN(n) ? null : n;
}
function firstDelay(stus) {
  for (const s of stus ?? []) { const d = toNum(s.departure?.delay ?? s.arrival?.delay); if (d !== null) return d; }
  return null;
}

const index = JSON.parse(await readFile(indexFile, 'utf8'));
const trips = index.trips;
const res = await fetch(RT_URL);
if (!res.ok) throw new Error(`RT fetch failed: ${res.status}`);
const msg = rt.FeedMessage.decode(Buffer.from(await res.arrayBuffer()));

const items = [];
let matched = 0;
for (const e of msg.entity ?? []) {
  const tu = e.tripUpdate;
  const tripId = tu?.trip?.tripId;
  if (!tu || !tripId) continue;
  const info = trips[tripId];
  if (!info) continue;
  matched++;
  const canceled = tu.trip?.scheduleRelationship === CANCELED;
  const delay = firstDelay(tu.stopTimeUpdate);
  let status;
  if (canceled) status = 'CANCELED';
  else if (delay !== null && delay >= DELAY_THRESHOLD) status = 'DELAYED';
  else continue;
  items.push({
    id: tripId, line: info.line, headsign: info.dest, origin: info.origin, status,
    ...(status === 'DELAYED' ? { delaySeconds: delay } : {}),
    ...(info.scheduledDeparture ? { scheduledDepartureAt: info.scheduledDeparture } : {}),
  });
}
items.sort((a, b) => (a.status !== b.status ? (a.status === 'CANCELED' ? -1 : 1) : (a.scheduledDepartureAt ?? '').localeCompare(b.scheduledDepartureAt ?? '')));

const out = {
  generatedAt: new Date().toISOString(),
  serviceDate: index.serviceDate,
  source: 'gtfs.de realtime-free (CC BY-SA 4.0)',
  counts: { matchedVrrTrips: matched, canceled: items.filter((i) => i.status === 'CANCELED').length, delayed: items.filter((i) => i.status === 'DELAYED').length },
  items,
};
await writeFile(outFile, JSON.stringify(out));
console.error(`matched ${matched}; problems ${items.length} -> ${outFile}`);
