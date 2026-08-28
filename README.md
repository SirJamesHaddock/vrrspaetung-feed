# VRRspätung live feed

Free, serverless data feed for the VRRspätung app. A GitHub Actions job refreshes
`vrr-status.json` roughly every 5 minutes from the public gtfs.de GTFS-Realtime
feed, projected onto a daily VRR bus timetable index (`vrr-index.json`).

- `vrr-status.json` — current VRR bus problems (CANCELED + DELAYED). The app reads this.
- `vrr-index.json` — VRR bus trips for the day (trip_id → line, origin, destination, scheduled departure).
- `build-live-status.mjs` — RT feed → slim status.
- `build-vrr-index.mjs` — daily full-dataset filter to VRR buses.

Realtime and schedule data © gtfs.de / DELFI e.V., licensed **CC BY-SA 4.0**.
This is an unofficial project and not affiliated with VRR.
