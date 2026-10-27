# Account diagnostics verification — 2026-10-27

The live panel uses the existing five authenticated accounts. Identities in
screenshots are masked. Isolated Chromium exercised both layouts, account details,
manual quota refresh, mobile sizing, and light/dark themes.

- Before: table displayed zero of four current provider errors; cards displayed
  three, hiding the Codex usage-limit message.
- Final display: table and cards identify all four weekly-exhausted accounts using
  the existing badge and reset display, without duplicate provider error panels.
  Full messages and retry times remain in details. Unknown limits, unrelated
  provider errors, and quota-fetch failures remain visible in both layouts.
- `verbose-table-dark.png` records the intermediate display that was simplified
  after user feedback. `after-*` images show the final display.
- Browser: no uncaught errors, failed panel HTTP requests, or mobile page overflow.
  Provider errors returned inside successful management responses are displayed.
- Cached-refresh regression: one HTTP 503 quota response was injected only in the
  disposable browser after a real successful quota read. Table, cards, and Details
  kept the error visible alongside unchanged cached weekly capacity.
  `cached-failure-*.png` records this test; it is not a live provider failure.
- Checks: 4,564 web tests, 266 repository tests, typecheck, build, artifact checks,
  and lint passed. Lint retains seven existing warnings.
- Live inference: GPT returned HTTP 200 and `PROXY_OK`. Claude returned HTTP 429
  because all three credentials were cooling down. The earliest reported retry
  was 2026-10-27 14:00:17 UTC. This change does not alter provider limits or routing.

See the JSON snapshots for observed results and the PNGs for visual evidence.
