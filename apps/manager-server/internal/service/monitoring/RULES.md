# Monitoring analytics

The monitoring service owns read orchestration and safe result projection. The
existing usage repositories own filtering, grouping, paging and retention reads;
`service/pricing` owns all rates, resolution and money calculations. Prox has no
pricing arithmetic and receives no raw envelopes, credential hashes or bodies.

`ProxAnalytics` is a fixed, panel-authorized, server-to-server operation. Its
catalog is supplied by the trusted adapter, not a browser. Queries accept at most
31 days, 200 rows, 1000 registrations, 256 bytes of search, an IANA timezone and a
paired timestamp/ID cursor. Unknown JSON fields and trailing input are rejected.
Two queries may run per service, under the controller's 30-second deadline.

Every primary view excludes explicitly supplementary records using the usage
owner's retained-envelope predicate. Legacy records without record kind remain
included and ambiguous. No IDs, stored rows, accounting values, archive retention or
historical tiers are rewritten. No retry or inbound-request claim is made.

Providers and model rates come from full filtered aggregation plus registration,
never the request page. Grouped latency uses SQLite ranking so Go holds only
results, not all samples. Uninstrumented TTFT stays unknown; output speed divides
output by full latency only. Request zero tokens require reported presence and a
true component flag; positive retained legacy counters stay visible. Aggregates
remain known retained counters with explicit measurement limitations.

New ingestion applies validated canonical output totals (including Gemini
thoughts) and explicit effective tier after identity generation. Archive restore
preserves archived derived values. Historical repair needs a separately approved
and verified typed migration, including projection/rollup invalidation.

Focused enforcement: `TestProxAnalyticsWholeScopePagingRatesAndSafeFields`,
`TestProxAnalyticsPanelAuthorizationAndStrictBody`, `TestProxNullableRequestTokens`,
`TestProxQueryBoundsAndCancelledAdmission`, and the pricing/usage analytics tests.

The measured 30-day snapshot timeout justified `idx_usage_events_supplementary_v1`,
a partial SQLite index of supplementary timestamp/IDs. Startup builds it once
through `EnsureAnalyticsSupplementaryIndex`; no raw values are updated. Inserts,
archive restores and deletions update it atomically through SQLite. Queries with
no supplementary records reuse existing fast readers; an indexed end-of-query
check retries once on the raw filtered path if supplementary ingestion races the
read. Deleted raw history forces the raw path because old rollups cannot exclude
archived supplementary records. The missing-index error fails closed.
