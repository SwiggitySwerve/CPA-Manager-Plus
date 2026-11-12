# Monitoring analytics

The monitoring service owns read orchestration and safe result projection. The
existing usage repositories own filtering, grouping, paging and retention reads;
`service/pricing` owns all rates, resolution and money calculations. Prox has no
pricing arithmetic and receives no raw envelopes, credential hashes or bodies.

`ProxAnalytics` is a fixed, panel-authorized, server-to-server operation. Its
catalog is supplied by the trusted adapter, not a browser. Queries accept at most
31 days, 200 rows, 1000 registrations, 256 bytes of search, an IANA timezone and a
paired timestamp/ID cursor. Unknown JSON fields and trailing input are rejected.
Two queries may run per service, plus one reserved slot for queries the hourly rollups
can serve (no search filter; `analyticsHourlyRollupEligible`), so at most three run at
once. The controller's 30-second deadline includes the wait for a slot. A rollup-eligible
query tries the reserved slot first and then either slot; search-filtered queries scan
raw rows, can take the whole deadline cold, and never use the reserved slot, so two of
them cannot queue a fast view. The reserved slot is admission only: all queries still
share the SQLite connection pool.
The pool queues its writers (`internal/repository/sqlite/write_gate.go`): a write transaction
or a statement outside a transaction waits its turn in Go, bounded by its context, instead
of polling SQLite until `busy_timeout` and failing with SQLITE_BUSY.
Read-only transactions and queries never queue, so analytics reads do not wait for writers.
Writes run as queries (`INSERT ... RETURNING`) queue until their rows close. A queued writer
keeps its pool connection, so the pool has 33 connections beyond its configured 4 for the
gate holder and at most 32 queued writers, and no pool writer touches SQLite outside the
gate: one that finds the queue full, or waits 2 minutes, fails with `ErrWriteGateUnavailable`
and returns its connection (`TestPoolWriteQueueOverflowKeepsReaderConnections`).
Every view then reads under one model-price snapshot (`WithModelPriceSnapshot`). A price
save waits for running snapshots and blocks new ones, so that wait also ends with the
request context: a cancelled or timed-out request returns its context error instead of
queueing behind a long read and the save (`TestWithModelPriceSnapshotWaitEndsWithContext`).

Model, outcome and provider filters are served by the hourly rollups. The provider is a
key of `usage_pricing_hourly_provider_rollups_v1` only: the pricing rollup fills it in
the same batch as `usage_pricing_hourly_rollups_v1`, with the same rows split by
`coalesce(provider, '')` and `coalesce(auth_provider_snapshot, '')`, and a provider
filter is the raw path's predicate on those two columns (either one matches, compared in
lower case). The permanent core aggregate has no provider, so the store first checks the
provider rows of every provider against it, then narrows the source that passed and
derives the summary from those pricing rows; provider rows that do not add up to the core
are recomputed from events, never served. Any new key column must keep that check: it is
what makes a stale or half-built table read as slow, not as wrong. The table gets its
history from the pricing rollup's rebuild, triggered by the layout suffix on
`usagepricing.StructureRevision`; `stats_v1` keeps `PriceStructureRevision` and does not
rebuild. While the rebuild runs, every view the pricing rollup serves reads events instead.

Latency has two meanings and both hourly pricing tables keep both. `latency_sum_ms` and
`latency_samples` count every nonzero latency, like the core aggregate, the raw summary and
the coverage check. `positive_latency_sum_ms` and `positive_latency_samples` count latencies
above zero only, like the raw timeline and the latency ranks; the rollup timeline reads those.
Ingestion keeps negative timings, so the two differ whenever one is stored
(`TestProxRollupTimelineIgnoresNonPositiveLatency`; the equivalence fixture stores negative
latency and TTFT). The coverage check cannot see the positive pair: it is exact because the
`provider-2` layout revision rebuilds every pricing row with it. TTFT has no rollup column.
Timeline points are summed in full key order, pricing band included
(`TestBuildTimelineIsIndependentOfPricingBandOrder`).

Every primary view excludes explicitly supplementary records using the usage
owner's retained-envelope predicate. Legacy records without record kind remain
included and ambiguous. No IDs, stored rows, accounting values, archive retention or
historical tiers are rewritten. No retry or inbound-request claim is made.

Providers and model rates come from full filtered aggregation plus registration,
never the request page. `ProxAnalytics` ranks latency in Go: one
`LatencyBreakdownWithFilter` scan keeps every positive latency and TTFT sample of
its range in memory, in four groups per row (overall, bucket, model, provider).
That is bounded by one scan of at most 31 days with at most two admitted
concurrently, not by SQLite ranking; `GroupedLatencyWithFilter` still ranks in
SQLite and holds only results. Uninstrumented TTFT stays unknown; output speed divides
output by full latency only. Request zero tokens require reported presence and a
true component flag; positive retained legacy counters stay visible. Aggregates
remain known retained counters with explicit measurement limitations.

New ingestion applies validated canonical output totals (including Gemini
thoughts) and explicit effective tier after identity generation. Archive restore
preserves archived derived values. Historical repair needs a separately approved
and verified typed migration, including projection/rollup invalidation.

A response does not depend on the path that produced it. `ProxAnalytics` orders pricing
rows by their whole key before it combines cost estimates, because estimates add floats
and list reasons in row order; `TestProxProviderRollupMatchesRawPath` compares the raw
and rollup responses byte for byte (`PROX_ROLLUP_EQUIVALENCE=full` for every filter on
every range).

Focused enforcement: `TestProxAnalyticsWholeScopePagingRatesAndSafeFields`,
`TestProxAnalyticsPanelAuthorizationAndStrictBody`, `TestProxNullableRequestTokens`,
`TestProxQueryBoundsAndCancelledAdmission`,
`TestProxFastQueryIsAdmittedWhileSlowQueriesHoldGeneralSlots`,
`TestProxProviderRollupMatchesRawPath`, `TestProxProviderFilterExcludesSupplementaryRecords`,
`TestReaderProviderFilterMatchesRawEvents`, and the pricing/usage analytics tests.

When it exists, the raw latency scan behind a view without a search term reads only
`idx_usage_events_ts_latency_groups_cover` (timestamp, latency, TTFT, model, requested model,
provider columns and `failed`); search predicates read other columns and still scan the rows.
Without the index the scan reads the wide `usage_events` rows and a cold 30-day view exceeds the
30-second deadline; the index is optional for correctness, so a database that has not built it
yet only reads slower.
On a non-empty database `cleanup-derived` builds it offline, like the other deferred indexes.
Ranks are picked by selection, not by sorting each group, and equal the SQL nearest rank.

The measured 30-day snapshot timeout justified `idx_usage_events_supplementary_v1`,
a partial SQLite index of supplementary timestamp/IDs. Startup builds it once
through `EnsureAnalyticsSupplementaryIndex`; no raw values are updated. Inserts,
archive restores and deletions update it atomically through SQLite. Queries with
no supplementary records reuse existing fast readers; an indexed end-of-query
check retries once on the raw filtered path if supplementary ingestion races the
read. Deleted raw history forces the raw path because old rollups cannot exclude
archived supplementary records. The missing-index error fails closed.
