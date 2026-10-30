# Pricing

This package owns candidate resolution, configured/context/service-tier rates,
legacy multipliers and token-cost arithmetic. `ResolveModelPrice` exposes the
same two-pass candidate precedence used by legacy monetary consumers; no caller
may infer a billing identity from an arbitrary registration alias.

`EstimateCost` adds availability without changing legacy CostFor APIs. Missing
rates remain distinct from configured zero. Cache writes are priced exactly as the
legacy dashboard prices them (configured cache-write rate, else prompt rate), so
both views agree; the price book has no TTL dimension. Missing usage,
component rates, legacy cache presence and implicit tier rates make estimates
partial or unavailable. Failed outcomes do not suppress consumed work.

`PriceRevision` hashes rate values and configured flags, context/service schedules
and a pricing version; it excludes sync times and source payloads. It describes
current configured rates, not historically effective prices or invoices.

Tests cover legacy arithmetic and candidate precedence, missing versus explicit
zero prices, measured versus unknown zero usage, cache-write parity with legacy cost, and rate
revision stability across metadata refreshes versus monetary edits.
