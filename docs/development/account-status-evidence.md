# Account status evidence

`accountHealthEvidence.ts` owns classification of request, quota, and authentication
failures. `accountRows.ts` reconciles raw credential status with dated recovery
evidence. A successful quota lookup proves authentication; it does not prove that
inference quota or upstream failures have recovered. Timestamp-only authentication
recovery may suppress authentication failures only. Explicitly handled status
boundaries retain their existing scope; quota refresh must only create a raw
status boundary for an authentication failure.

Table, card, and detail views consume `buildOverviewRecentStatus` and render the
same `AccountStatusMessage`. Known five-hour, weekly, or monthly exhaustion uses
the existing health label and reset display in the list. The full provider message
and proxy retry time remain in account details. Unknown limits and unrelated
errors remain visible in both list layouts. Request history stays separate from current provider
status. Quota-lookup errors stay visible even when no quota windows are available
or cached windows remain. The quota-refresh evidence classifier detects these
failures independently of the capacity status derived from cached windows.
Messages are readable provider errors, without truncation. An unavailable
credential's valid `next_retry_after` is shown as the proxy's retry time; it is not a
promise that the provider will accept the next request. Quota resets, credential
state, and routing remain owned by the provider and proxy.

Regression coverage lives in `accountRows.test.ts`, `accountHealthEvidence.test.ts`,
`accountDetailViewModel.test.ts`, and `AccountsPage.test.tsx`.
