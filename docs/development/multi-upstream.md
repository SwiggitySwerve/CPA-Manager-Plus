# Multiple CPA instances

The `internal/service/proxy` package owns management request routing across CPA
instances. Consumers use its public service methods. The usage collectors in
`internal/worker/extra_collectors.go` independently feed the shared usage store.

## Configuration

- `CPA_EXTRA_UPSTREAM_URLS` lists additional instances whose accounts, models, and
  usage belong in the dashboard. The primary connection remains the default.
- `CPA_PROVIDER_UPSTREAMS` maps new OAuth logins and credential uploads to an
  instance, for example `claude=http://host:8319`. A mapped instance must also be
  listed in `CPA_EXTRA_UPSTREAM_URLS` for inventory and usage collection.
- All instances use the primary connection's management key. Model-list requests
  forward the caller's API key instead.

## Account inventory and quota

The merged account inventory must be complete before a refresh succeeds. A failed
instance or a malformed inventory returns HTTP 502 instead of a successful partial
list. The account page retains its last successful list and displays the refresh
error. On the first load, an unavailable instance prevents a complete inventory
from loading; retry after that instance recovers.

Credential-specific requests, including quota `api-call` requests, go to the
instance that owns the credential. Ownership is cached briefly and refreshed on
cache misses. A duplicate credential identity keeps the primary instance's entry;
`X-CPAMP-Upstream-Conflicts` reports the conflict.

Usage collectors write into the same database and feed shared usage rollups.
Account inventory and quota routing do not extend background inspection to new
provider types or merge instance configuration editing.

## Verification

Run the proxy and worker Go tests, including race detection, and the frontend
account-refresh and provider-quota tests. Live verification should check the
merged account list, one quota request per account, and usage records from both
providers. Never poll a production usage queue as a diagnostic: reading it can
consume events intended for the collector.
