# Usage source identity

`event.go` owns source masking and `SourceHash`, the historical lowercase SHA-256
of the trimmed original source (empty input stays empty). Normalization, legacy
import, credential-source masking, and persistence pseudonymization all use this
public source-hash contract. Generic event/API-key hashing retains `hashString`;
raw SHA-256 encoding stays in the existing `sha256Hex` primitive. Those have
different input-normalization responsibilities and do not define another source
identity rule.

`source_identity.go` verifies plaintext or masked stored sources against an
account using these existing contracts. Exact plaintext sources retain legacy
compatibility without a hash, but any supplied hash must agree. Masked sources
require both the expected mask and the exact original-account hash. Hashing the
mask, substituting another account's hash, omitting a hash, or attaching the right
hash to an unrelated source is insufficient.

Source verification is corroboration, not independent proof of account
ownership. The account-history owner also requires matching stored credential
snapshots and rejects conflicting evidence across the auth index. No raw events
are repaired or rewritten by these read rules.

`TestSourceIdentityCompatibilityFixtures` records deterministic source and hash
outputs across normalization, legacy import, and persistence. The extraction's
before/after fixture digests must agree. `TestSourceMatchesAccount` covers masked
collisions, missing/mismatching hashes, and plaintext compatibility.
