# Account-history identity

`identity.go` owns canonical history keys. `claude_legacy.go` owns the Claude
compatibility decision; `usageevent` supplies stored evidence and `monitoring`
applies the resulting alias only to reads. Canonical keys and raw events stay
unchanged.

Claude source/index compatibility requires all of the following:

- The target supplies an explicit Claude provider, filename, auth index, and account.
- A stored event corroborates that filename/index with explicit provider and
  account snapshots, including any requested account ID and project.
- Source-only events have an explicit Claude provider. The usage owner verifies
  their source through `usage.SourceMatchesAccount`: exact plaintext account
  matches may omit the hash; a supplied hash must agree. Masked sources must
  match the existing `maskSource` output and carry the exact lowercase
  `usage.SourceHash` of the original account. Missing, malformed, or mismatching
  hashes cannot authorize masked sources. A matching hash with an unrelated
  source representation also fails.
- The alias comes from the actual stored source and index, including masking.
  Every candidate must have that same canonical source/index key. Multiple
  different legacy keys deny the whole alias, even if individually corroborated.
- Every event sharing the index agrees with the target's provider, explicit file,
  account, account ID, and project whenever those fields are present. A conflicting
  snapshot, unknown provider, or unrelated source denies the alias.
- Current targets in the same request must also agree for a shared index, even
  if a conflicting credential has not produced a stored event yet.
- Raw identity evidence remains available. The repository denies Claude aliases
  if the identity ledger contains archived events without raw rows. This is
  deliberately conservative until durable Claude identity evidence exists.

Email alone and an auth index alone never authorize an alias. Missing evidence
leaves the original source bucket unassigned; it does not justify rewriting raw
events or expanding identity rules. The service retains its alias collision and
concurrent-insert fences. Codex keeps its existing member/workspace compatibility
resolver. Account-window and recent-request identity rules are unchanged.

The repository keeps distinct source/hash pairs when collecting evidence, so a
valid hash cannot hide a conflicting or missing hash on another event. It
enumerates raw index variants before querying evidence because Go canonical
keys trim Unicode whitespace as well as ASCII spaces. The usage package already
imports usageidentity; source verification therefore flows as a verified boolean
from usage through the repository to usageidentity, without a reverse import.

`TestResolveLegacyAccountKeyClaudeEvidence` checks the evidence boundary and raw
immutability. `TestAccountHistoryClaudeSourceAlias` checks totals, repeated reads,
history bounds, and credential isolation through the service. Existing Codex
identity and monitoring tests remain regression coverage.
