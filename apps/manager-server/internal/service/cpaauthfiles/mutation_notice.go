package cpaauthfiles

// MutationNotice tells a listener what a successful proxied auth-file mutation
// removed, so it can treat exactly those names as gone without waiting for a
// second listing. Everything else still has to be confirmed by listings.
type MutationNotice struct {
	// Upstream is the CLI-Proxy base URL the request was sent to.
	Upstream string
	// Deleted are the file names the mutation deleted.
	Deleted []string
	// DeletedAll is set when the mutation deleted every file of Upstream.
	DeletedAll bool
}
