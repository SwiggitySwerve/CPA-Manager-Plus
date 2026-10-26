package worker

import (
	"context"
	"log"
	"strings"

	collectorpkg "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/collector"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/config"
	collectorservice "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/service/collector"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
)

// StartExtraCollectors runs one more usage collector per CPA_EXTRA_UPSTREAM_URLS entry, writing into the
// same store as the primary collector. It is for deployments that split CPA into one instance per provider.
// Extra collectors reuse the primary connection's management key and collector settings. They get only the
// rollup handlers: automation handlers follow the primary connection, and a second runtime config would
// redirect them.
func StartExtraCollectors(ctx context.Context, cfg config.Config, st *store.Store,
	handler collectorpkg.UsageEventHandler) []*collectorpkg.Manager {
	if len(cfg.ExtraUpstreamURLs) == 0 {
		return nil
	}
	base := collectorpkg.RuntimeConfig{
		CPAUpstreamURL: cfg.CPAUpstreamURL,
		ManagementKey:  cfg.ManagementKey,
		CollectorMode:  cfg.CollectorMode,
		Queue:          cfg.Queue,
		PopSide:        cfg.PopSide,
		BatchSize:      cfg.BatchSize,
		PollInterval:   cfg.PollInterval,
		TLSSkipVerify:  cfg.TLSSkipVerify,
	}
	if managerCfg, ok, err := st.LoadManagerConfig(ctx); err == nil && ok {
		base = collectorservice.RuntimeConfigFromManagerConfigWithFallback(managerCfg, cfg)
	} else if err != nil {
		log.Printf("extra collectors: load manager config: %v", err)
	}
	if base.ManagementKey == "" {
		log.Printf("extra collectors: no management key configured; not starting %d extra collector(s)",
			len(cfg.ExtraUpstreamURLs))
		return nil
	}
	managers := make([]*collectorpkg.Manager, 0, len(cfg.ExtraUpstreamURLs))
	for _, upstream := range cfg.ExtraUpstreamURLs {
		if strings.TrimRight(upstream, "/") == strings.TrimRight(base.CPAUpstreamURL, "/") {
			log.Printf("extra collectors: %s is the primary upstream; skipped", upstream)
			continue
		}
		runtime := base
		runtime.CPAUpstreamURL = upstream
		manager := collectorpkg.NewManager(cfg, st)
		manager.SetUsageEventHandler(handler)
		manager.Start(ctx, runtime)
		log.Printf("extra collectors: collecting usage from %s", upstream)
		managers = append(managers, manager)
	}
	return managers
}
