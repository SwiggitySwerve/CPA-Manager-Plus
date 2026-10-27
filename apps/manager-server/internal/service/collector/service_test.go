package collector

import (
	"testing"

	collectorpkg "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/collector"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/config"
)

func TestStatusesIncludesEveryCollectorAndPreservesPrimaryStatus(t *testing.T) {
	primary := collectorpkg.NewManager(config.Config{Queue: "primary"}, nil)
	extra := collectorpkg.NewManager(config.Config{Queue: "claude"}, nil)
	service := New(primary)
	if statuses := service.Statuses(); len(statuses) != 1 {
		t.Fatalf("single-instance status count = %d", len(statuses))
	}
	managers := []*collectorpkg.Manager{extra}
	service.SetExtraManagers(managers)
	managers[0] = primary
	statuses := service.Statuses()
	if len(statuses) != 2 || statuses[0].Queue != "primary" || statuses[1].Queue != "claude" {
		t.Fatalf("statuses = %#v", statuses)
	}
	if service.Status().Queue != "primary" {
		t.Fatal("legacy status no longer represents the primary collector")
	}
	service.SetExtraManagers(nil)
	if statuses := service.Statuses(); len(statuses) != 1 {
		t.Fatalf("removed collector still visible: %#v", statuses)
	}
}
