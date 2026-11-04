package setting

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"

	_ "modernc.org/sqlite"
)

func TestKnownAuthFilesRoundTrip(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "settings.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err = db.Exec("create table settings(key text primary key,value text not null,updated_at_ms integer not null)"); err != nil {
		t.Fatal(err)
	}
	repo := New(db)
	ctx := context.Background()
	if got, err := repo.LoadKnownAuthFiles(ctx); err != nil || got != nil {
		t.Fatalf("empty load = %v, %v; want nil", got, err)
	}
	for _, names := range [][]string{{"a.json", "b.json"}, {"b.json"}} {
		if err := repo.SaveKnownAuthFiles(ctx, names); err != nil {
			t.Fatalf("save: %v", err)
		}
		if got, err := repo.LoadKnownAuthFiles(ctx); err != nil || !reflect.DeepEqual(got, names) {
			t.Fatalf("load = %v, %v; want %v", got, err, names)
		}
	}
}
