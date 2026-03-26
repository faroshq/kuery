//go:build e2e

package e2e_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/faroshq/kuery/apis/query/v1alpha1"
	"github.com/faroshq/kuery/internal/engine"
	"github.com/faroshq/kuery/internal/store"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"gorm.io/datatypes"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
)

// setupPostgresStore starts a PostgreSQL container and returns a connected store.
func setupPostgresStore(t *testing.T) store.Store {
	t.Helper()
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx, "postgres:16",
		postgres.WithDatabase("kuery_test"),
		postgres.WithUsername("kuery"),
		postgres.WithPassword("kuery"),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { pgContainer.Terminate(ctx) })

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("get connection string: %v", err)
	}

	s, err := store.NewStore(store.Config{
		Driver: "postgres",
		DSN:    connStr,
	})
	if err != nil {
		t.Fatalf("create postgres store: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	if err := s.AutoMigrate(); err != nil {
		t.Fatalf("auto-migrate: %v", err)
	}

	return s
}

// --- Seed helpers (same patterns as internal/engine/engine_test.go) ---

func pgMustJSON(v any) datatypes.JSON {
	b, _ := json.Marshal(v)
	return datatypes.JSON(b)
}

func pgTS(s string) *time.Time {
	t, _ := time.Parse(time.RFC3339, s)
	return &t
}

func pgSeedDeployment(t *testing.T, s store.Store, cluster, ns, name string, labels map[string]string) *store.ObjectModel {
	t.Helper()
	obj := map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": name, "namespace": ns, "labels": labels},
		"spec":     map[string]any{"replicas": 3},
	}
	m := &store.ObjectModel{
		ID: uuid.New(), UID: uuid.New().String(), Cluster: cluster,
		APIGroup: "apps", APIVersion: "v1", Kind: "Deployment", Resource: "deployments",
		Namespace: ns, Name: name, Labels: pgMustJSON(labels),
		CreationTS: pgTS("2025-06-01T00:00:00Z"), Object: pgMustJSON(obj),
	}
	if err := s.UpsertObject(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	return m
}

func pgSeedPod(t *testing.T, s store.Store, cluster, ns, name, parentUID string) *store.ObjectModel {
	t.Helper()
	ownerRefs := []map[string]string{{"uid": parentUID}}
	obj := map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": name, "namespace": ns},
		"spec":     map[string]any{"volumes": []any{}},
	}
	m := &store.ObjectModel{
		ID: uuid.New(), UID: uuid.New().String(), Cluster: cluster,
		APIVersion: "v1", Kind: "Pod", Resource: "pods",
		Namespace: ns, Name: name, OwnerRefs: pgMustJSON(ownerRefs),
		CreationTS: pgTS("2025-06-01T00:00:00Z"), Object: pgMustJSON(obj),
	}
	if err := s.UpsertObject(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	return m
}

func pgSeedRT(t *testing.T, s store.Store, cluster, group, version, kind, resource string) {
	t.Helper()
	rt := &store.ResourceTypeModel{
		Cluster: cluster, APIGroup: group, APIVersion: version,
		Kind: kind, Singular: strings.ToLower(kind), Resource: resource,
		ShortNames: pgMustJSON([]string{}), Categories: pgMustJSON([]string{"all"}),
		Namespaced: true,
	}
	if err := s.UpsertResourceType(context.Background(), rt); err != nil {
		t.Fatal(err)
	}
}

func pgSeedLinkedObject(t *testing.T, s store.Store, cluster, ns, name string, relatesTo []map[string]string) *store.ObjectModel {
	t.Helper()
	annotations := map[string]any{"kuery.io/relates-to": relatesTo}
	obj := map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"name": name, "namespace": ns, "annotations": annotations},
	}
	m := &store.ObjectModel{
		ID: uuid.New(), UID: uuid.New().String(), Cluster: cluster,
		APIVersion: "v1", Kind: "ConfigMap", Resource: "configmaps",
		Namespace: ns, Name: name, Annotations: pgMustJSON(annotations),
		CreationTS: pgTS("2025-06-01T00:00:00Z"), Object: pgMustJSON(obj),
	}
	if err := s.UpsertObject(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	return m
}

func pgSeedSecret(t *testing.T, s store.Store, cluster, ns, name string, labels map[string]string) *store.ObjectModel {
	t.Helper()
	obj := map[string]any{
		"apiVersion": "v1", "kind": "Secret",
		"metadata": map[string]any{"name": name, "namespace": ns, "labels": labels},
	}
	m := &store.ObjectModel{
		ID: uuid.New(), UID: uuid.New().String(), Cluster: cluster,
		APIVersion: "v1", Kind: "Secret", Resource: "secrets",
		Namespace: ns, Name: name, Labels: pgMustJSON(labels),
		CreationTS: pgTS("2025-06-01T00:00:00Z"), Object: pgMustJSON(obj),
	}
	if err := s.UpsertObject(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	return m
}

// --- PostgreSQL Integration Tests ---

func TestPostgres_BasicQuery(t *testing.T) {
	s := setupPostgresStore(t)
	pgSeedDeployment(t, s, "c1", "default", "nginx", map[string]string{"app": "nginx"})
	pgSeedDeployment(t, s, "c1", "default", "redis", map[string]string{"app": "redis"})

	e := engine.NewEngine(s)
	status, err := e.Execute(context.Background(), &v1alpha1.QuerySpec{Count: true})
	if err != nil {
		t.Fatal(err)
	}
	if status.Count == nil || *status.Count != 2 {
		t.Fatalf("expected count=2, got %v", status.Count)
	}
}

func TestPostgres_GroupKindFilter(t *testing.T) {
	s := setupPostgresStore(t)
	pgSeedDeployment(t, s, "c1", "default", "nginx", nil)
	pgSeedRT(t, s, "c1", "apps", "v1", "Deployment", "deployments")

	e := engine.NewEngine(s)
	status, err := e.Execute(context.Background(), &v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{
				{GroupKind: &v1alpha1.GroupKindFilter{APIGroup: "apps", Kind: "Deployment"}},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Objects) != 1 {
		t.Fatalf("expected 1 Deployment, got %d", len(status.Objects))
	}
}

func TestPostgres_LabelsContainment(t *testing.T) {
	// Test PostgreSQL @> JSONB containment operator for labels.
	s := setupPostgresStore(t)
	pgSeedDeployment(t, s, "c1", "default", "nginx", map[string]string{"app": "nginx", "env": "prod"})
	pgSeedDeployment(t, s, "c1", "default", "redis", map[string]string{"app": "redis"})

	e := engine.NewEngine(s)
	status, err := e.Execute(context.Background(), &v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{
				{Labels: map[string]string{"app": "nginx"}},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Objects) != 1 {
		t.Fatalf("expected 1 object with app=nginx, got %d", len(status.Objects))
	}
}

func TestPostgres_Descendants(t *testing.T) {
	s := setupPostgresStore(t)
	deploy := pgSeedDeployment(t, s, "c1", "default", "nginx", nil)
	pgSeedPod(t, s, "c1", "default", "nginx-pod-1", deploy.UID)
	pgSeedPod(t, s, "c1", "default", "nginx-pod-2", deploy.UID)

	projJSON, _ := json.Marshal(map[string]any{"metadata": map[string]any{"name": true}})
	e := engine.NewEngine(s)
	status, err := e.Execute(context.Background(), &v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{{Name: "nginx"}},
		},
		Objects: &v1alpha1.ObjectsSpec{
			Object: &k8sruntime.RawExtension{Raw: projJSON},
			Relations: map[string]v1alpha1.RelationSpec{
				"descendants": {Objects: &v1alpha1.ObjectsSpec{Object: &k8sruntime.RawExtension{Raw: projJSON}}},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Objects) != 1 {
		t.Fatalf("expected 1 root, got %d", len(status.Objects))
	}
	descendants := status.Objects[0].Relations["descendants"]
	if len(descendants) != 2 {
		t.Fatalf("expected 2 descendants, got %d", len(descendants))
	}
}

func TestPostgres_TransitiveCTE(t *testing.T) {
	s := setupPostgresStore(t)
	deploy := pgSeedDeployment(t, s, "c1", "default", "nginx", nil)
	pod := pgSeedPod(t, s, "c1", "default", "nginx-pod", deploy.UID)
	// Create a grandchild to test recursion.
	pgSeedPod(t, s, "c1", "default", "nginx-sidecar", pod.UID)

	projJSON, _ := json.Marshal(map[string]any{"metadata": map[string]any{"name": true}})
	e := engine.NewEngine(s)
	status, err := e.Execute(context.Background(), &v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{{Name: "nginx"}},
		},
		Objects: &v1alpha1.ObjectsSpec{
			Object: &k8sruntime.RawExtension{Raw: projJSON},
			Relations: map[string]v1alpha1.RelationSpec{
				"descendants+": {Objects: &v1alpha1.ObjectsSpec{Object: &k8sruntime.RawExtension{Raw: projJSON}}},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Objects) == 0 {
		t.Fatal("expected root")
	}
	// Should find pod at depth 1, sidecar at depth 2 (nested).
	d1 := status.Objects[0].Relations["descendants+"]
	if len(d1) != 1 {
		t.Fatalf("expected 1 direct descendant, got %d", len(d1))
	}
	d2 := d1[0].Relations["descendants+"]
	if len(d2) != 1 {
		t.Fatalf("expected 1 nested descendant, got %d", len(d2))
	}
}

func TestPostgres_CrossClusterLinked(t *testing.T) {
	s := setupPostgresStore(t)
	pgSeedLinkedObject(t, s, "c1", "default", "my-config",
		[]map[string]string{{"cluster": "c2", "kind": "Secret", "namespace": "default", "name": "shared-cert"}})
	pgSeedSecret(t, s, "c2", "default", "shared-cert", nil)

	projJSON, _ := json.Marshal(map[string]any{"metadata": map[string]any{"name": true}})
	e := engine.NewEngine(s)
	status, err := e.Execute(context.Background(), &v1alpha1.QuerySpec{
		Filter: &v1alpha1.QueryFilter{
			Objects: []v1alpha1.ObjectFilter{{Name: "my-config"}},
		},
		Objects: &v1alpha1.ObjectsSpec{
			Cluster: true,
			Object:  &k8sruntime.RawExtension{Raw: projJSON},
			Relations: map[string]v1alpha1.RelationSpec{
				"linked": {Objects: &v1alpha1.ObjectsSpec{Cluster: true, Object: &k8sruntime.RawExtension{Raw: projJSON}}},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Objects) == 0 {
		t.Fatal("expected root")
	}
	linked := status.Objects[0].Relations["linked"]
	if len(linked) != 1 {
		t.Fatalf("expected 1 linked object, got %d", len(linked))
	}
	if linked[0].Cluster != "c2" {
		t.Fatalf("expected linked from c2, got %q", linked[0].Cluster)
	}
}

func TestPostgres_Projection(t *testing.T) {
	s := setupPostgresStore(t)
	pgSeedDeployment(t, s, "c1", "default", "nginx", map[string]string{"app": "nginx"})

	projJSON, _ := json.Marshal(map[string]any{
		"metadata": map[string]any{"name": true},
		"spec":     map[string]any{"replicas": true},
	})
	e := engine.NewEngine(s)
	status, err := e.Execute(context.Background(), &v1alpha1.QuerySpec{
		Objects: &v1alpha1.ObjectsSpec{
			Object: &k8sruntime.RawExtension{Raw: projJSON},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Objects) == 0 {
		t.Fatal("expected result")
	}
	obj := status.Objects[0].Object
	if obj == nil {
		t.Fatal("expected projected object")
	}
	var projected map[string]any
	json.Unmarshal(obj.Raw, &projected)
	meta := projected["metadata"].(map[string]any)
	if meta["name"] != "nginx" {
		t.Fatalf("expected name=nginx, got %v", meta["name"])
	}
	// labels should NOT be present.
	if _, ok := meta["labels"]; ok {
		t.Fatal("labels should not be in projection")
	}
	spec := projected["spec"].(map[string]any)
	if spec["replicas"] != float64(3) {
		t.Fatalf("expected replicas=3, got %v", spec["replicas"])
	}
}
