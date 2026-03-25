//go:build e2e

package e2e_test

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/faroshq/kuery/apis/query/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
)

// newInsecureClient creates an HTTP client that skips TLS verification.
func newInsecureClient() *http.Client {
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
}

// queryKuery sends a Query spec to the kuery server and returns the status.
func queryKuery(t *testing.T, spec v1alpha1.QuerySpec) v1alpha1.QueryStatus {
	t.Helper()
	query := map[string]any{
		"apiVersion": "kuery.io/v1alpha1",
		"kind":       "Query",
		"spec":       spec,
	}
	body, err := json.Marshal(query)
	if err != nil {
		t.Fatalf("marshal query: %v", err)
	}

	resp, err := httpClient.Post(serverURL+"/apis/kuery.io/v1alpha1/queries", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST query: %v", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	// Check for error response (kind: Status).
	var raw map[string]any
	if err := json.Unmarshal(respBody, &raw); err != nil {
		t.Fatalf("unmarshal response: %v\nbody: %s", err, string(respBody))
	}

	if kind, _ := raw["kind"].(string); kind == "Status" {
		msg, _ := raw["message"].(string)
		t.Fatalf("query failed: %s", msg)
	}

	// Parse status.
	statusRaw, err := json.Marshal(raw["status"])
	if err != nil {
		t.Fatalf("marshal status: %v", err)
	}

	var status v1alpha1.QueryStatus
	if err := json.Unmarshal(statusRaw, &status); err != nil {
		t.Fatalf("unmarshal status: %v\nraw: %s", err, string(statusRaw))
	}
	return status
}

// waitForCondition polls until fn returns true or timeout expires.
func waitForCondition(timeout, interval time.Duration, fn func() bool) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return nil
		}
		time.Sleep(interval)
	}
	return fmt.Errorf("condition not met within %v", timeout)
}

// objectNames extracts metadata.name from each result's projected object.
func objectNames(t *testing.T, results []v1alpha1.ObjectResult) []string {
	t.Helper()
	var names []string
	for _, r := range results {
		name := getProjectedString(t, r.Object, "metadata", "name")
		names = append(names, name)
	}
	return names
}

// objectKinds extracts kind from each result's projected object.
func objectKinds(t *testing.T, results []v1alpha1.ObjectResult) []string {
	t.Helper()
	var kinds []string
	for _, r := range results {
		kind := getProjectedString(t, r.Object, "kind")
		kinds = append(kinds, kind)
	}
	return kinds
}

// getProjectedString navigates a JSON path in a projected object.
func getProjectedString(t *testing.T, obj *runtime.RawExtension, path ...string) string {
	t.Helper()
	if obj == nil || len(obj.Raw) == 0 {
		return ""
	}
	var data map[string]any
	if err := json.Unmarshal(obj.Raw, &data); err != nil {
		t.Fatalf("unmarshal projected object: %v", err)
	}
	current := data
	for i, key := range path {
		if i == len(path)-1 {
			val, _ := current[key].(string)
			return val
		}
		next, ok := current[key].(map[string]any)
		if !ok {
			return ""
		}
		current = next
	}
	return ""
}

// getProjectedValue navigates a JSON path and returns the raw value.
func getProjectedValue(t *testing.T, obj *runtime.RawExtension, path ...string) any {
	t.Helper()
	if obj == nil || len(obj.Raw) == 0 {
		return nil
	}
	var data map[string]any
	if err := json.Unmarshal(obj.Raw, &data); err != nil {
		return nil
	}
	var current any = data
	for _, key := range path {
		m, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		current = m[key]
	}
	return current
}

// hasName checks if any result has the given name.
func hasName(t *testing.T, results []v1alpha1.ObjectResult, name string) bool {
	t.Helper()
	for _, n := range objectNames(t, results) {
		if n == name {
			return true
		}
	}
	return false
}

// projectionSpec builds a raw JSON projection spec.
func projectionSpec(spec map[string]any) *runtime.RawExtension {
	raw, _ := json.Marshal(spec)
	return &runtime.RawExtension{Raw: raw}
}

// containsString checks if a slice contains a string.
func containsString(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}
