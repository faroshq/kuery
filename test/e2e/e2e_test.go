//go:build e2e

package e2e_test

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var (
	serverURL  string
	httpClient *http.Client
	tmpDir     string
	kueryCmd   *exec.Cmd
)

const (
	clusterA = "kuery-e2e-alpha"
	clusterB = "kuery-e2e-beta"
)

func TestMain(m *testing.M) {
	var code int
	defer func() { os.Exit(code) }()

	httpClient = newInsecureClient()

	var err error
	tmpDir, err = os.MkdirTemp("", "kuery-e2e-")
	if err != nil {
		log.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 1. Create kind clusters.
	log.Println("Creating kind clusters...")
	if err := createKindCluster(clusterA); err != nil {
		log.Fatalf("create cluster %s: %v", clusterA, err)
	}
	defer deleteKindCluster(clusterA)

	if err := createKindCluster(clusterB); err != nil {
		log.Fatalf("create cluster %s: %v", clusterB, err)
	}
	defer deleteKindCluster(clusterB)

	// 2. Export kubeconfigs.
	kcA := filepath.Join(tmpDir, "alpha.kubeconfig")
	kcB := filepath.Join(tmpDir, "beta.kubeconfig")
	if err := exportKubeconfig(clusterA, kcA); err != nil {
		log.Fatalf("export kubeconfig %s: %v", clusterA, err)
	}
	if err := exportKubeconfig(clusterB, kcB); err != nil {
		log.Fatalf("export kubeconfig %s: %v", clusterB, err)
	}

	// 3. Apply fixtures.
	log.Println("Applying fixtures...")
	if err := applyFixtures(kcA, clusterAFixtures); err != nil {
		log.Fatalf("apply fixtures cluster-a: %v", err)
	}
	if err := applyFixtures(kcB, clusterBFixtures); err != nil {
		log.Fatalf("apply fixtures cluster-b: %v", err)
	}

	// 4. Wait for rollouts.
	log.Println("Waiting for rollouts...")
	if err := waitForRollout(kcA, "demo", "nginx"); err != nil {
		log.Fatalf("rollout nginx: %v", err)
	}
	if err := waitForRollout(kcB, "demo", "redis"); err != nil {
		log.Fatalf("rollout redis: %v", err)
	}

	// 5. Build kuery binary.
	log.Println("Building kuery...")
	kueryBin := filepath.Join(tmpDir, "kuery")
	buildCmd := exec.Command("go", "build", "-o", kueryBin, "./cmd/kuery")
	buildCmd.Dir = findRepoRoot()
	buildCmd.Stdout = os.Stdout
	buildCmd.Stderr = os.Stderr
	if err := buildCmd.Run(); err != nil {
		log.Fatalf("build kuery: %v", err)
	}

	// 6. Find a free port.
	port, err := getFreePort()
	if err != nil {
		log.Fatalf("find free port: %v", err)
	}
	serverURL = fmt.Sprintf("https://localhost:%d", port)

	// 7. Start kuery server.
	log.Printf("Starting kuery on port %d...\n", port)
	logFile, err := os.Create(filepath.Join(tmpDir, "kuery.log"))
	if err != nil {
		log.Fatalf("create log file: %v", err)
	}
	defer logFile.Close()

	kueryCmd = exec.Command(kueryBin,
		"--store-driver=sqlite",
		"--store-dsn="+filepath.Join(tmpDir, "kuery.db"),
		fmt.Sprintf("--secure-port=%d", port),
		fmt.Sprintf("--kubeconfigs=cluster-a=%s,cluster-b=%s", kcA, kcB),
		"--sync-blacklist=events,events.events.k8s.io",
	)
	kueryCmd.Stdout = logFile
	kueryCmd.Stderr = logFile
	if err := kueryCmd.Start(); err != nil {
		log.Fatalf("start kuery: %v", err)
	}
	defer func() {
		if kueryCmd.Process != nil {
			kueryCmd.Process.Kill()
			kueryCmd.Wait()
		}
	}()

	// 8. Wait for API readiness.
	log.Println("Waiting for kuery API...")
	err = waitForCondition(60*time.Second, time.Second, func() bool {
		resp, err := httpClient.Get(serverURL + "/apis/kuery.io/v1alpha1")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return strings.Contains(string(body), "kuery.io")
	})
	if err != nil {
		dumpLog(tmpDir)
		log.Fatalf("kuery API not ready: %v", err)
	}

	// 9. Wait for data sync (poll for Deployments from both clusters).
	log.Println("Waiting for data sync...")
	syncClient := &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
	}
	err = waitForCondition(90*time.Second, 2*time.Second, func() bool {
		body := `{"apiVersion":"kuery.io/v1alpha1","kind":"Query","spec":{"filter":{"objects":[{"groupKind":{"apiGroup":"apps","kind":"Deployment"},"namespace":"demo"}]},"count":true}}`
		resp, err := syncClient.Post(serverURL+"/apis/kuery.io/v1alpha1/queries", "application/json", strings.NewReader(body))
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		respBody, _ := io.ReadAll(resp.Body)
		// Check that count >= 2 (nginx from cluster-a + redis from cluster-b).
		return strings.Contains(string(respBody), `"count":`) &&
			!strings.Contains(string(respBody), `"count":0`) &&
			!strings.Contains(string(respBody), `"count":1`)
	})
	if err != nil {
		dumpLog(tmpDir)
		log.Fatalf("data sync not ready: %v", err)
	}

	log.Println("Setup complete, running tests...")
	code = m.Run()
}

func createKindCluster(name string) error {
	cmd := exec.Command("kind", "create", "cluster", "--name", name, "--wait", "60s")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func deleteKindCluster(name string) {
	exec.Command("kind", "delete", "cluster", "--name", name).Run()
}

func exportKubeconfig(clusterName, path string) error {
	cmd := exec.Command("kind", "get", "kubeconfig", "--name", clusterName)
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("kind get kubeconfig: %w", err)
	}
	return os.WriteFile(path, out, 0600)
}

func applyFixtures(kubeconfig, yaml string) error {
	// Create namespace first.
	nsCmd := exec.Command("kubectl", "--kubeconfig="+kubeconfig, "create", "namespace", "demo")
	nsCmd.Run() // Ignore error if already exists.

	cmd := exec.Command("kubectl", "--kubeconfig="+kubeconfig, "-n", "demo", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(yaml)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func waitForRollout(kubeconfig, namespace, deployment string) error {
	cmd := exec.Command("kubectl", "--kubeconfig="+kubeconfig, "-n", namespace,
		"rollout", "status", "deployment/"+deployment, "--timeout=120s")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func getFreePort() (int, error) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port, nil
}

func findRepoRoot() string {
	// Walk up from the test file to find go.mod.
	dir, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "." // Fallback.
		}
		dir = parent
	}
}

func dumpLog(tmpDir string) {
	data, err := os.ReadFile(filepath.Join(tmpDir, "kuery.log"))
	if err != nil {
		return
	}
	// Print last 50 lines.
	lines := strings.Split(string(data), "\n")
	start := 0
	if len(lines) > 50 {
		start = len(lines) - 50
	}
	log.Println("=== kuery server log (last 50 lines) ===")
	for _, line := range lines[start:] {
		log.Println(line)
	}
}

func init() {
	// Ensure httpClient is non-nil even if TestMain hasn't run yet (for compilation).
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout:   30 * time.Second,
			Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
		}
	}
}

// Ensure tmpDir has a bytes import for fixtures application
var _ = bytes.NewReader
