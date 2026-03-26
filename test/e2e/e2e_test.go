//go:build e2e

package e2e_test

import (
	"context"
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

	"github.com/testcontainers/testcontainers-go/modules/k3s"
)

var (
	serverURL  string
	httpClient *http.Client
	tmpDir     string
	kueryCmd   *exec.Cmd
)

func TestMain(m *testing.M) {
	var code int
	defer func() { os.Exit(code) }()

	ctx := context.Background()
	httpClient = newInsecureClient()

	var err error
	tmpDir, err = os.MkdirTemp("", "kuery-e2e-")
	if err != nil {
		log.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 1. Start K3s clusters via testcontainers.
	log.Println("Starting K3s cluster A...")
	containerA, err := k3s.Run(ctx, "rancher/k3s:v1.31.6-k3s1")
	if err != nil {
		log.Fatalf("start K3s cluster-a: %v", err)
	}
	defer containerA.Terminate(ctx)

	log.Println("Starting K3s cluster B...")
	containerB, err := k3s.Run(ctx, "rancher/k3s:v1.31.6-k3s1")
	if err != nil {
		log.Fatalf("start K3s cluster-b: %v", err)
	}
	defer containerB.Terminate(ctx)

	// 2. Get kubeconfigs and write to files.
	kcA := filepath.Join(tmpDir, "alpha.kubeconfig")
	kcB := filepath.Join(tmpDir, "beta.kubeconfig")

	kubeConfigA, err := containerA.GetKubeConfig(ctx)
	if err != nil {
		log.Fatalf("get kubeconfig cluster-a: %v", err)
	}
	if err := os.WriteFile(kcA, kubeConfigA, 0600); err != nil {
		log.Fatalf("write kubeconfig cluster-a: %v", err)
	}

	kubeConfigB, err := containerB.GetKubeConfig(ctx)
	if err != nil {
		log.Fatalf("get kubeconfig cluster-b: %v", err)
	}
	if err := os.WriteFile(kcB, kubeConfigB, 0600); err != nil {
		log.Fatalf("write kubeconfig cluster-b: %v", err)
	}

	// 3. Apply fixtures via client-go.
	log.Println("Applying fixtures...")
	if err := applyYAMLFromKubeconfig(ctx, kubeConfigA, "demo", clusterAFixtures); err != nil {
		log.Fatalf("apply fixtures cluster-a: %v", err)
	}
	if err := applyYAMLFromKubeconfig(ctx, kubeConfigB, "demo", clusterBFixtures); err != nil {
		log.Fatalf("apply fixtures cluster-b: %v", err)
	}

	// 4. Wait for rollouts via client-go.
	log.Println("Waiting for rollouts...")
	if err := waitForDeploymentReady(ctx, kubeConfigA, "demo", "nginx", 120*time.Second); err != nil {
		log.Fatalf("rollout nginx: %v", err)
	}
	if err := waitForDeploymentReady(ctx, kubeConfigB, "demo", "redis", 120*time.Second); err != nil {
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

	// 9. Wait for data sync.
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
	dir, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "."
		}
		dir = parent
	}
}

func dumpLog(tmpDir string) {
	data, err := os.ReadFile(filepath.Join(tmpDir, "kuery.log"))
	if err != nil {
		return
	}
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
