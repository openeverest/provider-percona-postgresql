package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/streaming/pkg/httpstream"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// pgBackRestContainer is the container in the dedicated repo host pod
	// that runs pgBackRest.
	pgBackRestContainer = "pgbackrest"

	labelPGBackRestDedicated = "postgres-operator.crunchydata.com/pgbackrest-dedicated"

	// pgBackRestTimeout limits each pgBackRest command, so a hung command
	// fails and is retried instead of blocking the reconcile.
	pgBackRestTimeout = 5 * time.Minute
)

// podExecutor runs a command in a container and returns its output.
// It is an interface so tests can replace the Kubernetes exec API.
type podExecutor interface {
	Exec(ctx context.Context, namespace, pod, container string, command []string) (stdout, stderr []byte, err error)
}

// restPodExecutor runs commands through the pods/exec subresource, like
// kubectl exec. The REST config and clientset are created on first use.
type restPodExecutor struct {
	once      sync.Once
	config    *rest.Config
	clientset kubernetes.Interface
	initErr   error
}

func (e *restPodExecutor) init() error {
	e.once.Do(func() {
		e.config, e.initErr = ctrl.GetConfig()
		if e.initErr != nil {
			return
		}
		e.clientset, e.initErr = kubernetes.NewForConfig(e.config)
	})
	return e.initErr
}

func (e *restPodExecutor) Exec(ctx context.Context, namespace, pod, container string, command []string) ([]byte, []byte, error) {
	if err := e.init(); err != nil {
		return nil, nil, fmt.Errorf("create exec client: %w", err)
	}

	req := e.clientset.CoreV1().RESTClient().Post().
		Resource("pods").Namespace(namespace).Name(pod).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   command,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	// Prefer WebSocket and fall back to SPDY, as kubectl does.
	wsExec, err := remotecommand.NewWebSocketExecutor(e.config, "GET", req.URL().String())
	if err != nil {
		return nil, nil, fmt.Errorf("create websocket executor: %w", err)
	}
	spdyExec, err := remotecommand.NewSPDYExecutor(e.config, "POST", req.URL())
	if err != nil {
		return nil, nil, fmt.Errorf("create spdy executor: %w", err)
	}
	exec, err := remotecommand.NewFallbackExecutor(wsExec, spdyExec, httpstream.IsUpgradeFailure)
	if err != nil {
		return nil, nil, fmt.Errorf("create executor: %w", err)
	}

	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr})
	return stdout.Bytes(), stderr.Bytes(), err
}

// pgBackRestBackup is one entry of "pgbackrest info --output=json".
type pgBackRestBackup struct {
	Label string `json:"label"`
	Type  string `json:"type"`
	// Reference lists the backups this backup depends on. It is empty for
	// a full backup.
	Reference []string `json:"reference"`
}

type pgBackRestStanzaInfo struct {
	Name   string             `json:"name"`
	Backup []pgBackRestBackup `json:"backup"`
}

// repoHostPod returns the running dedicated pgBackRest repo host pod of the
// cluster, or nil when there is none (for example, the cluster was deleted).
func repoHostPod(c *controller.Context, namespace, cluster string) (*corev1.Pod, error) {
	pods := &corev1.PodList{}
	if err := c.Client().List(c.Context(), pods,
		client.InNamespace(namespace),
		client.MatchingLabels{labelCluster: cluster},
		client.HasLabels{labelPGBackRestDedicated},
	); err != nil {
		return nil, fmt.Errorf("list repo host pods of cluster %q: %w", cluster, err)
	}
	for i := range pods.Items {
		if pods.Items[i].Status.Phase == corev1.PodRunning {
			return &pods.Items[i], nil
		}
	}
	return nil, nil
}

// purgeBackupData removes the operator backup's set from its pgBackRest
// repo. It keeps the data, and returns nil, when the set cannot be removed
// safely or the repo cannot be reached because the cluster is gone, so a
// Backup is never stuck in deletion. It returns an error only for failures
// worth retrying.
func (p *Provider) purgeBackupData(c *controller.Context, opBackup *pgv2.PerconaPGBackup) error {
	l := log.FromContext(c.Context()).WithValues("perconaPGBackup", opBackup.Name)

	label := opBackup.Status.BackupName
	if opBackup.Status.State != pgv2.BackupSucceeded || label == "" || opBackup.Spec.RepoName == nil {
		l.Info("Keeping backup data: backup has no completed set", "state", opBackup.Status.State)
		return nil
	}
	repo := strings.TrimPrefix(*opBackup.Spec.RepoName, "repo")

	pod, err := repoHostPod(c, opBackup.Namespace, opBackup.Spec.PGCluster)
	if err != nil {
		return err
	}
	if pod == nil {
		l.Info("Keeping backup data: no running pgBackRest repo host", "cluster", opBackup.Spec.PGCluster)
		return nil
	}

	run := func(args ...string) ([]byte, error) {
		cmd := append([]string{"pgbackrest", "--stanza=" + pgBackRestStanza, "--repo=" + repo}, args...)
		ctx, cancel := context.WithTimeout(c.Context(), pgBackRestTimeout)
		defer cancel()
		stdout, stderr, err := p.exec.Exec(ctx, pod.Namespace, pod.Name, pgBackRestContainer, cmd)
		if err != nil {
			return nil, fmt.Errorf("%s: %w: %s", strings.Join(cmd, " "), err, strings.TrimSpace(string(stderr)))
		}
		return stdout, nil
	}

	out, err := run("info", "--output=json")
	if err != nil {
		return err
	}
	var stanzas []pgBackRestStanzaInfo
	if err := json.Unmarshal(out, &stanzas); err != nil {
		return fmt.Errorf("parse pgbackrest info: %w", err)
	}
	var backups []pgBackRestBackup
	for _, s := range stanzas {
		if s.Name == pgBackRestStanza {
			backups = s.Backup
		}
	}

	if ok, reason := canExpireSet(backups, label); !ok {
		l.Info("Keeping backup data", "set", label, "repo", repo, "reason", reason)
		return nil
	}

	if _, err := run("expire", "--set="+label); err != nil {
		return err
	}
	l.Info("Removed backup data", "set", label, "repo", repo)
	return nil
}

// canExpireSet reports whether "pgbackrest expire --set=label" removes only
// that set. When it returns false, reason says why the data must be kept.
func canExpireSet(backups []pgBackRestBackup, label string) (ok bool, reason string) {
	found := false
	fulls := 0
	dependent := false
	ourType := ""

	for _, b := range backups {
		if b.Label == label {
			found = true
			ourType = b.Type
		}
		if b.Type == "full" {
			fulls++
		}
		if slices.Contains(b.Reference, label) {
			dependent = true
		}
	}

	if !found {
		return false, "set is not in the repo"
	}
	if dependent {
		return false, "other backups depend on this set"
	}
	if ourType == "full" && fulls == 1 {
		return false, "set is the only full backup in the repo"
	}
	return true, ""
}
