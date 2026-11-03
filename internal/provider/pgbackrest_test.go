package provider

import (
	"context"
	"errors"
	"slices"
	"testing"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestCanExpireSet(t *testing.T) {
	t.Parallel()

	const (
		oldFull = "20261021-050935F"
		newFull = "20261021-074136F"
		incr    = "20261021-074136F_20261102-095700I"
		incr2   = "20261021-074136F_20261102-110000I"
	)
	full := func(label string) pgBackRestBackup { return pgBackRestBackup{Label: label, Type: "full"} }
	inc := func(label string, refs ...string) pgBackRestBackup {
		return pgBackRestBackup{Label: label, Type: "incr", Reference: refs}
	}

	tests := []struct {
		name    string
		backups []pgBackRestBackup
		label   string
		wantOK  bool
	}{
		{
			name:    "full with another full and no dependents",
			backups: []pgBackRestBackup{full(oldFull), full(newFull)},
			label:   oldFull,
			wantOK:  true,
		},
		{
			name:    "only full in the repo",
			backups: []pgBackRestBackup{full(newFull)},
			label:   newFull,
		},
		{
			name:    "full that an incremental depends on",
			backups: []pgBackRestBackup{full(oldFull), full(newFull), inc(incr, newFull)},
			label:   newFull,
		},
		{
			name:    "set not in the repo",
			backups: []pgBackRestBackup{full(newFull)},
			label:   oldFull,
		},
		{
			name:    "incremental with no dependents",
			backups: []pgBackRestBackup{full(newFull), inc(incr, newFull)},
			label:   incr,
			wantOK:  true,
		},
		{
			name:    "incremental that a later incremental depends on",
			backups: []pgBackRestBackup{full(newFull), inc(incr, newFull), inc(incr2, newFull, incr)},
			label:   incr,
		},
		{
			name:  "empty repo",
			label: newFull,
		},
		{
			name:    "full while an incremental depends on another full",
			backups: []pgBackRestBackup{full(oldFull), full(newFull), inc(incr, newFull)},
			label:   oldFull,
			wantOK:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ok, reason := canExpireSet(tt.backups, tt.label)
			assert.Equal(t, tt.wantOK, ok)
			if tt.wantOK {
				assert.Empty(t, reason)
			} else {
				assert.NotEmpty(t, reason)
			}
		})
	}
}

// fakePodExecutor answers "pgbackrest info" with infoJSON and records every
// command it receives.
type fakePodExecutor struct {
	infoJSON  string
	expireErr error
	commands  [][]string
}

func (f *fakePodExecutor) Exec(_ context.Context, _, _, _ string, command []string) ([]byte, []byte, error) {
	f.commands = append(f.commands, command)
	switch {
	case slices.Contains(command, "info"):
		return []byte(f.infoJSON), nil, nil
	case slices.Contains(command, "expire"):
		return nil, []byte("expire failed"), f.expireErr
	}
	return nil, nil, errors.New("unexpected command")
}

func (f *fakePodExecutor) expireCommands() [][]string {
	var out [][]string
	for _, c := range f.commands {
		if slices.Contains(c, "expire") {
			out = append(out, c)
		}
	}
	return out
}

func TestCleanupBackupPurge(t *testing.T) {
	t.Parallel()

	const (
		ns      = "everest"
		cluster = "pg-prod"
		set     = "20261021-050935F"
		// Two full backups, so the older one can be expired.
		twoFulls = `[{"name":"db","backup":[
			{"label":"20261021-050935F","type":"full","reference":null},
			{"label":"20261021-074136F","type":"full","reference":null}]}]`
	)

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, pgv2.AddToScheme(scheme))
	require.NoError(t, backupv1alpha1.AddToScheme(scheme))

	repoHost := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster + "-repo-host-0",
			Namespace: ns,
			Labels: map[string]string{
				labelCluster:             cluster,
				labelPGBackRestDedicated: "",
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	mkBackup := func(policy backupv1alpha1.BackupDeletionPolicy) *backupv1alpha1.Backup {
		return &backupv1alpha1.Backup{
			ObjectMeta: metav1.ObjectMeta{Name: "b1", Namespace: ns},
			Spec:       backupv1alpha1.BackupSpec{DeletionPolicy: policy},
		}
	}
	mkOpBackup := func(state pgv2.PGBackupState) *pgv2.PerconaPGBackup {
		return &pgv2.PerconaPGBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "b1", Namespace: ns},
			Spec:       pgv2.PerconaPGBackupSpec{PGCluster: cluster, RepoName: ptr.To("repo1")},
			Status:     pgv2.PerconaPGBackupStatus{State: state, BackupName: set},
		}
	}
	newCtx := func(objs ...client.Object) (*controller.Context, client.Client) {
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
		inst := &corev1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: cluster, Namespace: ns}}
		return controller.NewContext(context.Background(), k8sClient, inst, "provider-percona-postgresql"), k8sClient
	}
	opBackupGone := func(t *testing.T, k8sClient client.Client) bool {
		t.Helper()
		err := k8sClient.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "b1"}, &pgv2.PerconaPGBackup{})
		return apierrors.IsNotFound(err)
	}

	t.Run("Delete expires the set before deleting the operator backup", func(t *testing.T) {
		t.Parallel()
		exec := &fakePodExecutor{infoJSON: twoFulls}
		p := &Provider{exec: exec}
		c, k8sClient := newCtx(repoHost.DeepCopy(), mkOpBackup(pgv2.BackupSucceeded))

		done, err := p.CleanupBackup(c, mkBackup(backupv1alpha1.BackupDeletionPolicyDelete))
		require.NoError(t, err)
		assert.False(t, done)
		assert.Equal(t, [][]string{
			{"pgbackrest", "--stanza=db", "--repo=1", "expire", "--set=" + set},
		}, exec.expireCommands())
		assert.True(t, opBackupGone(t, k8sClient))
	})

	t.Run("Retain never runs pgbackrest", func(t *testing.T) {
		t.Parallel()
		exec := &fakePodExecutor{infoJSON: twoFulls}
		p := &Provider{exec: exec}
		c, k8sClient := newCtx(repoHost.DeepCopy(), mkOpBackup(pgv2.BackupSucceeded))

		_, err := p.CleanupBackup(c, mkBackup(backupv1alpha1.BackupDeletionPolicyRetain))
		require.NoError(t, err)
		assert.Empty(t, exec.commands)
		assert.True(t, opBackupGone(t, k8sClient))
	})

	t.Run("expire failure keeps the operator backup and retries", func(t *testing.T) {
		t.Parallel()
		exec := &fakePodExecutor{infoJSON: twoFulls, expireErr: errors.New("exit code 1")}
		p := &Provider{exec: exec}
		c, k8sClient := newCtx(repoHost.DeepCopy(), mkOpBackup(pgv2.BackupSucceeded))

		done, err := p.CleanupBackup(c, mkBackup(backupv1alpha1.BackupDeletionPolicyDelete))
		require.Error(t, err)
		assert.False(t, done)
		assert.False(t, opBackupGone(t, k8sClient))
	})

	t.Run("only full in the repo is kept", func(t *testing.T) {
		t.Parallel()
		exec := &fakePodExecutor{infoJSON: `[{"name":"db","backup":[
			{"label":"20261021-050935F","type":"full","reference":null}]}]`}
		p := &Provider{exec: exec}
		c, k8sClient := newCtx(repoHost.DeepCopy(), mkOpBackup(pgv2.BackupSucceeded))

		_, err := p.CleanupBackup(c, mkBackup(backupv1alpha1.BackupDeletionPolicyDelete))
		require.NoError(t, err)
		assert.Empty(t, exec.expireCommands())
		assert.True(t, opBackupGone(t, k8sClient))
	})

	t.Run("no repo host pod keeps the data", func(t *testing.T) {
		t.Parallel()
		exec := &fakePodExecutor{infoJSON: twoFulls}
		p := &Provider{exec: exec}
		c, k8sClient := newCtx(mkOpBackup(pgv2.BackupSucceeded))

		_, err := p.CleanupBackup(c, mkBackup(backupv1alpha1.BackupDeletionPolicyDelete))
		require.NoError(t, err)
		assert.Empty(t, exec.commands)
		assert.True(t, opBackupGone(t, k8sClient))
	})

	t.Run("backup that did not succeed keeps the data", func(t *testing.T) {
		t.Parallel()
		exec := &fakePodExecutor{infoJSON: twoFulls}
		p := &Provider{exec: exec}
		c, k8sClient := newCtx(repoHost.DeepCopy(), mkOpBackup(pgv2.BackupFailed))

		_, err := p.CleanupBackup(c, mkBackup(backupv1alpha1.BackupDeletionPolicyDelete))
		require.NoError(t, err)
		assert.Empty(t, exec.commands)
		assert.True(t, opBackupGone(t, k8sClient))
	})
}
