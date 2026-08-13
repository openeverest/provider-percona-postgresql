package provider

import (
	"testing"
	"time"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var pitrBase = time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)

// mkOpBackup builds a Succeeded operator backup on repo1 completed at
// base+offset. restorable sets status.latestRestorableTime to one hour past
// completion.
func mkOpBackup(name string, offset time.Duration, restorable bool) pgv2.PerconaPGBackup {
	completed := metav1.NewTime(pitrBase.Add(offset))
	repo := "repo1"
	b := pgv2.PerconaPGBackup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "everest"},
		Spec: pgv2.PerconaPGBackupSpec{
			PGCluster: "pg-prod",
			RepoName:  &repo,
		},
		Status: pgv2.PerconaPGBackupStatus{
			State:       pgv2.BackupSucceeded,
			CompletedAt: &completed,
		},
	}
	if restorable {
		t := metav1.NewTime(pitrBase.Add(offset + time.Hour))
		b.Status.LatestRestorableTime = pgv2.PITRestoreDateTime{Time: &t}
	}
	return b
}

func TestPITRWindow_NoBackups(t *testing.T) {
	t.Parallel()

	got := pitrWindow(nil)
	require.NotNil(t, got)
	assert.Equal(t, corev1alpha1.PITRStateUnavailable, got.State)
	assert.Equal(t, pitrReasonNoBackups, got.Reason)
	assert.Nil(t, got.EarliestRestorableTime)
}

func TestPITRWindow_SpansAllBackups(t *testing.T) {
	t.Parallel()

	got := pitrWindow([]pgv2.PerconaPGBackup{
		mkOpBackup("b1", 0, true),
		mkOpBackup("b2", time.Hour, true),
		mkOpBackup("b3", 2*time.Hour, true),
	})

	require.NotNil(t, got)
	assert.Equal(t, corev1alpha1.PITRStateAvailable, got.State)
	// Earliest is the oldest backup's completion; latest comes from the newest.
	assert.Equal(t, pitrBase, got.EarliestRestorableTime.Time)
	assert.Equal(t, pitrBase.Add(3*time.Hour), got.LatestRestorableTime.Time)
}

// The operator refreshes latestRestorableTime on backups of the repo, so in
// the interval before a brand-new backup has been stamped the maximum across
// all backups is the correct end of the window.
func TestPITRWindow_NewestBackupNotYetStamped(t *testing.T) {
	t.Parallel()

	got := pitrWindow([]pgv2.PerconaPGBackup{
		mkOpBackup("b1", 0, true),
		mkOpBackup("b2", time.Hour, false),
	})

	require.NotNil(t, got)
	assert.Equal(t, corev1alpha1.PITRStateAvailable, got.State)
	assert.Equal(t, pitrBase.Add(time.Hour), got.LatestRestorableTime.Time)
}

func TestPITRWindow_NoRestorableTimeYet(t *testing.T) {
	t.Parallel()

	got := pitrWindow([]pgv2.PerconaPGBackup{
		mkOpBackup("b1", 0, false),
	})

	require.NotNil(t, got)
	assert.Equal(t, corev1alpha1.PITRStateUnavailable, got.State)
	assert.Equal(t, pitrReasonNoBackups, got.Reason)
	assert.Nil(t, got.LatestRestorableTime)
}

func TestCollectRepoBackups_FiltersAndSorts(t *testing.T) {
	t.Parallel()

	other := mkOpBackup("other-cluster", 0, true)
	other.Spec.PGCluster = "pg-staging"

	otherRepo := "repo2"
	otherStorage := mkOpBackup("other-repo", 0, true)
	otherStorage.Spec.RepoName = &otherRepo

	running := mkOpBackup("running", 0, true)
	running.Status.State = pgv2.BackupRunning

	noRepo := mkOpBackup("no-repo", 0, true)
	noRepo.Spec.RepoName = nil

	got := collectRepoBackups([]pgv2.PerconaPGBackup{
		mkOpBackup("newer", time.Hour, true),
		other,
		otherStorage,
		running,
		noRepo,
		mkOpBackup("older", 0, true),
	}, "pg-prod", "repo1")

	require.Len(t, got, 2)
	assert.Equal(t, "older", got[0].Name, "results must be oldest first")
	assert.Equal(t, "newer", got[1].Name)
}

func TestCollectRepoBackups_EmptyRepoNameMatchesNothing(t *testing.T) {
	t.Parallel()

	got := collectRepoBackups([]pgv2.PerconaPGBackup{
		mkOpBackup("b1", 0, true),
	}, "pg-prod", "")

	assert.Empty(t, got)
}
