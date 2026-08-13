package provider

import (
	"context"
	"fmt"
	"sort"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// Reasons published on InstanceBackupStoragePITRStatus.
const (
	pitrReasonWindowAvailable = "WindowAvailable"
	pitrReasonNoBackups       = "NoRestorableBackup"
)

var _ controller.InstanceBackupStatusReporter = (*Provider)(nil)

// BackupStorageStatuses publishes the point-in-time recovery window observed on
// each PITR-enabled storage of the Instance. Each storage maps to its own
// pgBackRest repo, and pgBackRest archives WAL to every repo, so every storage
// carries an independent stream and window.
func (p *Provider) BackupStorageStatuses(c *controller.Context) ([]corev1alpha1.InstanceBackupStorageStatus, error) {
	backupCfg := c.Instance().Spec.Backup
	if backupCfg == nil || !backupCfg.Enabled {
		return nil, nil
	}

	// The live cluster carries the stable storage->repo slot map.
	pgCluster := &pgv2.PerconaPGCluster{}
	if err := c.Get(pgCluster, c.Name()); err != nil {
		if apierrors.IsNotFound(err) {
			pgCluster = nil
		} else {
			return nil, fmt.Errorf("get PerconaPGCluster %q: %w", c.Name(), err)
		}
	}

	list := &pgv2.PerconaPGBackupList{}
	if err := c.List(list); err != nil {
		return nil, fmt.Errorf("list PerconaPGBackups: %w", err)
	}

	out := make([]corev1alpha1.InstanceBackupStorageStatus, 0, len(backupCfg.Storages))
	for _, s := range backupCfg.Storages {
		entry := corev1alpha1.InstanceBackupStorageStatus{Name: s.StorageRef.Name}
		if s.PITR != nil && s.PITR.Enabled {
			repoName, _ := storageNameToRepoName(c, s.StorageRef.Name, pgCluster)
			entry.PITR = pitrWindow(collectRepoBackups(list.Items, c.Name(), repoName))
		}
		out = append(out, entry)
	}
	return out, nil
}

// enqueueOperatorBackupInstance maps a PerconaPGBackup event to a reconcile
// request for the Instance named by spec.pgCluster, so that status the operator
// stamps on its backups reaches instance.status.backup.storages.
func enqueueOperatorBackupInstance() func(ctx context.Context, obj client.Object) []reconcile.Request {
	return func(_ context.Context, obj client.Object) []reconcile.Request {
		b, ok := obj.(*pgv2.PerconaPGBackup)
		if !ok || b.Spec.PGCluster == "" {
			return nil
		}
		return []reconcile.Request{{
			NamespacedName: types.NamespacedName{
				Namespace: b.Namespace,
				Name:      b.Spec.PGCluster,
			},
		}}
	}
}

// collectRepoBackups returns the Succeeded backups of one cluster on one
// pgBackRest repo, oldest first. An empty repoName matches nothing.
func collectRepoBackups(all []pgv2.PerconaPGBackup, cluster, repoName string) []pgv2.PerconaPGBackup {
	if repoName == "" {
		return nil
	}
	var out []pgv2.PerconaPGBackup
	for i := range all {
		b := all[i]
		if b.Spec.PGCluster != cluster ||
			b.Spec.RepoName == nil || *b.Spec.RepoName != repoName ||
			b.Status.State != pgv2.BackupSucceeded ||
			b.Status.CompletedAt == nil {
			continue
		}
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Status.CompletedAt.Before(out[j].Status.CompletedAt)
	})
	return out
}

// pitrWindow derives the recovery window from the operator's backups.
//
// pgBackRest tracks a WAL archive per repo but the operator exposes no window,
// only its end: status.latestRestorableTime, refreshed on backups of the repo.
// No gap signal is exposed on the CRD surface either, so the window is derived
// as:
//
//	earliest = completedAt of the oldest Succeeded backup on the repo
//	latest   = newest latestRestorableTime published on the repo
//
// The latest is taken as the maximum across backups rather than off the newest
// backup only, so the window stays correct in the interval before a brand-new
// backup has been stamped.
func pitrWindow(backups []pgv2.PerconaPGBackup) *corev1alpha1.InstanceBackupStoragePITRStatus {
	if len(backups) == 0 {
		return &corev1alpha1.InstanceBackupStoragePITRStatus{
			State:   corev1alpha1.PITRStateUnavailable,
			Reason:  pitrReasonNoBackups,
			Message: "No Succeeded backup on this storage yet",
		}
	}

	var latest *metav1.Time
	for i := range backups {
		if t := backups[i].Status.LatestRestorableTime.Time; t != nil && (latest == nil || t.After(latest.Time)) {
			latest = t
		}
	}

	// The operator has not published an end for the WAL stream yet, so nothing
	// is restorable by time even though a base exists.
	if latest == nil {
		return &corev1alpha1.InstanceBackupStoragePITRStatus{
			State:   corev1alpha1.PITRStateUnavailable,
			Reason:  pitrReasonNoBackups,
			Message: "Waiting for the operator to report a restorable time",
		}
	}

	return &corev1alpha1.InstanceBackupStoragePITRStatus{
		EarliestRestorableTime: backups[0].Status.CompletedAt,
		LatestRestorableTime:   latest,
		State:                  corev1alpha1.PITRStateAvailable,
		Reason:                 pitrReasonWindowAvailable,
	}
}
