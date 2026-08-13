package provider

import (
	"context"
	"encoding/json"
	"fmt"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	apicommon "github.com/openeverest/openeverest/v2/api/common/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	instanceNameLabelKey = "instanceName"

	// finalizerDeleteBackupData is the finalizer the Percona PG operator
	// sets on every PerconaPGBackup. When present during deletion, the
	// operator purges the backup data from storage. We remove it when the
	// OpenEverest Backup's DeletionPolicy is Retain so data is preserved.
	finalizerDeleteBackupData = "internal.percona.com/delete-backup"
)

// Compile-time interface checks.
var _ controller.BackupProvider = (*Provider)(nil)
var _ controller.BackupWatcher = (*Provider)(nil)
var _ controller.RestoreWatcher = (*Provider)(nil)

// SyncBackup creates or updates the operator's backup resource, sets a controller
// reference from the Backup CR to enable owner-based watches, and maps operator
// status to OpenEverest states.
func (p *Provider) SyncBackup(c *controller.Context, backup *backupv1alpha1.Backup) (controller.BackupExecutionStatus, error) {
	l := log.FromContext(c.Context())
	l.Info("Syncing backup", "name", backup.Name)

	if backup.Labels == nil {
		backup.Labels = map[string]string{}
	}
	if backup.Labels[instanceNameLabelKey] != backup.Spec.InstanceRef.Name {
		origBackupCR := backup.DeepCopy()
		backup.Labels[instanceNameLabelKey] = backup.Spec.InstanceRef.Name
		if err := c.Client().Patch(c.Context(), backup, client.MergeFrom(origBackupCR)); err != nil {
			return controller.BackupExecutionStatus{}, fmt.Errorf("patch Backup %q labels: %w", backup.Name, err)
		}
		// Re-fetch the backup to ensure we have the latest resource version
		// before using it as a controller reference owner.
		if err := c.Client().Get(c.Context(), client.ObjectKeyFromObject(backup), backup); err != nil {
			return controller.BackupExecutionStatus{}, fmt.Errorf("re-fetch Backup %q after label patch: %w", backup.Name, err)
		}
	}

	opRef := &apicommon.TypedObjectRef{
		Group: pgv2.GroupVersion.Group,
		Kind:  "PerconaPGBackup",
		Name:  backup.Name,
	}
	managedByRuntime := backup.Spec.ScheduleName == ""
	ensureBackupControllerReference := func(opBackup *pgv2.PerconaPGBackup) error {
		if err := controllerutil.SetControllerReference(backup, opBackup, c.Client().Scheme()); err != nil {
			return fmt.Errorf("set backup controller reference: %w", err)
		}
		return nil
	}

	opBackup := &pgv2.PerconaPGBackup{}
	err := c.Client().Get(c.Context(), client.ObjectKey{Namespace: backup.Namespace, Name: backup.Name}, opBackup)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return controller.BackupExecutionStatus{}, fmt.Errorf("get PerconaPGBackup %q: %w", backup.Name, err)
		}

		if !managedByRuntime {
			return controller.BackupExecutionStatus{
				State:             backupv1alpha1.BackupStatePending,
				Message:           "Waiting for operator scheduled backup",
				OperatorBackupRef: opRef,
			}, nil
		}

		pgCluster := &pgv2.PerconaPGCluster{}
		if err := c.Client().Get(c.Context(), client.ObjectKey{Namespace: backup.Namespace, Name: backup.Spec.InstanceRef.Name}, pgCluster); err != nil {
			if apierrors.IsNotFound(err) {
				return controller.BackupExecutionStatus{
					State:             backupv1alpha1.BackupStateFailed,
					Message:           fmt.Sprintf("PerconaPGCluster %q not found", backup.Spec.InstanceRef.Name),
					OperatorBackupRef: opRef,
				}, nil
			}
			return controller.BackupExecutionStatus{}, fmt.Errorf("get PerconaPGCluster %q: %w", backup.Spec.InstanceRef.Name, err)
		}

		// Ensure the storage referenced by this backup is registered on the Instance.
		// This must happen before checking if backups are enabled, because when all
		// storages were pruned the provider disables backups — auto-registering the
		// storage will trigger the next Instance Sync to re-enable them.
		repoName, found := storageNameToRepoName(c, backup.Spec.StorageRef.Name, pgCluster)
		if !found {
			if registered, err := autoRegisterStorage(c, backup.Spec.StorageRef.Name); err != nil {
				return controller.BackupExecutionStatus{
					State:             backupv1alpha1.BackupStateFailed,
					Message:           fmt.Sprintf("Failed to auto-register storage %q: %v", backup.Spec.StorageRef.Name, err),
					OperatorBackupRef: opRef,
				}, nil
			} else if !registered {
				return controller.BackupExecutionStatus{
					State:             backupv1alpha1.BackupStatePending,
					Message:           fmt.Sprintf("Waiting for storage %q to be configured on the instance", backup.Spec.StorageRef.Name),
					OperatorBackupRef: opRef,
				}, nil
			}
			// Storage was registered — requeue to let the next Sync configure the repo.
			return controller.BackupExecutionStatus{
				State:             backupv1alpha1.BackupStatePending,
				Message:           fmt.Sprintf("Storage %q registered on the instance, waiting for repo configuration", backup.Spec.StorageRef.Name),
				OperatorBackupRef: opRef,
			}, nil
		}

		if !pgCluster.Spec.Backups.IsEnabled() || len(pgCluster.Spec.Backups.PGBackRest.Repos) == 0 {
			return controller.BackupExecutionStatus{
				State:             backupv1alpha1.BackupStatePending,
				Message:           "Waiting for backup repos to be configured on the cluster",
				OperatorBackupRef: opRef,
			}, nil
		}

		if !hasRepo(pgCluster, repoName) {
			return controller.BackupExecutionStatus{
				State:             backupv1alpha1.BackupStatePending,
				Message:           fmt.Sprintf("Waiting for repo %q to be configured on the cluster", repoName),
				OperatorBackupRef: opRef,
			}, nil
		}

		// Wait for the cluster to finish initializing (e.g. stanza-create)
		// before creating the backup. When a new storage is added the operator
		// needs to initialize the pgBackRest stanza which briefly puts the
		// cluster into the "initializing" state. Creating a backup before this
		// completes almost always results in a failure.
		if pgCluster.Status.State == pgv2.AppStateInit {
			return controller.BackupExecutionStatus{
				State:             backupv1alpha1.BackupStatePending,
				Message:           "Waiting for the cluster to finish initializing",
				OperatorBackupRef: opRef,
			}, nil
		}

		if !isStanzaCreated(pgCluster, repoName) {
			return controller.BackupExecutionStatus{
				State:             backupv1alpha1.BackupStatePending,
				Message:           fmt.Sprintf("Waiting for stanza to be created for repo %q", repoName),
				OperatorBackupRef: opRef,
			}, nil
		}

		backupType := resolveBackupType(backup)

		opBackup = &pgv2.PerconaPGBackup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      backup.Name,
				Namespace: backup.Namespace,
			},
			Spec: pgv2.PerconaPGBackupSpec{
				PGCluster: backup.Spec.InstanceRef.Name,
				RepoName:  &repoName,
				Options:   []string{fmt.Sprintf("--type=%s", backupType)},
			},
		}
		if err := ensureBackupControllerReference(opBackup); err != nil {
			return controller.BackupExecutionStatus{}, err
		}
		if err := c.Client().Create(c.Context(), opBackup); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return controller.BackupExecutionStatus{}, fmt.Errorf("create PerconaPGBackup %q: %w", backup.Name, err)
			}
			if err := c.Client().Get(c.Context(), client.ObjectKey{Namespace: backup.Namespace, Name: backup.Name}, opBackup); err != nil {
				return controller.BackupExecutionStatus{}, fmt.Errorf("get PerconaPGBackup %q after AlreadyExists: %w", backup.Name, err)
			}
		}
	}

	if managedByRuntime {
		origBackup := opBackup.DeepCopy()
		if immutableChangeMsg := immutableBackupSpecChangeMessage(opBackup, backup); immutableChangeMsg != "" {
			immutableErr := fmt.Errorf("cannot change immutable backup spec")
			l.Error(
				immutableErr,
				"failed to reconcile backup CR",
				"backup", backup.Name,
				"requestedInstanceName", backup.Spec.InstanceRef.Name,
				"existingInstanceName", opBackup.Spec.PGCluster,
				"requestedRepoName", backup.Spec.StorageRef.Name,
				"existingRepoName", safeDerefString(opBackup.Spec.RepoName),
				"reason", immutableChangeMsg,
			)
		}
		if err := ensureBackupControllerReference(opBackup); err != nil {
			return controller.BackupExecutionStatus{}, err
		}
		if err := c.Client().Patch(c.Context(), opBackup, client.MergeFrom(origBackup)); err != nil {
			return controller.BackupExecutionStatus{}, fmt.Errorf("patch PerconaPGBackup %q: %w", backup.Name, err)
		}
	}

	exec := controller.BackupExecutionStatus{
		OperatorBackupRef: opRef,
		Message:           string(opBackup.Status.State),
	}

	if !opBackup.CreationTimestamp.IsZero() {
		t := opBackup.CreationTimestamp
		exec.StartedAt = &t
	}

	switch opBackup.Status.State {
	case pgv2.BackupFailed:
		exec.State = backupv1alpha1.BackupStateFailed
		if opBackup.Status.Error != "" {
			exec.Message = opBackup.Status.Error
		}
	case pgv2.BackupSucceeded:
		exec.State = backupv1alpha1.BackupStateSucceeded
		exec.CompletedAt = opBackup.Status.CompletedAt
		exec.Message = "Backup completed"
	case pgv2.BackupRunning, pgv2.BackupStarting:
		exec.State = backupv1alpha1.BackupStateRunning
		exec.Message = "Backup is running"
	default:
		exec.State = backupv1alpha1.BackupStatePending
		exec.Message = "Backup is pending"
	}

	return exec, nil
}

// SyncRestore resolves the source Backup CR, creates or updates the operator's
// restore resource with a controller reference, and maps operator status to
// OpenEverest states.
func (p *Provider) SyncRestore(c *controller.Context, restore *backupv1alpha1.Restore) (controller.RestoreExecutionStatus, error) {
	l := log.FromContext(c.Context())
	l.Info("Syncing restore", "name", restore.Name)

	if restore.Labels == nil {
		restore.Labels = map[string]string{}
	}
	if restore.Labels[instanceNameLabelKey] != restore.Spec.InstanceRef.Name {
		origRestoreCR := restore.DeepCopy()
		restore.Labels[instanceNameLabelKey] = restore.Spec.InstanceRef.Name
		if err := c.Client().Patch(c.Context(), restore, client.MergeFrom(origRestoreCR)); err != nil {
			return controller.RestoreExecutionStatus{}, fmt.Errorf("patch Restore %q labels: %w", restore.Name, err)
		}
		// Re-fetch the restore to ensure we have the latest resource version
		// before using it as a controller reference owner.
		if err := c.Client().Get(c.Context(), client.ObjectKeyFromObject(restore), restore); err != nil {
			return controller.RestoreExecutionStatus{}, fmt.Errorf("re-fetch Restore %q after label patch: %w", restore.Name, err)
		}
	}

	opRef := &apicommon.TypedObjectRef{
		Group: pgv2.GroupVersion.Group,
		Kind:  "PerconaPGRestore",
		Name:  restore.Name,
	}

	repoName, restoreOptions, pending, err := resolveRestoreSource(c, restore, opRef)
	if err != nil {
		return controller.RestoreExecutionStatus{}, err
	}
	if pending != nil {
		return *pending, nil
	}

	opRestore := &pgv2.PerconaPGRestore{}
	err = c.Client().Get(c.Context(), client.ObjectKey{Namespace: restore.Namespace, Name: restore.Name}, opRestore)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return controller.RestoreExecutionStatus{}, fmt.Errorf("get PerconaPGRestore %q: %w", restore.Name, err)
		}

		opRestore = &pgv2.PerconaPGRestore{
			ObjectMeta: metav1.ObjectMeta{Name: restore.Name, Namespace: restore.Namespace},
			Spec: pgv2.PerconaPGRestoreSpec{
				PGCluster: restore.Spec.InstanceRef.Name,
				RepoName:  repoName,
				Options:   restoreOptions,
			},
		}

		if err := controllerutil.SetControllerReference(restore, opRestore, c.Client().Scheme()); err != nil {
			return controller.RestoreExecutionStatus{}, fmt.Errorf("set restore controller reference: %w", err)
		}
		if err := c.Client().Create(c.Context(), opRestore); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return controller.RestoreExecutionStatus{}, fmt.Errorf("create PerconaPGRestore %q: %w", restore.Name, err)
			}
			if err := c.Client().Get(c.Context(), client.ObjectKey{Namespace: restore.Namespace, Name: restore.Name}, opRestore); err != nil {
				return controller.RestoreExecutionStatus{}, fmt.Errorf("get PerconaPGRestore %q after AlreadyExists: %w", restore.Name, err)
			}
		}
	}

	out := controller.RestoreExecutionStatus{
		OperatorRestoreRef: opRef,
		Message:            string(opRestore.Status.State),
	}

	if !opRestore.CreationTimestamp.IsZero() {
		t := opRestore.CreationTimestamp
		out.StartedAt = &t
	}

	switch opRestore.Status.State {
	case pgv2.RestoreFailed:
		out.State = backupv1alpha1.RestoreStateFailed
	case pgv2.RestoreSucceeded:
		out.State = backupv1alpha1.RestoreStateSucceeded
		out.CompletedAt = opRestore.Status.CompletedAt
		out.Message = "Restore completed"
	case pgv2.RestoreStarting, pgv2.RestoreRunning:
		out.State = backupv1alpha1.RestoreStateRunning
		out.Message = "Restore is running"
	default:
		out.State = backupv1alpha1.RestoreStatePending
		out.Message = "Restore is pending"
	}

	return out, nil
}

// resolveRestoreRepoName determines the repo name and the pgBackRest backup
// set label for the restore from the source operator backup.
func resolveRestoreRepoName(
	c *controller.Context,
	restore *backupv1alpha1.Restore,
	opBackupName string,
	opRef *apicommon.TypedObjectRef,
) (*string, string, *controller.RestoreExecutionStatus, error) {
	opBackup := &pgv2.PerconaPGBackup{}
	if err := c.Client().Get(c.Context(), client.ObjectKey{Namespace: restore.Namespace, Name: opBackupName}, opBackup); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, "", &controller.RestoreExecutionStatus{
				State:              backupv1alpha1.RestoreStatePending,
				Message:            "Waiting for operator backup",
				OperatorRestoreRef: opRef,
			}, nil
		}
		return nil, "", nil, fmt.Errorf("get operator backup %q: %w", opBackupName, err)
	}

	if opBackup.Status.State == pgv2.BackupFailed {
		message := "Operator backup failed; cannot restore"
		if opBackup.Status.Error != "" {
			message = opBackup.Status.Error
		}
		return nil, "", &controller.RestoreExecutionStatus{
			State:              backupv1alpha1.RestoreStateFailed,
			Message:            message,
			OperatorRestoreRef: opRef,
		}, nil
	}
	if opBackup.Status.State != pgv2.BackupSucceeded {
		return nil, "", &controller.RestoreExecutionStatus{
			State:              backupv1alpha1.RestoreStatePending,
			Message:            "Waiting for operator backup to complete",
			OperatorRestoreRef: opRef,
		}, nil
	}

	return opBackup.Spec.RepoName, opBackup.Status.BackupName, nil, nil
}

// resolveRestoreSource translates the Restore's data source into the operator
// inputs: the pgBackRest repo to read from and the restore options.
//
// A non-nil RestoreExecutionStatus means the source is not usable yet (or at
// all) and the caller should surface that status verbatim.
func resolveRestoreSource(
	c *controller.Context,
	restore *backupv1alpha1.Restore,
	opRef *apicommon.TypedObjectRef,
) (*string, []string, *controller.RestoreExecutionStatus, error) {
	switch restore.Spec.DataSource.Type {
	case backupv1alpha1.DataSourceTypeBackup:
		return resolveBackupSource(c, restore, opRef)
	case backupv1alpha1.DataSourceTypePointInTime:
		return resolvePointInTimeSource(c, restore, opRef)
	default:
		return nil, nil, &controller.RestoreExecutionStatus{
			State:              backupv1alpha1.RestoreStateFailed,
			Message:            fmt.Sprintf("Unsupported dataSource type %q", restore.Spec.DataSource.Type),
			OperatorRestoreRef: opRef,
		}, nil
	}
}

// resolveBackupSource restores the state captured by a named Backup CR. The
// restore is pinned to the backup's pgBackRest set label and stops right after
// restoring it (--type=immediate) so no WAL is replayed past the backup's
// point-in-time.
func resolveBackupSource(
	c *controller.Context,
	restore *backupv1alpha1.Restore,
	opRef *apicommon.TypedObjectRef,
) (*string, []string, *controller.RestoreExecutionStatus, error) {
	ref := restore.Spec.DataSource.Backup
	if ref == nil || ref.BackupRef.Name == "" {
		return nil, nil, &controller.RestoreExecutionStatus{
			State:              backupv1alpha1.RestoreStateFailed,
			Message:            "Restore dataSource.backup.backupRef is required",
			OperatorRestoreRef: opRef,
		}, nil
	}

	sourceBackup := &backupv1alpha1.Backup{}
	if err := c.Client().Get(c.Context(), client.ObjectKey{Namespace: restore.Namespace, Name: ref.BackupRef.Name}, sourceBackup); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, &controller.RestoreExecutionStatus{
				State:              backupv1alpha1.RestoreStatePending,
				Message:            "Waiting for source Backup",
				OperatorRestoreRef: opRef,
			}, nil
		}
		return nil, nil, nil, fmt.Errorf("get source Backup %q: %w", ref.BackupRef.Name, err)
	}

	if sourceBackup.Status.State == backupv1alpha1.BackupStateFailed {
		return nil, nil, &controller.RestoreExecutionStatus{
			State:              backupv1alpha1.RestoreStateFailed,
			Message:            "Source Backup failed; cannot restore",
			OperatorRestoreRef: opRef,
		}, nil
	}

	// Resolve the repo name and backup set label from the source backup's operator backup.
	repoName, backupSetName, pending, err := resolveRestoreRepoName(c, restore, sourceBackup.Name, opRef)
	if err != nil {
		return nil, nil, nil, err
	}
	if pending != nil {
		return nil, nil, pending, nil
	}

	var options []string
	// Pin the restore to the specific backup set so pgBackRest does not
	// simply restore the latest backup in the repo.
	if backupSetName != "" {
		options = append(options, fmt.Sprintf("--set=%s", backupSetName))
	}
	// Stop right after restoring the backup set without replaying WAL files.
	// Without this, pgBackRest replays all available WAL and the database
	// ends up at the latest state rather than the backup's point-in-time.
	options = append(options, "--type=immediate")

	return repoName, options, nil, nil
}

// resolvePointInTimeSource rolls the WAL stream forward to a recovery target.
//
// The client names the stream (source Instance + storage) and the target,
// never a backup: pgBackRest selects the base backup itself when no --set is
// given, so unlike PSMDB and PXC no base selection happens here. The storage
// maps to the pgBackRest repo the WAL is archived to.
func resolvePointInTimeSource(
	c *controller.Context,
	restore *backupv1alpha1.Restore,
	opRef *apicommon.TypedObjectRef,
) (*string, []string, *controller.RestoreExecutionStatus, error) {
	pitr := restore.Spec.DataSource.PointInTime
	if pitr == nil {
		return nil, nil, &controller.RestoreExecutionStatus{
			State:              backupv1alpha1.RestoreStateFailed,
			Message:            "Restore dataSource.pointInTime is required when type is \"PointInTime\"",
			OperatorRestoreRef: opRef,
		}, nil
	}
	// A schema rule already enforces this; repeated for paths that bypass
	// admission.
	if pitr.RecoveryTarget == backupv1alpha1.RecoveryTargetDate && pitr.Date == nil {
		return nil, nil, &controller.RestoreExecutionStatus{
			State:              backupv1alpha1.RestoreStateFailed,
			Message:            "Restore dataSource.pointInTime.date is required when recoveryTarget is \"date\"",
			OperatorRestoreRef: opRef,
		}, nil
	}

	// A PerconaPGRestore reads from a repo registered on the target cluster,
	// and repo paths embed the owning cluster's identity -- another Instance's
	// WAL stream is not reachable through them.
	if pitr.Source.InstanceRef != nil && pitr.Source.InstanceRef.Name != restore.Spec.InstanceRef.Name {
		return nil, nil, &controller.RestoreExecutionStatus{
			State:              backupv1alpha1.RestoreStateFailed,
			Message:            "This provider only supports point-in-time recovery of the target Instance's own stream",
			OperatorRestoreRef: opRef,
		}, nil
	}

	pgCluster := &pgv2.PerconaPGCluster{}
	if err := c.Client().Get(c.Context(), client.ObjectKey{Namespace: restore.Namespace, Name: restore.Spec.InstanceRef.Name}, pgCluster); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, &controller.RestoreExecutionStatus{
				State:              backupv1alpha1.RestoreStatePending,
				Message:            fmt.Sprintf("Waiting for PerconaPGCluster %q", restore.Spec.InstanceRef.Name),
				OperatorRestoreRef: opRef,
			}, nil
		}
		return nil, nil, nil, fmt.Errorf("get PerconaPGCluster %q: %w", restore.Spec.InstanceRef.Name, err)
	}

	repoName, found := storageNameToRepoName(c, pitr.Source.StorageRef.Name, pgCluster)
	if !found || !hasRepo(pgCluster, repoName) {
		return nil, nil, &controller.RestoreExecutionStatus{
			State:              backupv1alpha1.RestoreStatePending,
			Message:            fmt.Sprintf("Waiting for storage %q to be configured on the instance", pitr.Source.StorageRef.Name),
			OperatorRestoreRef: opRef,
		}, nil
	}

	// No --set: pgBackRest selects the base backup for the target itself.
	// For "latest" no options are needed either -- the default recovery type
	// replays the WAL stream to its end.
	var options []string
	if pitr.RecoveryTarget == backupv1alpha1.RecoveryTargetDate {
		// PostgreSQL interprets timezone-less timestamps as node-local time,
		// so the offset is always emitted.
		options = append(options,
			"--type=time",
			fmt.Sprintf("--target=%q", pitr.Date.UTC().Format("2006-01-02 15:04:05-07:00")))
	}

	return &repoName, options, nil, nil
}

// CleanupBackup deletes the operator backup resource.
// When the Backup's DeletionPolicy is Retain, the operator's
// internal.percona.com/delete-backup finalizer is removed first so the
// Percona operator skips data purging and leaves the backup data in storage.
// When the policy is Delete (default), the finalizer is left in place so the
// operator cleans up the data.
// Return true only when fully deleted, false to requeue.
func (p *Provider) CleanupBackup(c *controller.Context, backup *backupv1alpha1.Backup) (bool, error) {
	l := log.FromContext(c.Context())
	l.Info("Cleaning up backup", "name", backup.Name, "deletionPolicy", backup.Spec.DeletionPolicy)

	name := backup.Name

	opBackup := &pgv2.PerconaPGBackup{}
	err := c.Client().Get(c.Context(), client.ObjectKey{Namespace: backup.Namespace, Name: name}, opBackup)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, fmt.Errorf("get PerconaPGBackup %q: %w", name, err)
	}

	// When the user wants to retain backup data, strip the operator's
	// delete-backup finalizer so it won't purge data from storage.
	if backup.Spec.DeletionPolicy == backupv1alpha1.BackupDeletionPolicyRetain {
		if controllerutil.RemoveFinalizer(opBackup, finalizerDeleteBackupData) {
			if err := c.Client().Update(c.Context(), opBackup); err != nil {
				return false, fmt.Errorf("remove delete-backup finalizer from PerconaPGBackup %q: %w", name, err)
			}
		}
	}

	if opBackup.DeletionTimestamp.IsZero() {
		if err := c.Client().Delete(c.Context(), opBackup); err != nil && !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("delete PerconaPGBackup %q: %w", name, err)
		}
	}

	return false, nil
}

// CleanupRestore deletes the operator restore resource. Return true when fully
// deleted, false to requeue.
func (p *Provider) CleanupRestore(c *controller.Context, restore *backupv1alpha1.Restore) (bool, error) {
	l := log.FromContext(c.Context())
	l.Info("Cleaning up restore", "name", restore.Name)

	name := restore.Name

	opRestore := &pgv2.PerconaPGRestore{}
	err := c.Client().Get(c.Context(), client.ObjectKey{Namespace: restore.Namespace, Name: name}, opRestore)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, fmt.Errorf("get PerconaPGRestore %q: %w", name, err)
	}

	if opRestore.DeletionTimestamp.IsZero() {
		if err := c.Client().Delete(c.Context(), opRestore); err != nil && !apierrors.IsNotFound(err) {
			return false, fmt.Errorf("delete PerconaPGRestore %q: %w", name, err)
		}
	}

	return false, nil
}

// BackupWatches implements controller.BackupWatcher. Register watches so operator
// backup status changes trigger reconciliation.
func (p *Provider) BackupWatches() []controller.WatchConfig {
	return []controller.WatchConfig{
		controller.WatchExternal(
			&pgv2.PerconaPGBackup{},
			handler.EnqueueRequestsFromMapFunc(func(_ context.Context, obj client.Object) []reconcile.Request {
				return []reconcile.Request{{NamespacedName: client.ObjectKeyFromObject(obj)}}
			}),
			controller.ResourceVersionChangedPredicate,
		),
	}
}

// RestoreWatches implements controller.RestoreWatcher. Register watches so operator
// restore status changes trigger reconciliation.
func (p *Provider) RestoreWatches() []controller.WatchConfig {
	return []controller.WatchConfig{
		controller.WatchExternal(
			&pgv2.PerconaPGRestore{},
			handler.EnqueueRequestsFromMapFunc(func(_ context.Context, obj client.Object) []reconcile.Request {
				return []reconcile.Request{{NamespacedName: client.ObjectKeyFromObject(obj)}}
			}),
			controller.ResourceVersionChangedPredicate,
		),
	}
}

func safeDerefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func hasRepo(pgCluster *pgv2.PerconaPGCluster, repoName string) bool {
	for _, repo := range pgCluster.Spec.Backups.PGBackRest.Repos {
		if repo.Name == repoName {
			return true
		}
	}
	return false
}

// isStanzaCreated checks whether the pgBackRest stanza has been created for
// the given repo by inspecting the cluster's status. The Percona PG operator
// sets RepoStatus.StanzaCreated to true once `pgbackrest stanza-create`
// completes successfully.
func isStanzaCreated(pgCluster *pgv2.PerconaPGCluster, repoName string) bool {
	if pgCluster.Status.PGBackRest == nil {
		return false
	}
	for _, repo := range pgCluster.Status.PGBackRest.Repos {
		if repo.Name == repoName {
			return repo.StanzaCreated
		}
	}
	return false
}

func immutableBackupSpecChangeMessage(opBackup *pgv2.PerconaPGBackup, backup *backupv1alpha1.Backup) string {
	if backup.Spec.InstanceRef.Name != opBackup.Spec.PGCluster {
		return fmt.Sprintf(
			"cannot change backup spec.InstanceRef.Name after creation (requested %q, existing %q)",
			backup.Spec.InstanceRef.Name,
			opBackup.Spec.PGCluster,
		)
	}
	if opBackup.Spec.RepoName != nil && backup.Spec.StorageRef.Name != *opBackup.Spec.RepoName {
		return fmt.Sprintf(
			"cannot change backup spec.StorageRef.Name after creation (requested %q, existing %q)",
			backup.Spec.StorageRef.Name,
			*opBackup.Spec.RepoName,
		)
	}

	return ""
}

const defaultBackupType = "full"

// resolveBackupType extracts the pgBackRest backup type from the Backup CR's
// parameters and returns the pgBackRest CLI --type flag value (full/diff/incr).
func resolveBackupType(backup *backupv1alpha1.Backup) string {
	if backup.Spec.Parameters == nil || len(backup.Spec.Parameters.Raw) == 0 {
		return defaultBackupType
	}
	var cfg struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(backup.Spec.Parameters.Raw, &cfg); err != nil {
		return defaultBackupType
	}
	if cfg.Type == "" {
		return defaultBackupType
	}
	return cfg.Type
}
