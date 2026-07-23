package provider

import (
	"context"
	"fmt"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	corev1 "k8s.io/api/core/v1"
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
	if backup.Labels[instanceNameLabelKey] != backup.Spec.InstanceName {
		origBackupCR := backup.DeepCopy()
		backup.Labels[instanceNameLabelKey] = backup.Spec.InstanceName
		if err := c.Client().Patch(c.Context(), backup, client.MergeFrom(origBackupCR)); err != nil {
			return controller.BackupExecutionStatus{}, fmt.Errorf("patch Backup %q labels: %w", backup.Name, err)
		}
	}

	opRef := &corev1.TypedLocalObjectReference{
		APIGroup: ptrTo(pgv2.GroupVersion.Group),
		Kind:     "PerconaPGBackup",
		Name:     backup.Name,
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
		if err := c.Client().Get(c.Context(), client.ObjectKey{Namespace: backup.Namespace, Name: backup.Spec.InstanceName}, pgCluster); err != nil {
			if apierrors.IsNotFound(err) {
				return controller.BackupExecutionStatus{
					State:             backupv1alpha1.BackupStatePending,
					Message:           "Waiting for PerconaPGCluster",
					OperatorBackupRef: opRef,
				}, nil
			}
			return controller.BackupExecutionStatus{}, fmt.Errorf("get PerconaPGCluster %q: %w", backup.Spec.InstanceName, err)
		}

		// Ensure the storage referenced by this backup is registered on the Instance.
		// This must happen before checking if backups are enabled, because when all
		// storages were pruned the provider disables backups — auto-registering the
		// storage will trigger the next Instance Sync to re-enable them.
		repoName, found := storageNameToRepoName(c, backup.Spec.StorageName, pgCluster)
		if !found {
			if registered, err := autoRegisterStorage(c, backup.Spec.StorageName); err != nil {
				return controller.BackupExecutionStatus{}, fmt.Errorf("auto-register storage %q: %w", backup.Spec.StorageName, err)
			} else if !registered {
				return controller.BackupExecutionStatus{
					State:             backupv1alpha1.BackupStatePending,
					Message:           fmt.Sprintf("Waiting for storage %q to be configured on the instance", backup.Spec.StorageName),
					OperatorBackupRef: opRef,
				}, nil
			}
			// Storage was registered — requeue to let the next Sync configure the repo.
			return controller.BackupExecutionStatus{
				State:             backupv1alpha1.BackupStatePending,
				Message:           fmt.Sprintf("Storage %q registered on the instance, waiting for repo configuration", backup.Spec.StorageName),
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

		opBackup = &pgv2.PerconaPGBackup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      backup.Name,
				Namespace: backup.Namespace,
			},
			Spec: pgv2.PerconaPGBackupSpec{
				PGCluster: backup.Spec.InstanceName,
				RepoName:  &repoName,
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
				"requestedInstanceName", backup.Spec.InstanceName,
				"existingInstanceName", opBackup.Spec.PGCluster,
				"requestedRepoName", backup.Spec.StorageName,
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
	if restore.Labels[instanceNameLabelKey] != restore.Spec.InstanceName {
		origRestoreCR := restore.DeepCopy()
		restore.Labels[instanceNameLabelKey] = restore.Spec.InstanceName
		if err := c.Client().Patch(c.Context(), restore, client.MergeFrom(origRestoreCR)); err != nil {
			return controller.RestoreExecutionStatus{}, fmt.Errorf("patch Restore %q labels: %w", restore.Name, err)
		}
	}

	opRef := &corev1.TypedLocalObjectReference{
		APIGroup: ptrTo(pgv2.GroupVersion.Group),
		Kind:     "PerconaPGRestore",
		Name:     restore.Name,
	}

	if restore.Spec.DataSource.Backup == nil || restore.Spec.DataSource.Backup.BackupName == "" {
		return controller.RestoreExecutionStatus{
			State:              backupv1alpha1.RestoreStateFailed,
			Message:            "Restore dataSource.backup.backupName is required",
			OperatorRestoreRef: opRef,
		}, nil
	}

	backupName := restore.Spec.DataSource.Backup.BackupName

	sourceBackup := &backupv1alpha1.Backup{}
	if err := c.Client().Get(c.Context(), client.ObjectKey{Namespace: restore.Namespace, Name: backupName}, sourceBackup); err != nil {
		if apierrors.IsNotFound(err) {
			return controller.RestoreExecutionStatus{
				State:              backupv1alpha1.RestoreStatePending,
				Message:            "Waiting for source Backup",
				OperatorRestoreRef: opRef,
			}, nil
		}
		return controller.RestoreExecutionStatus{}, fmt.Errorf("get source Backup %q: %w", backupName, err)
	}

	if sourceBackup.Status.State == backupv1alpha1.BackupStateFailed {
		return controller.RestoreExecutionStatus{
			State:              backupv1alpha1.RestoreStateFailed,
			Message:            "Source Backup failed; cannot restore",
			OperatorRestoreRef: opRef,
		}, nil
	}

	if restore.Spec.DataSource.Backup.PITR != nil {
		if sourceBackup.Status.State != backupv1alpha1.BackupStateSucceeded {
			return controller.RestoreExecutionStatus{
				State:              backupv1alpha1.RestoreStatePending,
				Message:            "Waiting for source Backup to complete",
				OperatorRestoreRef: opRef,
			}, nil
		}
	}

	opBackupName := sourceBackup.Name

	// Resolve the repo name from the source backup's operator backup.
	repoName, pending, err := resolveRestoreRepoName(c, restore, opBackupName, opRef)
	if err != nil {
		return controller.RestoreExecutionStatus{}, err
	}
	if pending != nil {
		return *pending, nil
	}

	// Build PITR options if requested.
	restoreOptions, pitrPending, err := desiredPITRRestoreOptions(c, restore, opBackupName, opRef)
	if err != nil {
		return controller.RestoreExecutionStatus{}, err
	}
	if pitrPending != nil {
		return *pitrPending, nil
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
				PGCluster: restore.Spec.InstanceName,
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

	origRestore := opRestore.DeepCopy()
	if err := controllerutil.SetControllerReference(restore, opRestore, c.Client().Scheme()); err != nil {
		return controller.RestoreExecutionStatus{}, fmt.Errorf("set restore controller reference: %w", err)
	}
	if err := c.Client().Patch(c.Context(), opRestore, client.MergeFrom(origRestore)); err != nil {
		return controller.RestoreExecutionStatus{}, fmt.Errorf("patch PerconaPGRestore %q: %w", restore.Name, err)
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

// resolveRestoreRepoName determines the repo name for the restore from the
// source operator backup.
func resolveRestoreRepoName(
	c *controller.Context,
	restore *backupv1alpha1.Restore,
	opBackupName string,
	opRef *corev1.TypedLocalObjectReference,
) (*string, *controller.RestoreExecutionStatus, error) {
	opBackup := &pgv2.PerconaPGBackup{}
	if err := c.Client().Get(c.Context(), client.ObjectKey{Namespace: restore.Namespace, Name: opBackupName}, opBackup); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, &controller.RestoreExecutionStatus{
				State:              backupv1alpha1.RestoreStatePending,
				Message:            "Waiting for operator backup",
				OperatorRestoreRef: opRef,
			}, nil
		}
		return nil, nil, fmt.Errorf("get operator backup %q: %w", opBackupName, err)
	}

	if opBackup.Status.State == pgv2.BackupFailed {
		message := "Operator backup failed; cannot restore"
		if opBackup.Status.Error != "" {
			message = opBackup.Status.Error
		}
		return nil, &controller.RestoreExecutionStatus{
			State:              backupv1alpha1.RestoreStateFailed,
			Message:            message,
			OperatorRestoreRef: opRef,
		}, nil
	}
	if opBackup.Status.State != pgv2.BackupSucceeded {
		return nil, &controller.RestoreExecutionStatus{
			State:              backupv1alpha1.RestoreStatePending,
			Message:            "Waiting for operator backup to complete",
			OperatorRestoreRef: opRef,
		}, nil
	}

	return opBackup.Spec.RepoName, nil, nil
}

// desiredPITRRestoreOptions builds pgBackRest restore options for PITR.
func desiredPITRRestoreOptions(
	c *controller.Context,
	restore *backupv1alpha1.Restore,
	opBackupName string,
	opRef *corev1.TypedLocalObjectReference,
) ([]string, *controller.RestoreExecutionStatus, error) {
	if restore.Spec.DataSource.Backup == nil || restore.Spec.DataSource.Backup.PITR == nil {
		return nil, nil, nil
	}
	pitr := restore.Spec.DataSource.Backup.PITR

	if pitr.Type == backupv1alpha1.PITRTypeDate && pitr.Date == nil {
		return nil, &controller.RestoreExecutionStatus{
			State:              backupv1alpha1.RestoreStateFailed,
			Message:            "Restore dataSource.pitr.date is required when pitr.type is \"date\"",
			OperatorRestoreRef: opRef,
		}, nil
	}

	opBackup := &pgv2.PerconaPGBackup{}
	if err := c.Client().Get(c.Context(), client.ObjectKey{Namespace: restore.Namespace, Name: opBackupName}, opBackup); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, &controller.RestoreExecutionStatus{
				State:              backupv1alpha1.RestoreStatePending,
				Message:            "Waiting for operator backup",
				OperatorRestoreRef: opRef,
			}, nil
		}
		return nil, nil, fmt.Errorf("get operator backup %q: %w", opBackupName, err)
	}

	if opBackup.Status.State == pgv2.BackupFailed {
		message := "Operator backup failed; cannot run PITR"
		if opBackup.Status.Error != "" {
			message = opBackup.Status.Error
		}
		return nil, &controller.RestoreExecutionStatus{
			State:              backupv1alpha1.RestoreStateFailed,
			Message:            message,
			OperatorRestoreRef: opRef,
		}, nil
	}
	if opBackup.Status.State != pgv2.BackupSucceeded {
		return nil, &controller.RestoreExecutionStatus{
			State:              backupv1alpha1.RestoreStatePending,
			Message:            "Waiting for operator backup to complete",
			OperatorRestoreRef: opRef,
		}, nil
	}

	// pgBackRest PITR is configured via --type and --target options.
	var opts []string
	opts = append(opts, "--type=time")
	if pitr.Date != nil {
		opts = append(opts, fmt.Sprintf("--target=%q", pitr.Date.UTC().Format("2006-01-02 15:04:05")))
	}

	return opts, nil, nil
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

// hasActiveRestoreForInstance reports whether the namespace has at least one
// non-terminal Restore for the given instance.
func hasActiveRestoreForInstance(c *controller.Context, namespace, instanceName string) (bool, error) {
	restoreList := &backupv1alpha1.RestoreList{}
	if err := c.Client().List(
		c.Context(),
		restoreList,
		client.InNamespace(namespace),
	); err != nil {
		return false, fmt.Errorf("list Restore resources for instance %q: %w", instanceName, err)
	}

	for i := range restoreList.Items {
		r := restoreList.Items[i]
		if r.Spec.InstanceName != instanceName || !r.DeletionTimestamp.IsZero() {
			continue
		}
		switch r.Status.State {
		case backupv1alpha1.RestoreStateSucceeded, backupv1alpha1.RestoreStateFailed:
			continue
		default:
			return true, nil
		}
	}

	return false, nil
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

func ptrTo[T any](v T) *T {
	return &v
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
	if backup.Spec.InstanceName != opBackup.Spec.PGCluster {
		return fmt.Sprintf(
			"cannot change backup spec.instanceName after creation (requested %q, existing %q)",
			backup.Spec.InstanceName,
			opBackup.Spec.PGCluster,
		)
	}
	if opBackup.Spec.RepoName != nil && backup.Spec.StorageName != *opBackup.Spec.RepoName {
		return fmt.Sprintf(
			"cannot change backup spec.storageName after creation (requested %q, existing %q)",
			backup.Spec.StorageName,
			*opBackup.Spec.RepoName,
		)
	}

	return ""
}
