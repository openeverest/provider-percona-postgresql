package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	upstreamv1beta1 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/upstream.pgv2.percona.com/v1beta1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Compile-time interface checks.
var _ controller.BackupMirror = (*Provider)(nil)

const (
	// maxPGBackRestRepos is the maximum number of repos pgBackRest supports (repo1..repo4).
	maxPGBackRestRepos = 4

	// annotationPGBackrestBackupJobType is the annotation the PG operator sets
	// on PerconaPGBackup CRs to indicate the backup's origin.
	// Known values: "manual", "replica-create".
	annotationPGBackrestBackupJobType = "pgv2.percona.com/pgbackrest-backup-job-type"

	// pgBackrestJobTypeReplicaCreate is the job-type annotation value for the
	// initial backup that the PG operator takes to bootstrap replicas. These
	// are internal operator backups and should not be mirrored.
	pgBackrestJobTypeReplicaCreate = "replica-create"

	// repoSlotMapAnnotation stores a stable JSON mapping of storage names to
	// repo slot indices (0-based) on the PerconaPGCluster. This prevents repo
	// slots from shifting when storages are added or removed.
	repoSlotMapAnnotation = "openeverest.io/repo-slot-map"
)

// Mirror implements controller.BackupMirror (optional). The runtime invokes
// Mirror() for operator backup events. Return a Backup CR to create it
// idempotently, or nil to skip (on-demand backups, missing Instance, or backups
// when Instance has no backup configuration).
func (p *Provider) Mirror(ctx context.Context, c client.Client, obj client.Object) (*backupv1alpha1.Backup, error) {
	pgBackup, ok := obj.(*pgv2.PerconaPGBackup)
	if !ok {
		return nil, fmt.Errorf("unexpected operator backup type %T", obj)
	}

	if !pgBackup.DeletionTimestamp.IsZero() {
		return nil, nil
	}

	// Skip backups that are already owned by an OpenEverest Backup CR (on-demand).
	for _, owner := range pgBackup.OwnerReferences {
		if owner.Controller != nil && *owner.Controller && owner.APIVersion == backupv1alpha1.GroupVersion.String() && owner.Kind == "Backup" {
			return nil, nil
		}
	}

	// Determine if this is a scheduled backup.
	if !isScheduledBackup(pgBackup) {
		return nil, nil
	}

	scheduleName := scheduleNameFromBackup(pgBackup)

	instance := &corev1alpha1.Instance{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: pgBackup.Namespace, Name: pgBackup.Spec.PGCluster}, instance); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get instance %q: %w", pgBackup.Spec.PGCluster, err)
	}

	if instance.Spec.Backup == nil || instance.Spec.Backup.ClassRef.Name == "" {
		return nil, nil
	}

	repoName := ""
	if pgBackup.Spec.RepoName != nil {
		repoName = *pgBackup.Spec.RepoName
	}
	if repoName == "" {
		return nil, nil
	}

	// Resolve the pgBackRest repo name (e.g. "repo1") back to the OpenEverest
	// storage name using the slot map persisted on the PerconaPGCluster.
	storageName, err := repoNameToStorageName(ctx, c, pgBackup.Namespace, pgBackup.Spec.PGCluster, repoName)
	if err != nil {
		return nil, err
	}
	if storageName == "" {
		// Cannot resolve storage name; skip mirroring.
		return nil, nil
	}

	return &backupv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pgBackup.Name,
			Namespace: pgBackup.Namespace,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: pgv2.GroupVersion.String(),
				Kind:       "PerconaPGBackup",
				Name:       pgBackup.Name,
				UID:        pgBackup.UID,
			}},
		},
		Spec: backupv1alpha1.BackupSpec{
			InstanceName:    pgBackup.Spec.PGCluster,
			BackupClassName: instance.Spec.Backup.ClassRef.Name,
			StorageName:     storageName,
			ScheduleName:    scheduleName,
		},
	}, nil
}

// repoNameToStorageName resolves a pgBackRest repo name (e.g. "repo1") back to
// the OpenEverest storage name by reading the slot map annotation from the
// PerconaPGCluster resource.
func repoNameToStorageName(ctx context.Context, c client.Client, namespace, clusterName, repoName string) (string, error) {
	pgCluster := &pgv2.PerconaPGCluster{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: clusterName}, pgCluster); err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", fmt.Errorf("get PerconaPGCluster %q for repo slot map: %w", clusterName, err)
	}

	slotMap := loadRepoSlotMap(pgCluster)
	if slotMap == nil {
		return "", nil
	}

	// Reverse lookup: find the storage name that maps to this repo name's slot.
	for storageName, slot := range slotMap {
		if pgBackRestRepoName(slot) == repoName {
			return storageName, nil
		}
	}

	return "", nil
}

// isScheduledBackup determines whether a PerconaPGBackup was created by the
// PG operator for a CronJob-triggered (scheduled) backup.
//
// The PG operator creates PerconaPGBackup CRs for three kinds of backups:
//  1. replica-create — internal bootstrap backup (job-type annotation = "replica-create")
//  2. manual/on-demand — created by SyncBackup with an owner reference to a Backup CR
//  3. scheduled (cron) — auto-created for CronJob-triggered jobs, identified by
//     having generateName set and NOT being a replica-create backup
//
// On-demand backups are already filtered out by the owner-reference check in
// Mirror() before this function is called.
func isScheduledBackup(pgBackup *pgv2.PerconaPGBackup) bool {
	// Skip replica-create backups — these are internal operator backups for
	// bootstrapping replicas, not user-visible scheduled backups.
	if pgBackup.Annotations[annotationPGBackrestBackupJobType] == pgBackrestJobTypeReplicaCreate {
		return false
	}

	// The operator uses GenerateName when auto-creating PerconaPGBackup CRs
	// for CronJob-triggered backups. On-demand backups (created by SyncBackup)
	// use a fixed Name (matching the Backup CR name) and no GenerateName.
	return pgBackup.GenerateName != ""
}

// scheduleNameFromBackup derives the schedule name from the PerconaPGBackup.
// The PG operator names scheduled backups as "<cluster>-<repo>-<type>-<random>",
// so we extract the backup type (e.g. "full", "incr", "diff") as the schedule name.
func scheduleNameFromBackup(pgBackup *pgv2.PerconaPGBackup) string {
	return deriveScheduleName(pgBackup.Name, pgBackup.Spec.PGCluster, safeDerefString(pgBackup.Spec.RepoName))
}

// deriveScheduleName extracts the backup type (schedule name) from a
// PG-operator-generated backup name. The naming convention is:
//
//	<cluster>-<repo>-<type>-<random>
//
// For example "inst-0hc-repo1-full-p9dz9" → "full".
// If the name doesn't match the expected pattern, the full name is returned as
// a fallback schedule name.
func deriveScheduleName(backupName, clusterName, repoName string) string {
	prefix := clusterName + "-" + repoName + "-"
	if !strings.HasPrefix(backupName, prefix) {
		// Cannot parse; use the backup name itself as the schedule name.
		return backupName
	}
	remainder := strings.TrimPrefix(backupName, prefix)
	// remainder is "<type>-<random>" or "<type>-<random>-<random>".
	// Extract the type portion before the last dash-separated random suffix.
	parts := strings.SplitN(remainder, "-", 2)
	if len(parts) == 0 || parts[0] == "" {
		return backupName
	}
	return parts[0]
}

func applyBackupSettings(c *controller.Context, pgCluster *pgv2.PerconaPGCluster) error {
	if c.Instance().Spec.Backup == nil || !c.Instance().Spec.Backup.Enabled {
		backupsEnabled := false
		pgCluster.Spec.Backups.Enabled = &backupsEnabled
		pgCluster.Spec.Backups.PGBackRest.Repos = nil
		pgCluster.Spec.Backups.PGBackRest.Configuration = nil
		pgCluster.Spec.Backups.PGBackRest.Global = nil
		// Clean up all potential credential secrets.
		for i := 0; i < maxPGBackRestRepos; i++ {
			secretName := pgBackRestCredentialSecretName(c.Instance().Name, pgBackRestRepoName(i))
			orphan := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      secretName,
					Namespace: c.Instance().Namespace,
				},
			}
			if err := c.Delete(orphan); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("delete credential secret %q: %w", secretName, err)
			}
		}
		// Clear the slot map annotation since backups are disabled.
		delete(pgCluster.Annotations, repoSlotMapAnnotation)
		return nil
	}

	backupClass, err := c.BackupClassForInstance()
	if err != nil {
		return &controller.BackupConfigError{Reason: "BackupClassLookupFailed", Message: err.Error()}
	}
	if err := controller.ValidateInstanceBackupAgainstClass(c.Instance(), backupClass); err != nil {
		reason := "InvalidBackupConfiguration"
		if errors.Is(err, controller.ErrBackupClassLimitsExceeded) {
			reason = controller.LimitsExceededReason
		}
		return &controller.BackupConfigError{Reason: reason, Message: err.Error()}
	}

	providerSpec, err := c.ProviderSpec()
	if err != nil {
		return err
	}

	// Resolve pgBackRest image.
	pgBackRestImage := controller.GetDefaultImage(providerSpec, componentTypePGBackRest)
	if pgBackRestImage == "" {
		return &controller.BackupConfigError{Reason: "BackupImageUnavailable", Message: "cannot resolve default pgbackrest image from provider versions catalog"}
	}

	backupsEnabled := true
	pgCluster.Spec.Backups.Enabled = &backupsEnabled
	pgCluster.Spec.Backups.PGBackRest.Image = pgBackRestImage

	if len(c.Instance().Spec.Backup.Storages) == 0 {
		return &controller.BackupConfigError{Reason: "NoStoragesConfigured", Message: "spec.backup.enabled=true requires at least one storage"}
	}

	// Build a stable slot assignment: read existing mapping from the annotation
	// on the LIVE PGCluster (not the fresh in-memory one we're building), then
	// preserve slots for storages that still exist and assign free slots to new ones.
	existingCluster := &pgv2.PerconaPGCluster{}
	if err := c.Get(existingCluster, c.Instance().Name); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("get existing PerconaPGCluster for slot map: %w", err)
	}
	slotMap := loadRepoSlotMap(existingCluster)
	slotMap = reconcileRepoSlotMap(slotMap, c.Instance().Spec.Backup.Storages)

	var repos []upstreamv1beta1.PGBackRestRepo
	var configurations []corev1.VolumeProjection
	globalConfig := make(map[string]string)
	for _, storage := range c.Instance().Spec.Backup.Storages {
		if storage.StorageRef.Name == "" {
			return &controller.BackupConfigError{Reason: "StorageReferenceMissing", Message: fmt.Sprintf("backup storage %q must set storageRef.name", storage.Name)}
		}

		slot, ok := slotMap[storage.Name]
		if !ok {
			return &controller.BackupConfigError{Reason: "SlotAssignmentFailed", Message: fmt.Sprintf("no repo slot assigned for storage %q", storage.Name)}
		}
		repoName := pgBackRestRepoName(slot)

		bs, err := c.BackupStorage(storage.StorageRef.Name)
		if err != nil {
			return &controller.BackupConfigError{Reason: "StorageNotFound", Message: err.Error()}
		}

		repo, repoGlobal, err := buildPGBackRestRepo(repoName, storage, bs, string(c.Instance().UID))
		if err != nil {
			return err
		}

		// Merge repo-specific global config.
		for k, v := range repoGlobal {
			globalConfig[k] = v
		}

		// Apply schedules to the repo.
		if len(storage.Schedules) > 0 {
			schedules := buildPGBackRestSchedules(storage.Schedules)
			repo.BackupSchedules = schedules
		}

		repos = append(repos, repo)

		// Configure S3 credentials and options for the repo.
		if bs.Spec.Type == backupv1alpha1.BackupStorageTypeS3 && bs.Spec.S3 != nil {
			credSecret, projection, err := ensurePGBackRestCredentialSecret(c, repoName, bs)
			if err != nil {
				return err
			}
			if credSecret != nil {
				if err := c.Apply(credSecret); err != nil {
					return fmt.Errorf("apply pgBackRest credential secret for storage %q: %w", storage.Name, err)
				}
			}
			if projection != nil {
				configurations = append(configurations, *projection)
			}

			// Handle ForcePathStyle — pgBackRest calls this "uri-style".
			if bs.Spec.S3.ForcePathStyle != nil && *bs.Spec.S3.ForcePathStyle {
				globalConfig[repoName+"-s3-uri-style"] = "path"
			}

			// Handle VerifyTLS — pgBackRest calls this "storage-verify-tls".
			if bs.Spec.S3.VerifyTLS != nil && !*bs.Spec.S3.VerifyTLS {
				globalConfig[repoName+"-storage-verify-tls"] = "n"
			}
		}
	}

	pgCluster.Spec.Backups.PGBackRest.Repos = repos

	// Replace configurations entirely so removed repos don't leave stale secret projections.
	pgCluster.Spec.Backups.PGBackRest.Configuration = configurations

	// Replace global config entirely so removed repos don't leave stale entries.
	pgCluster.Spec.Backups.PGBackRest.Global = globalConfig

	// Persist the stable slot map annotation.
	saveRepoSlotMap(pgCluster, slotMap)

	// Clean up orphaned credential secrets for repos that no longer exist.
	activeSecrets := make(map[string]struct{}, len(c.Instance().Spec.Backup.Storages))
	for _, storage := range c.Instance().Spec.Backup.Storages {
		if slot, ok := slotMap[storage.Name]; ok {
			activeSecrets[pgBackRestCredentialSecretName(c.Instance().Name, pgBackRestRepoName(slot))] = struct{}{}
		}
	}
	for i := 0; i < maxPGBackRestRepos; i++ {
		secretName := pgBackRestCredentialSecretName(c.Instance().Name, pgBackRestRepoName(i))
		if _, active := activeSecrets[secretName]; active {
			continue
		}
		orphan := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      secretName,
				Namespace: c.Instance().Namespace,
			},
		}
		if err := c.Delete(orphan); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete orphaned credential secret %q: %w", secretName, err)
		}
	}

	return nil
}

// pgBackRestRepoName returns a pgBackRest repo name (repo1..repo4) for the
// given zero-based storage index.
func pgBackRestRepoName(index int) string {
	return fmt.Sprintf("repo%d", index+1)
}

// storageNameToRepoName resolves the pgBackRest repo name (repo1..repo4) for
// an OpenEverest storage name. If a PGCluster is provided, it reads the
// persisted slot map annotation for stable resolution; otherwise it computes
// the mapping from the current storages list.
func storageNameToRepoName(c *controller.Context, storageName string, pgCluster *pgv2.PerconaPGCluster) (string, bool) {
	if c.Instance().Spec.Backup == nil {
		return "", false
	}
	var existing repoSlotMap
	if pgCluster != nil {
		existing = loadRepoSlotMap(pgCluster)
	}
	slotMap := reconcileRepoSlotMap(existing, c.Instance().Spec.Backup.Storages)
	slot, ok := slotMap[storageName]
	if !ok {
		return "", false
	}
	return pgBackRestRepoName(slot), true
}

// repoSlotMap maps storage names to their assigned repo slot indices (0-based).
type repoSlotMap map[string]int

// loadRepoSlotMap reads the stable slot mapping from the PGCluster annotation.
func loadRepoSlotMap(pgCluster *pgv2.PerconaPGCluster) repoSlotMap {
	if pgCluster.Annotations == nil {
		return nil
	}
	raw := pgCluster.Annotations[repoSlotMapAnnotation]
	if raw == "" {
		return nil
	}
	var m repoSlotMap
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil
	}
	return m
}

// saveRepoSlotMap persists the stable slot mapping as an annotation on the PGCluster.
func saveRepoSlotMap(pgCluster *pgv2.PerconaPGCluster, m repoSlotMap) {
	if pgCluster.Annotations == nil {
		pgCluster.Annotations = make(map[string]string)
	}
	data, _ := json.Marshal(m)
	pgCluster.Annotations[repoSlotMapAnnotation] = string(data)
}

// reconcileRepoSlotMap takes an existing slot map (possibly nil) and the current
// list of storages, and returns an updated map where:
// - Existing storages keep their previously assigned slots.
// - Removed storages are evicted (their slots become free).
// - New storages are assigned to the lowest available free slot.
func reconcileRepoSlotMap(existing repoSlotMap, storages []corev1alpha1.InstanceBackupStorage) repoSlotMap {
	result := make(repoSlotMap, len(storages))

	// Build a set of current storage names.
	currentNames := make(map[string]struct{}, len(storages))
	for _, s := range storages {
		currentNames[s.Name] = struct{}{}
	}

	// Track which slots are occupied.
	occupied := [maxPGBackRestRepos]bool{}

	// Preserve existing assignments for storages that still exist.
	for name, slot := range existing {
		if _, ok := currentNames[name]; ok && slot >= 0 && slot < maxPGBackRestRepos {
			result[name] = slot
			occupied[slot] = true
		}
	}

	// Assign free slots to new storages (those not yet in the result).
	for _, s := range storages {
		if _, ok := result[s.Name]; ok {
			continue
		}
		// Find the lowest free slot.
		for slot := 0; slot < maxPGBackRestRepos; slot++ {
			if !occupied[slot] {
				result[s.Name] = slot
				occupied[slot] = true
				break
			}
		}
	}

	return result
}

// pgBackRestCredentialSecretName returns a deterministic name for the
// pgBackRest credential Secret derived from the instance and storage names.
func pgBackRestCredentialSecretName(instanceName, storageName string) string {
	return instanceName + "-pgbackrest-" + storageName + "-creds"
}

// ensurePGBackRestCredentialSecret builds a Secret containing pgBackRest-formatted
// S3 credentials and a matching VolumeProjection to mount it. The caller is
// responsible for applying the Secret to the cluster.
func ensurePGBackRestCredentialSecret(
	c *controller.Context,
	repoName string,
	bs *backupv1alpha1.BackupStorage,
) (*corev1.Secret, *corev1.VolumeProjection, error) {
	accessKey, secretKey, err := c.BackupStorageCredentials(bs)
	if err != nil {
		return nil, nil, &controller.BackupConfigError{
			Reason:  "CredentialsUnavailable",
			Message: fmt.Sprintf("cannot read credentials for BackupStorage %q: %v", bs.Name, err),
		}
	}
	if accessKey == "" || secretKey == "" {
		return nil, nil, &controller.BackupConfigError{
			Reason:  "CredentialsUnavailable",
			Message: fmt.Sprintf("BackupStorage %q credentials secret is missing AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY", bs.Name),
		}
	}

	// Build a pgBackRest INI config fragment with the S3 credentials.
	configKey := repoName + "-s3-credentials.conf"
	configData := fmt.Sprintf("[global]\n%s-s3-key=%s\n%s-s3-key-secret=%s\n", repoName, accessKey, repoName, secretKey)

	secretName := pgBackRestCredentialSecretName(c.Instance().Name, repoName)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: c.Instance().Namespace,
		},
		StringData: map[string]string{
			configKey: configData,
		},
	}

	projection := &corev1.VolumeProjection{
		Secret: &corev1.SecretProjection{
			LocalObjectReference: corev1.LocalObjectReference{
				Name: secretName,
			},
		},
	}

	return secret, projection, nil
}

// buildPGBackRestRepo creates a pgBackRest repo configuration from an OpenEverest storage spec.
func buildPGBackRestRepo(
	repoName string,
	storage corev1alpha1.InstanceBackupStorage,
	bs *backupv1alpha1.BackupStorage,
	instanceUID string,
) (upstreamv1beta1.PGBackRestRepo, map[string]string, error) {
	repo := upstreamv1beta1.PGBackRestRepo{
		Name: repoName,
	}
	repoGlobal := make(map[string]string)

	switch bs.Spec.Type {
	case backupv1alpha1.BackupStorageTypeS3:
		if bs.Spec.S3 == nil {
			return repo, nil, &controller.BackupConfigError{Reason: "StorageTypeUnsupported", Message: fmt.Sprintf("BackupStorage %q has type s3 but missing s3 config", bs.Name)}
		}
		bucket := resolveBackupBucket(bs.Spec.S3.Bucket)
		repo.S3 = &upstreamv1beta1.RepoS3{
			Bucket:   bucket,
			Region:   bs.Spec.S3.Region,
			Endpoint: bs.Spec.S3.EndpointURL,
		}
		// Use instance UID as a path prefix so different instances don't
		// collide when sharing the same bucket.
		if instanceUID != "" {
			repoGlobal[repoName+"-path"] = fmt.Sprintf("/pgbackrest/%s/%s", instanceUID, repoName)
		}
	default:
		return repo, nil, &controller.BackupConfigError{Reason: "StorageTypeUnsupported", Message: fmt.Sprintf("BackupStorage %q type %q is not supported; only s3 is supported", bs.Name, bs.Spec.Type)}
	}

	return repo, repoGlobal, nil
}

func buildPGBackRestSchedules(schedules []corev1alpha1.InstanceBackupSchedule) *upstreamv1beta1.PGBackRestBackupSchedules {
	s := &upstreamv1beta1.PGBackRestBackupSchedules{}
	for _, schedule := range schedules {
		if !schedule.Enabled {
			continue
		}
		// Map schedule names to pgBackRest backup types.
		// Default to full backup if not specified.
		cron := schedule.Cron
		switch strings.ToLower(schedule.Name) {
		case "full":
			s.Full = &cron
		case "differential":
			s.Differential = &cron
		case "incremental":
			s.Incremental = &cron
		default:
			// Default unrecognized schedule names to full backup.
			s.Full = &cron
		}
	}
	return s
}

func resolveBackupBucket(storageBucket string) string {
	return strings.Trim(storageBucket, "/")
}

// OperatorBackupType implements controller.BackupMirror (optional).
func (p *Provider) OperatorBackupType() client.Object {
	return &pgv2.PerconaPGBackup{}
}

// pruneUnreferencedStorages removes storages from the Instance spec that have
// no schedules configured AND are not referenced by any existing Backup CR.
// This automatically frees repo slots when a storage is no longer in use.
// Returns true if the Instance was modified (and patched).
func pruneUnreferencedStorages(c *controller.Context) (bool, error) {
	if c.Instance().Spec.Backup == nil || len(c.Instance().Spec.Backup.Storages) == 0 {
		return false, nil
	}

	// List all Backup CRs for this instance.
	backups, err := c.BackupsForInstance()
	if err != nil {
		return false, fmt.Errorf("list backups for storage pruning: %w", err)
	}

	// Build a set of storage names that are still referenced by at least one
	// non-deleted Backup CR (regardless of state — even failed backups hold
	// data on the storage that we shouldn't orphan).
	referencedStorages := make(map[string]struct{})
	for i := range backups {
		if backups[i].DeletionTimestamp.IsZero() {
			referencedStorages[backups[i].Spec.StorageName] = struct{}{}
		}
	}

	// Filter: keep storages that have schedules OR are referenced by backups.
	var kept []corev1alpha1.InstanceBackupStorage
	for _, storage := range c.Instance().Spec.Backup.Storages {
		hasSchedules := len(storage.Schedules) > 0
		_, hasBackups := referencedStorages[storage.Name]
		if hasSchedules || hasBackups {
			kept = append(kept, storage)
		}
	}

	// Nothing to prune.
	if len(kept) == len(c.Instance().Spec.Backup.Storages) {
		return false, nil
	}

	// Patch the Instance to remove unreferenced storages.
	// When ALL storages are pruned, also disable backups to avoid an
	// inconsistent state (enabled=true with zero storages) that would
	// block reconciliation with a BackupConfigError.
	patch := c.Instance().DeepCopy()
	patch.Spec.Backup.Storages = kept
	if len(kept) == 0 {
		patch.Spec.Backup.Enabled = false
	}
	if err := c.Client().Patch(c.Context(), patch, client.MergeFrom(c.Instance())); err != nil {
		return false, fmt.Errorf("patch Instance to prune unreferenced storages: %w", err)
	}

	// Update the in-memory instance so subsequent logic uses the pruned list.
	c.Instance().Spec.Backup.Storages = kept
	if len(kept) == 0 {
		c.Instance().Spec.Backup.Enabled = false
	}

	return true, nil
}

// autoRegisterStorage checks if a BackupStorage resource with the given name
// exists and, if so, adds it to the Instance's storages list. This enables
// Backup CRs to reference storages that haven't been explicitly configured on
// the Instance yet — the provider auto-registers them on first use.
// Returns true if the storage was registered (Instance patched).
func autoRegisterStorage(c *controller.Context, storageName string) (bool, error) {
	// Verify the BackupStorage resource exists.
	if _, err := c.BackupStorage(storageName); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("lookup BackupStorage %q: %w", storageName, err)
	}

	// Ensure the Instance has backup configured.
	if c.Instance().Spec.Backup == nil {
		return false, nil
	}

	// Check we haven't exceeded the max repos limit.
	if len(c.Instance().Spec.Backup.Storages) >= maxPGBackRestRepos {
		return false, nil
	}

	// Add the storage to the Instance.
	newStorage := corev1alpha1.InstanceBackupStorage{
		Name:       storageName,
		StorageRef: corev1.LocalObjectReference{Name: storageName},
	}

	patch := c.Instance().DeepCopy()
	patch.Spec.Backup.Storages = append(patch.Spec.Backup.Storages, newStorage)
	// Ensure backups are enabled so that the next Instance Sync will
	// configure repos on the PG cluster. Without this the storage is
	// registered but applyBackupSettings sees Enabled=false and keeps
	// backups disabled, leaving the backup stuck in Pending forever.
	patch.Spec.Backup.Enabled = true
	if err := c.Client().Patch(c.Context(), patch, client.MergeFrom(c.Instance())); err != nil {
		return false, fmt.Errorf("patch Instance to register storage %q: %w", storageName, err)
	}

	// Update in-memory instance.
	c.Instance().Spec.Backup.Storages = patch.Spec.Backup.Storages
	c.Instance().Spec.Backup.Enabled = true

	return true, nil
}
