package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"strconv"
	"strings"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	"github.com/openeverest/openeverest/v2/api/backup/v1alpha1/storageclasses"
	apicommon "github.com/openeverest/openeverest/v2/api/common/v1alpha1"
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

	// pgBackRestConfigDirectory is where the operator mounts
	// spec.backups.pgbackrest.configuration.
	pgBackRestConfigDirectory = "/etc/pgbackrest/conf.d"
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

	// Skip mirroring when the Instance is being deleted. During cascade
	// deletion the PerconaPGCluster is still running (and pgBackRest
	// schedules may fire new backups) but we must not create new Backup CRs
	// that would delay the Instance cleanup.
	if !instance.GetDeletionTimestamp().IsZero() {
		return nil, nil
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
			Origin: backupv1alpha1.BackupOrigin{
				Type:        backupv1alpha1.BackupOriginTypeInstance,
				InstanceRef: &apicommon.ObjectRef{Name: pgBackup.Spec.PGCluster},
			},
			ClassRef:     apicommon.ObjectRef{Name: instance.Spec.Backup.ClassRef.Name},
			StorageRef:   apicommon.ObjectRef{Name: storageName},
			ScheduleName: scheduleName,
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

	pgCluster.Spec.Backups.PGBackRest.Image = pgBackRestImage

	if len(c.Instance().Spec.Backup.Storages) == 0 {
		return &controller.BackupConfigError{Reason: "NoStoragesConfigured", Message: "spec.backup.enabled=true requires at least one storage"}
	}
	for _, storage := range c.Instance().Spec.Backup.Storages {
		if storage.StorageRef.Name == "" {
			return &controller.BackupConfigError{Reason: "StorageReferenceMissing", Message: fmt.Sprintf("backup storage %q must set storageRef.name", storage.StorageRef.Name)}
		}
	}

	// Build a stable slot assignment: read existing mapping from the annotation
	// on the LIVE PGCluster (not the fresh in-memory one we're building), then
	// preserve slots for storages that still exist and assign free slots to new ones.
	live := &pgv2.PerconaPGCluster{}
	if err := c.Get(live, c.Instance().Name); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("get existing PerconaPGCluster for slot map: %w", err)
	}
	slotMap := reconcileRepoSlotMap(loadRepoSlotMap(live), c.Instance().Spec.Backup.Storages)

	rendered, err := buildPGBackRestRepos(c, live, slotMap)
	if err != nil {
		return err
	}
	for _, secret := range rendered.secrets {
		if err := c.Apply(secret); err != nil {
			return fmt.Errorf("apply pgBackRest credential secret %q: %w", secret.Name, err)
		}
	}

	// The operator requires at least one repo while backups are enabled.
	backupsEnabled := len(rendered.repos) > 0
	pgCluster.Spec.Backups.Enabled = &backupsEnabled

	// Replace repos, configurations and global config entirely so removed
	// repos don't leave stale entries behind.
	pgCluster.Spec.Backups.PGBackRest.Repos = rendered.repos
	pgCluster.Spec.Backups.PGBackRest.Configuration = rendered.configuration
	pgCluster.Spec.Backups.PGBackRest.Global = rendered.global

	// Persist the stable slot map annotation.
	saveRepoSlotMap(pgCluster, slotMap)

	// Clean up credential secrets the configuration no longer mounts.
	for i := 0; i < maxPGBackRestRepos; i++ {
		secretName := pgBackRestCredentialSecretName(c.Instance().Name, pgBackRestRepoName(i))
		if _, mounted := rendered.secretNames[secretName]; mounted {
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
		currentNames[s.StorageRef.Name] = struct{}{}
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
		if _, ok := result[s.StorageRef.Name]; ok {
			continue
		}
		// Find the lowest free slot.
		for slot := 0; slot < maxPGBackRestRepos; slot++ {
			if !occupied[slot] {
				result[s.StorageRef.Name] = slot
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

// pgBackRestRepos is the pgBackRest configuration of an Instance's backup
// storages.
type pgBackRestRepos struct {
	repos         []upstreamv1beta1.PGBackRestRepo
	configuration []corev1.VolumeProjection
	global        map[string]string
	// secrets are the credential Secrets to apply.
	secrets []*corev1.Secret
	// secretNames are the credential Secrets configuration mounts.
	secretNames map[string]struct{}
}

// buildPGBackRestRepos renders every spec.backup.storages entry as the
// pgBackRest repo of its slot. An entry that must not be rendered keeps the
// repo, repoN-* options and credentials projection the live cluster has for
// it, and is left out while the live cluster has none.
func buildPGBackRestRepos(c *controller.Context, live *pgv2.PerconaPGCluster, slotMap repoSlotMap) (pgBackRestRepos, error) {
	storages, err := c.BackupStorages()
	if err != nil {
		return pgBackRestRepos{}, fmt.Errorf("resolve backup storages: %w", err)
	}
	liveSlots := loadRepoSlotMap(live)
	out := pgBackRestRepos{global: map[string]string{}, secretNames: map[string]struct{}{}}
	for _, s := range storages {
		slot, ok := slotMap[s.Name()]
		if !ok {
			return pgBackRestRepos{}, &controller.BackupConfigError{Reason: "SlotAssignmentFailed", Message: fmt.Sprintf("no repo slot assigned for storage %q", s.Name())}
		}
		repoName := pgBackRestRepoName(slot)
		secretName := pgBackRestCredentialSecretName(c.Name(), repoName)

		if s.Apply() {
			r, err := buildPGBackRestS3Repo(c, s.Storage, repoName, s.Path, secretName)
			if err == nil {
				if len(s.Entry.Schedules) > 0 {
					r.repo.BackupSchedules = buildPGBackRestSchedules(s.Entry.Schedules)
					if err := applyRetentionConfig(r.global, repoName, s.Entry.Schedules); err != nil {
						return pgBackRestRepos{}, &controller.BackupConfigError{Reason: "RetentionTypeUnsupported", Message: err.Error()}
					}
				}
				out.repos = append(out.repos, r.repo)
				maps.Copy(out.global, r.global)
				out.configuration = append(out.configuration, r.projection)
				out.secrets = append(out.secrets, r.secret)
				out.secretNames[secretName] = struct{}{}
				continue
			}
			c.RejectBackupStorage(s.Name(), err)
		}

		if liveSlot, onLive := liveSlots[s.Name()]; onLive && liveSlot == slot {
			out.hold(live, repoName, secretName)
		}
	}
	return out, nil
}

// hold copies what the live cluster has for the repo repoName.
func (r *pgBackRestRepos) hold(live *pgv2.PerconaPGCluster, repoName, secretName string) {
	pgBackRest := live.Spec.Backups.PGBackRest
	for _, repo := range pgBackRest.Repos {
		if repo.Name == repoName {
			r.repos = append(r.repos, repo)
		}
	}
	for k, v := range pgBackRest.Global {
		if strings.HasPrefix(k, repoName+"-") {
			r.global[k] = v
		}
	}
	for _, projection := range pgBackRest.Configuration {
		if projection.Secret != nil && projection.Secret.Name == secretName {
			r.configuration = append(r.configuration, projection)
			r.secretNames[secretName] = struct{}{}
		}
	}
}

// pgBackRestS3Repo is a pgBackRest repo on an s3 BackupStorage, with the
// Secret pgBackRest reads its credentials and CA from.
type pgBackRestS3Repo struct {
	repo       upstreamv1beta1.PGBackRestRepo
	global     map[string]string
	secret     *corev1.Secret
	projection corev1.VolumeProjection
}

// buildPGBackRestS3Repo renders the s3 BackupStorage bs as the pgBackRest
// repo repoName, writing under path in the bucket and reading its
// credentials and CA from the Secret secretName.
func buildPGBackRestS3Repo(c *controller.Context, bs *backupv1alpha1.BackupStorage, repoName, path, secretName string) (pgBackRestS3Repo, error) {
	s3, creds, err := c.BackupStorageS3(bs)
	if err != nil {
		return pgBackRestS3Repo{}, err
	}
	if s3.EffectiveImplementation() == storageclasses.S3ImplementationGCS {
		return pgBackRestS3Repo{}, fmt.Errorf("pgBackRest cannot use GCS through its S3 API: %w", controller.ErrImplementationUnsupported)
	}
	endpoint, err := s3.Endpoint()
	if err != nil {
		return pgBackRestS3Repo{}, err
	}
	if !endpoint.Secure {
		return pgBackRestS3Repo{}, fmt.Errorf("pgBackRest cannot reach S3 over plain HTTP: %w", storageclasses.ErrUnsupportedParameter)
	}
	tls, err := c.BackupStorageTLS(bs, controller.TLSSupport{CustomCA: true, SkipVerify: true})
	if err != nil {
		return pgBackRestS3Repo{}, err
	}

	host := endpoint.HostPort()
	// pgBackRest connects to port 443 unless the endpoint names another.
	if endpoint.Port == 443 && !strings.Contains(endpoint.Host, ":") {
		host = endpoint.Host
	}
	r := pgBackRestS3Repo{
		repo: upstreamv1beta1.PGBackRestRepo{
			Name: repoName,
			S3: &upstreamv1beta1.RepoS3{
				Bucket:   bs.Spec.Location.Bucket,
				Endpoint: host,
				Region:   s3.Region,
			},
		},
		global: map[string]string{repoName + "-path": "/" + path},
	}
	if s3.ForcePathStyle {
		r.global[repoName+"-s3-uri-style"] = "path"
	}
	if tls.InsecureSkipVerify {
		r.global[repoName+"-storage-verify-tls"] = "n"
	}

	data := map[string]string{
		repoName + "-s3-credentials.conf": fmt.Sprintf("[global]\n%s-s3-key=%s\n%s-s3-key-secret=%s\n",
			repoName, creds.AccessKeyID, repoName, creds.SecretAccessKey),
	}
	if tls.CA != nil {
		caKey := repoName + "-storage-ca.crt"
		data[caKey] = string(tls.CA)
		r.global[repoName+"-storage-ca-file"] = pgBackRestConfigDirectory + "/" + caKey
	}
	r.secret = &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: c.Namespace()},
		StringData: data,
	}
	r.projection = corev1.VolumeProjection{
		Secret: &corev1.SecretProjection{LocalObjectReference: corev1.LocalObjectReference{Name: secretName}},
	}
	return r, nil
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
		case "diff":
			s.Differential = &cron
		case "incr":
			s.Incremental = &cron
		default:
			// Default unrecognized schedule names to full backup.
			s.Full = &cron
		}
	}
	return s
}

// applyRetentionConfig sets pgBackRest retention global config keys based on
// the Retention field of each schedule. pgBackRest supports per-repo
// retention via global config keys:
//
//	repo<N>-retention-full      = full backups (count) or days (time) to retain
//	repo<N>-retention-full-type = "count" or "time"
//	repo<N>-retention-diff      = number of differential backups to retain (count only)
//
// When Retention is unset, no retention key is emitted and pgBackRest
// keeps all backups (its default).
func applyRetentionConfig(globalConfig map[string]string, repoName string, schedules []corev1alpha1.InstanceBackupSchedule) error {
	for _, schedule := range schedules {
		if !schedule.Enabled || schedule.Retention == nil {
			continue
		}
		value, retentionType, err := pgBackRestRetention(schedule.Retention)
		if err != nil {
			return fmt.Errorf("schedule %q: %w", schedule.Name, err)
		}
		switch strings.ToLower(schedule.Name) {
		case "full", "":
			globalConfig[repoName+"-retention-full"] = value
			globalConfig[repoName+"-retention-full-type"] = retentionType
		case "diff":
			if retentionType != "count" {
				return fmt.Errorf("schedule %q: pgBackRest only supports count retention for differential backups", schedule.Name)
			}
			globalConfig[repoName+"-retention-diff"] = value
		case "incr":
			// pgBackRest does not have a separate retention key for
			// incremental backups — incremental retention is tied to the
			// full backup retention. Setting full retention here ensures
			// that old incremental chains are cleaned up when the parent
			// full backup expires.
			globalConfig[repoName+"-retention-full"] = value
			globalConfig[repoName+"-retention-full-type"] = retentionType
		default:
			// Unrecognized schedule names default to full backup type.
			globalConfig[repoName+"-retention-full"] = value
			globalConfig[repoName+"-retention-full-type"] = retentionType
		}
	}
	return nil
}

var retentionDurationPattern = regexp.MustCompile(`^([1-9][0-9]*)([dwm])$`)

// daysPerRetentionUnit counts a month as 31 days so a time window is never
// shorter than requested; pgBackRest only expresses time retention in days.
var daysPerRetentionUnit = map[string]int{"d": 1, "w": 7, "m": 31}

// pgBackRestRetention returns the repo-retention value and type for r.
func pgBackRestRetention(r *corev1alpha1.BackupScheduleRetention) (value, retentionType string, err error) {
	switch r.Type {
	case corev1alpha1.BackupScheduleRetentionTypeCount:
		return strconv.Itoa(int(*r.Count)), "count", nil
	case corev1alpha1.BackupScheduleRetentionTypeTime:
		m := retentionDurationPattern.FindStringSubmatch(r.Duration)
		if m == nil {
			return "", "", fmt.Errorf("invalid retention duration %q", r.Duration)
		}
		n, err := strconv.Atoi(m[1])
		if err != nil {
			return "", "", fmt.Errorf("invalid retention duration %q: %w", r.Duration, err)
		}
		return strconv.Itoa(n * daysPerRetentionUnit[m[2]]), "time", nil
	default:
		return "", "", fmt.Errorf("unsupported retention type %q", r.Type)
	}
}

// OperatorBackupType implements controller.BackupMirror (optional).
func (p *Provider) OperatorBackupType() client.Object {
	return &pgv2.PerconaPGBackup{}
}

// pruneUnreferencedStorages removes storages from the Instance spec that have
// no schedules configured, are not referenced by any existing Backup CR, and
// are not the storage spec.dataSource depends on. This automatically frees
// repo slots when a storage is no longer in use. Returns true if the Instance
// was modified (and patched).
func pruneUnreferencedStorages(c *controller.Context) (bool, error) {
	if c.Instance().Spec.Backup == nil || len(c.Instance().Spec.Backup.Storages) == 0 {
		return false, nil
	}
	var dataSourceStorage string
	if ds := c.Instance().Spec.DataSource; ds != nil {
		name, err := dataSourceStorageName(c, ds)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, fmt.Errorf("resolve data source storage for pruning: %w", err)
		}
		dataSourceStorage = name
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
			referencedStorages[backups[i].Spec.StorageRef.Name] = struct{}{}
		}
	}

	// Filter: keep storages that have schedules, are referenced by backups,
	// or are what spec.dataSource reads from.
	var kept []corev1alpha1.InstanceBackupStorage
	for _, storage := range c.Instance().Spec.Backup.Storages {
		hasSchedules := len(storage.Schedules) > 0
		_, hasBackups := referencedStorages[storage.StorageRef.Name]
		isDataSourceStorage := dataSourceStorage != "" && storage.StorageRef.Name == dataSourceStorage
		if hasSchedules || hasBackups || isDataSourceStorage {
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
		return false, fmt.Errorf("cannot register storage %q: instance already has the maximum number of backup repositories (%d)", storageName, maxPGBackRestRepos)
	}

	// Add the storage to the Instance.
	newStorage := corev1alpha1.InstanceBackupStorage{
		StorageRef: apicommon.ObjectRef{Name: storageName},
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
