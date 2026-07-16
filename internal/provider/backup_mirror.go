package provider

import (
	"context"
	"errors"
	"fmt"
	"strings"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	upstreamv1beta1 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/upstream.pgv2.percona.com/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Compile-time interface checks.
var _ controller.BackupMirror = (*Provider)(nil)

const (
	// pgBackrestJobTypeCron is the annotation value for scheduled backups.
	pgBackrestJobTypeCron = "backup"
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

	// Determine if this is a scheduled backup by checking the job-type annotation.
	scheduleName := scheduleNameFromAnnotations(pgBackup)
	if scheduleName == "" {
		// Not a scheduled backup; skip mirroring.
		return nil, nil
	}

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
			StorageName:     repoName,
			ScheduleName:    scheduleName,
		},
	}, nil
}

// scheduleNameFromAnnotations extracts the schedule name from PG backup annotations.
// PG operator scheduled backups set the job-type annotation.
func scheduleNameFromAnnotations(pgBackup *pgv2.PerconaPGBackup) string {
	if pgBackup.Annotations == nil {
		return ""
	}

	// The PG operator uses the "percona.com/backup-job-type" annotation for scheduled backups.
	jobType := pgBackup.Annotations[pgv2.PGBackrestAnnotationJobType]
	if jobType == "" {
		return ""
	}

	// Use the backup name from the annotation as the schedule name, or derive from the backup name.
	backupName := pgBackup.Annotations[pgv2.PGBackrestAnnotationBackupName]
	if backupName != "" {
		return backupName
	}

	return jobType
}

func applyBackupSettings(c *controller.Context, pgCluster *pgv2.PerconaPGCluster) error {
	if c.Instance().Spec.Backup == nil || !c.Instance().Spec.Backup.Enabled {
		backupsEnabled := false
		pgCluster.Spec.Backups.Enabled = &backupsEnabled
		pgCluster.Spec.Backups.PGBackRest.Repos = nil
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

	var repos []upstreamv1beta1.PGBackRestRepo
	for _, storage := range c.Instance().Spec.Backup.Storages {
		if storage.StorageRef.Name == "" {
			return &controller.BackupConfigError{Reason: "StorageReferenceMissing", Message: fmt.Sprintf("backup storage %q must set storageRef.name", storage.Name)}
		}

		bs, err := c.BackupStorage(storage.StorageRef.Name)
		if err != nil {
			return &controller.BackupConfigError{Reason: "StorageNotFound", Message: err.Error()}
		}

		repo, err := buildPGBackRestRepo(storage, bs, string(c.Instance().UID))
		if err != nil {
			return err
		}

		// Apply schedules to the repo.
		if len(storage.Schedules) > 0 {
			schedules := buildPGBackRestSchedules(storage.Schedules)
			repo.BackupSchedules = schedules
		}

		repos = append(repos, repo)
	}

	if len(c.Instance().Spec.Backup.Storages) == 0 {
		return &controller.BackupConfigError{Reason: "NoStoragesConfigured", Message: "spec.backup.enabled=true requires at least one storage"}
	}

	pgCluster.Spec.Backups.PGBackRest.Repos = repos

	return nil
}

// buildPGBackRestRepo creates a pgBackRest repo configuration from an OpenEverest storage spec.
func buildPGBackRestRepo(
	storage corev1alpha1.InstanceBackupStorage,
	bs *backupv1alpha1.BackupStorage,
	instanceUID string,
) (upstreamv1beta1.PGBackRestRepo, error) {
	repo := upstreamv1beta1.PGBackRestRepo{
		Name: storage.Name,
	}

	switch bs.Spec.Type {
	case backupv1alpha1.BackupStorageTypeS3:
		if bs.Spec.S3 == nil {
			return repo, &controller.BackupConfigError{Reason: "StorageTypeUnsupported", Message: fmt.Sprintf("BackupStorage %q has type s3 but missing s3 config", bs.Name)}
		}
		bucket := resolveBackupBucket(bs.Spec.S3.Bucket, instanceUID)
		repo.S3 = &upstreamv1beta1.RepoS3{
			Bucket:   bucket,
			Region:   bs.Spec.S3.Region,
			Endpoint: bs.Spec.S3.EndpointURL,
		}
	default:
		return repo, &controller.BackupConfigError{Reason: "StorageTypeUnsupported", Message: fmt.Sprintf("BackupStorage %q type %q is not supported; only s3 is supported", bs.Name, bs.Spec.Type)}
	}

	return repo, nil
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

func resolveBackupBucket(storageBucket, instanceUID string) string {
	bucket := strings.Trim(storageBucket, "/")
	if bucket != "" && instanceUID != "" && !strings.Contains(bucket, "/") {
		return fmt.Sprintf("%s/%s", bucket, instanceUID)
	}
	return bucket
}

// OperatorBackupType implements controller.BackupMirror (optional).
func (p *Provider) OperatorBackupType() client.Object {
	return &pgv2.PerconaPGBackup{}
}
