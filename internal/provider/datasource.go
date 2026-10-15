package provider

import (
	"fmt"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
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

// pgBackRestStanza is the stanza the operator creates for every cluster.
const pgBackRestStanza = "db"

// dataSourceBootstrap is what a new cluster needs to be created from another
// Instance's pgBackRest repo.
type dataSourceBootstrap struct {
	DataSource       *upstreamv1beta1.DataSource
	CredentialSecret *corev1.Secret
}

// isSeedingRestore reports whether the Restore is the one the runtime creates
// to seed the Instance from its spec.dataSource.
func isSeedingRestore(c *controller.Context, restore *backupv1alpha1.Restore) bool {
	return c.Instance().Spec.DataSource != nil &&
		restore.Name == c.Name()+controller.DataSourceRestoreNameSuffix
}

// dataSourceSourceInstance returns the name of the Instance whose pgBackRest
// repo holds the data, or "" when it cannot be told yet.
func dataSourceSourceInstance(c *controller.Context, ds *backupv1alpha1.DataSource) (string, error) {
	switch ds.Type {
	case backupv1alpha1.DataSourceTypeBackup:
		if ds.Backup == nil {
			return "", nil
		}
		backup := &backupv1alpha1.Backup{}
		if err := c.Get(backup, ds.Backup.BackupRef.Name); err != nil {
			if apierrors.IsNotFound(err) {
				return "", nil
			}
			return "", fmt.Errorf("get source Backup %q: %w", ds.Backup.BackupRef.Name, err)
		}
		if backup.Spec.Origin.InstanceRef == nil {
			return "", nil
		}
		return backup.Spec.Origin.InstanceRef.Name, nil
	case backupv1alpha1.DataSourceTypePointInTime:
		if ds.PointInTime == nil || ds.PointInTime.Source.InstanceRef == nil {
			return "", nil
		}
		return ds.PointInTime.Source.InstanceRef.Name, nil
	default:
		return "", nil
	}
}

// dataSourceStorageName returns the BackupStorage the source data lives in.
func dataSourceStorageName(c *controller.Context, ds *backupv1alpha1.DataSource) (string, error) {
	if ds.Type == backupv1alpha1.DataSourceTypePointInTime {
		return ds.PointInTime.Source.StorageRef.Name, nil
	}
	backup := &backupv1alpha1.Backup{}
	if err := c.Get(backup, ds.Backup.BackupRef.Name); err != nil {
		return "", fmt.Errorf("get source Backup %q: %w", ds.Backup.BackupRef.Name, err)
	}
	return backup.Spec.StorageRef.Name, nil
}

// buildDataSourceBootstrap describes how the operator creates the cluster from
// the source Instance's repo. The source repo is only read by the bootstrap
// restore and is deliberately not registered on the new cluster, because the
// new cluster's archive-push would write into it.
//
// A nil bootstrap with a nil status means the Instance has no data source, or
// seeds from itself. A non-nil status means the source is not usable yet.
func buildDataSourceBootstrap(c *controller.Context) (*dataSourceBootstrap, *controller.RestoreExecutionStatus, error) {
	ds := c.Instance().Spec.DataSource
	if ds == nil {
		return nil, nil, nil
	}

	seeding := &backupv1alpha1.Restore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      c.Name() + controller.DataSourceRestoreNameSuffix,
			Namespace: c.Namespace(),
		},
		Spec: backupv1alpha1.RestoreSpec{
			InstanceRef: apicommon.ObjectRef{Name: c.Name()},
			DataSource:  *ds,
		},
	}

	repoName, options, pending, err := resolveRestoreSource(c, seeding, nil)
	if err != nil || pending != nil {
		return nil, pending, err
	}

	sourceName, err := dataSourceSourceInstance(c, ds)
	if err != nil {
		return nil, nil, err
	}
	if sourceName == "" || sourceName == c.Name() {
		return nil, nil, nil
	}
	sourceInstance := &corev1alpha1.Instance{}
	if err := c.Get(sourceInstance, sourceName); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, &controller.RestoreExecutionStatus{
				State:   backupv1alpha1.RestoreStatePending,
				Message: fmt.Sprintf("Waiting for source Instance %q", sourceName),
			}, nil
		}
		return nil, nil, fmt.Errorf("get source Instance %q: %w", sourceName, err)
	}

	storageName, err := dataSourceStorageName(c, ds)
	if err != nil {
		return nil, nil, err
	}
	bs, err := c.BackupStorage(storageName)
	if err != nil {
		return nil, nil, err
	}

	repo, global, err := buildPGBackRestRepo(*repoName, corev1alpha1.InstanceBackupStorage{}, bs, string(sourceInstance.UID))
	if err != nil {
		return nil, nil, err
	}

	source := &upstreamv1beta1.PGBackRestDataSource{
		Repo:    repo,
		Stanza:  pgBackRestStanza,
		Options: options,
		Global:  global,
	}

	var secret *corev1.Secret
	if bs.Spec.S3 != nil {
		secret, source.Configuration, err = dataSourceCredentials(c, *repoName, bs)
		if err != nil {
			return nil, nil, err
		}
		if bs.Spec.S3.ForcePathStyle != nil && *bs.Spec.S3.ForcePathStyle {
			source.Global[*repoName+"-s3-uri-style"] = "path"
		}
		if bs.Spec.S3.VerifyTLS != nil && !*bs.Spec.S3.VerifyTLS {
			source.Global[*repoName+"-storage-verify-tls"] = "n"
		}
	}

	return &dataSourceBootstrap{
		DataSource:       &upstreamv1beta1.DataSource{PGBackRest: source},
		CredentialSecret: secret,
	}, nil, nil
}

// dataSourceCredentials builds the Secret that holds the source storage
// credentials. It is named apart from the cluster's own repo credentials,
// which can use the same repo name.
func dataSourceCredentials(
	c *controller.Context,
	repoName string,
	bs *backupv1alpha1.BackupStorage,
) (*corev1.Secret, []corev1.VolumeProjection, error) {
	secret, projection, err := ensurePGBackRestCredentialSecret(c, repoName, bs)
	if err != nil {
		return nil, nil, err
	}
	secret.Name = c.Name() + "-pgbackrest-datasource-creds"
	projection.Secret.Name = secret.Name
	return secret, []corev1.VolumeProjection{*projection}, nil
}

// applyDataSourceBootstrap makes a new cluster start from the source repo. An
// existing cluster keeps what it was created with: the operator only reads the
// data source at creation, and the source may be gone by then.
func applyDataSourceBootstrap(c *controller.Context, cluster *pgv2.PerconaPGCluster) error {
	existing := &pgv2.PerconaPGCluster{}
	err := c.Get(existing, c.Name())
	if err == nil {
		cluster.Spec.DataSource = existing.Spec.DataSource
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get existing PerconaPGCluster for data source: %w", err)
	}

	bootstrap, pending, err := buildDataSourceBootstrap(c)
	if err != nil {
		return err
	}
	if pending != nil {
		// Creating the cluster now would seed an empty database.
		return fmt.Errorf("data source is not ready: %s", pending.Message)
	}
	if bootstrap == nil {
		return nil
	}
	if bootstrap.CredentialSecret != nil {
		if err := c.Apply(bootstrap.CredentialSecret); err != nil {
			return fmt.Errorf("apply data source credential secret: %w", err)
		}
	}
	cluster.Spec.DataSource = bootstrap.DataSource
	return nil
}

// seedingRestoreStatus reports the seeding of an Instance from another
// Instance's repo. The operator performs it while creating the cluster, so
// there is no PerconaPGRestore to follow and the cluster state stands in for it.
func seedingRestoreStatus(
	c *controller.Context,
	restore *backupv1alpha1.Restore,
	opRef *apicommon.TypedObjectRef,
) (controller.RestoreExecutionStatus, error) {
	out := controller.RestoreExecutionStatus{OperatorRestoreRef: opRef}

	cluster := &pgv2.PerconaPGCluster{}
	if err := c.Client().Get(c.Context(), client.ObjectKey{Namespace: restore.Namespace, Name: restore.Spec.InstanceRef.Name}, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			out.State = backupv1alpha1.RestoreStatePending
			out.Message = fmt.Sprintf("Waiting for PerconaPGCluster %q", restore.Spec.InstanceRef.Name)
			return out, nil
		}
		return out, fmt.Errorf("get PerconaPGCluster %q: %w", restore.Spec.InstanceRef.Name, err)
	}

	if cluster.Status.State == pgv2.AppStateReady {
		out.State = backupv1alpha1.RestoreStateSucceeded
		out.Message = "Restore completed"
		now := metav1.Now()
		out.CompletedAt = &now
		return out, nil
	}
	out.State = backupv1alpha1.RestoreStateRunning
	out.Message = "Restore is running"
	return out, nil
}
