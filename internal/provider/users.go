package provider

import (
	"fmt"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	upstreamv1beta1 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/upstream.pgv2.percona.com/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// restoredFromInstanceAnnotation records the source Instance name after a
// successful restore from another cluster. pgBackRest restore keeps the
// source's database name (the source cluster name) and roles; the dest
// cluster's default database is gone. The annotation lets us keep pointing
// the dest user at that restored database even if the source Backup CR is
// later deleted.
const restoredFromInstanceAnnotation = "openeverest.io/restored-from-instance"

// applyPostRestoreUsers rewires spec.users after a restore from another
// Instance. The PG operator hashes user/database SQL and will not re-run it
// after an in-place restore, so the dest role and dest-named database never
// come back. Changing spec.users busts that hash: the dest user is recreated
// with the dest password, granted access to the restored (source-named)
// database, and the user secret URI is updated to that database.
func applyPostRestoreUsers(c *controller.Context, cluster *pgv2.PerconaPGCluster) error {
	sourceName, err := restoredSourceInstanceName(c, cluster)
	if err != nil {
		return err
	}
	if sourceName == "" || sourceName == c.Name() {
		return nil
	}

	if cluster.Annotations == nil {
		cluster.Annotations = map[string]string{}
	}
	cluster.Annotations[restoredFromInstanceAnnotation] = sourceName
	cluster.Spec.Users = usersForRestoredSource(c.Name(), sourceName)
	return nil
}

func restoredSourceInstanceName(c *controller.Context, cluster *pgv2.PerconaPGCluster) (string, error) {
	if name := cluster.Annotations[restoredFromInstanceAnnotation]; name != "" {
		return name, nil
	}

	existing := &pgv2.PerconaPGCluster{}
	if err := c.Get(existing, c.Name()); err != nil {
		if apierrors.IsNotFound(err) {
			return sourceInstanceFromSucceededRestores(c)
		}
		return "", fmt.Errorf("get existing PerconaPGCluster for restored users: %w", err)
	}
	if name := existing.Annotations[restoredFromInstanceAnnotation]; name != "" {
		return name, nil
	}
	return sourceInstanceFromSucceededRestores(c)
}

func sourceInstanceFromSucceededRestores(c *controller.Context) (string, error) {
	restores, err := c.RestoresForInstance()
	if err != nil {
		return "", fmt.Errorf("list restores for post-restore users: %w", err)
	}

	var latest *backupv1alpha1.Restore
	for i := range restores {
		r := &restores[i]
		if r.Status.State != backupv1alpha1.RestoreStateSucceeded {
			continue
		}
		if r.Spec.DataSource.Type != backupv1alpha1.DataSourceTypeBackup || r.Spec.DataSource.Backup == nil {
			continue
		}
		if latest == nil {
			latest = r
			continue
		}
		if r.Status.CompletedAt != nil && (latest.Status.CompletedAt == nil || r.Status.CompletedAt.After(latest.Status.CompletedAt.Time)) {
			latest = r
		}
	}
	if latest == nil {
		return "", nil
	}

	backup := &backupv1alpha1.Backup{}
	if err := c.Get(backup, latest.Spec.DataSource.Backup.BackupRef.Name); err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", fmt.Errorf("get source Backup %q for post-restore users: %w", latest.Spec.DataSource.Backup.BackupRef.Name, err)
	}
	return backup.Spec.InstanceRef.Name, nil
}

func usersForRestoredSource(clusterName, sourceName string) []upstreamv1beta1.PostgresUserSpec {
	grant := true
	return []upstreamv1beta1.PostgresUserSpec{{
		Name: upstreamv1beta1.PostgresIdentifier(clusterName),
		Databases: []upstreamv1beta1.PostgresIdentifier{
			upstreamv1beta1.PostgresIdentifier(sourceName),
		},
		GrantPublicSchemaAccess: &grant,
	}}
}
