package provider

import (
	"context"
	"testing"
	"time"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	apicommon "github.com/openeverest/openeverest/v2/api/common/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	upstreamv1beta1 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/upstream.pgv2.percona.com/v1beta1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestResolvePointInTimeSource(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	require.NoError(t, pgv2.AddToScheme(scheme))

	instance := &corev1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "pg-prod", Namespace: "everest"},
		Spec: corev1alpha1.InstanceSpec{
			ProviderRef: apicommon.ObjectRef{Name: "provider-percona-postgresql"},
			Backup: &corev1alpha1.InstanceBackupSpec{
				Enabled:  true,
				ClassRef: apicommon.ObjectRef{Name: "pg"},
				Storages: []corev1alpha1.InstanceBackupStorage{
					{
						StorageRef: apicommon.ObjectRef{Name: "minio-primary"},
						PITR:       &corev1alpha1.InstanceBackupStoragePITR{Enabled: true},
					},
				},
			},
		},
	}
	pgCluster := &pgv2.PerconaPGCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pg-prod",
			Namespace: "everest",
			Annotations: map[string]string{
				repoSlotMapAnnotation: `{"minio-primary":0}`,
			},
		},
		Spec: pgv2.PerconaPGClusterSpec{
			Backups: pgv2.Backups{
				PGBackRest: pgv2.PGBackRestArchive{
					Repos: []upstreamv1beta1.PGBackRestRepo{{Name: "repo1"}},
				},
			},
		},
	}

	mkRestore := func(pitr *backupv1alpha1.DataSourcePointInTime) *backupv1alpha1.Restore {
		return &backupv1alpha1.Restore{
			ObjectMeta: metav1.ObjectMeta{Name: "r1", Namespace: "everest"},
			Spec: backupv1alpha1.RestoreSpec{
				InstanceRef: apicommon.ObjectRef{Name: "pg-prod"},
				DataSource: backupv1alpha1.DataSource{
					Type:        backupv1alpha1.DataSourceTypePointInTime,
					PointInTime: pitr,
				},
			},
		}
	}
	newCtx := func() *controller.Context {
		inst := instance.DeepCopy()
		pg := pgCluster.DeepCopy()
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(inst, pg).Build()
		return controller.NewContext(context.Background(), k8sClient, inst, "provider-percona-postgresql")
	}

	t.Run("date target maps storage to repo and emits offset", func(t *testing.T) {
		t.Parallel()

		date := metav1.NewTime(time.Date(2026, 8, 1, 15, 30, 0, 0, time.FixedZone("CEST", 2*3600)))
		repo, options, pending, err := resolvePointInTimeSource(newCtx(), mkRestore(&backupv1alpha1.DataSourcePointInTime{
			Source:         backupv1alpha1.StreamSource{StorageRef: apicommon.ObjectRef{Name: "minio-primary"}},
			RecoveryTarget: backupv1alpha1.RecoveryTargetDate,
			Date:           &date,
		}), nil)

		require.NoError(t, err)
		require.Nil(t, pending)
		require.NotNil(t, repo)
		assert.Equal(t, "repo1", *repo)
		require.Len(t, options, 2)
		assert.Equal(t, "--type=time", options[0])
		// 15:30 +02:00 is 13:30 UTC; the offset must be explicit.
		assert.Equal(t, `--target="2026-08-01 13:30:00+00:00"`, options[1])
	})

	t.Run("latest target needs no options", func(t *testing.T) {
		t.Parallel()

		repo, options, pending, err := resolvePointInTimeSource(newCtx(), mkRestore(&backupv1alpha1.DataSourcePointInTime{
			Source:         backupv1alpha1.StreamSource{StorageRef: apicommon.ObjectRef{Name: "minio-primary"}},
			RecoveryTarget: backupv1alpha1.RecoveryTargetLatest,
		}), nil)

		require.NoError(t, err)
		require.Nil(t, pending)
		require.NotNil(t, repo)
		assert.Equal(t, "repo1", *repo)
		assert.Empty(t, options)
	})

	t.Run("another instance's stream is rejected", func(t *testing.T) {
		t.Parallel()

		_, _, pending, err := resolvePointInTimeSource(newCtx(), mkRestore(&backupv1alpha1.DataSourcePointInTime{
			Source: backupv1alpha1.StreamSource{
				InstanceRef: &apicommon.ObjectRef{Name: "pg-other"},
				StorageRef:  apicommon.ObjectRef{Name: "minio-primary"},
			},
			RecoveryTarget: backupv1alpha1.RecoveryTargetLatest,
		}), nil)

		require.NoError(t, err)
		require.NotNil(t, pending)
		assert.Equal(t, backupv1alpha1.RestoreStateFailed, pending.State)
	})

	t.Run("unregistered storage waits", func(t *testing.T) {
		t.Parallel()

		_, _, pending, err := resolvePointInTimeSource(newCtx(), mkRestore(&backupv1alpha1.DataSourcePointInTime{
			Source:         backupv1alpha1.StreamSource{StorageRef: apicommon.ObjectRef{Name: "unknown"}},
			RecoveryTarget: backupv1alpha1.RecoveryTargetLatest,
		}), nil)

		require.NoError(t, err)
		require.NotNil(t, pending)
		assert.Equal(t, backupv1alpha1.RestoreStatePending, pending.State)
	})
}

func TestResolveBackupSource(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, backupv1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	require.NoError(t, pgv2.AddToScheme(scheme))

	repo1 := "repo1"
	sourceInstance := &corev1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "pg-src", Namespace: "everest"},
	}
	destInstance := &corev1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "pg-dest", Namespace: "everest"},
	}
	sourceBackup := &backupv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "src-backup", Namespace: "everest"},
		Spec: backupv1alpha1.BackupSpec{
			InstanceRef: apicommon.ObjectRef{Name: "pg-src"},
			StorageRef:  apicommon.ObjectRef{Name: "minio"},
		},
		Status: backupv1alpha1.BackupStatus{State: backupv1alpha1.BackupStateSucceeded},
	}
	opBackup := &pgv2.PerconaPGBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "src-backup", Namespace: "everest"},
		Spec:       pgv2.PerconaPGBackupSpec{PGCluster: "pg-src", RepoName: &repo1},
		Status:     pgv2.PerconaPGBackupStatus{State: pgv2.BackupSucceeded, BackupName: "20260903-090125F"},
	}

	mkRestore := func(instance string) *backupv1alpha1.Restore {
		return &backupv1alpha1.Restore{
			ObjectMeta: metav1.ObjectMeta{Name: "r1", Namespace: "everest"},
			Spec: backupv1alpha1.RestoreSpec{
				InstanceRef: apicommon.ObjectRef{Name: instance},
				DataSource: backupv1alpha1.DataSource{
					Type:   backupv1alpha1.DataSourceTypeBackup,
					Backup: &backupv1alpha1.DataSourceBackup{BackupRef: apicommon.ObjectRef{Name: "src-backup"}},
				},
			},
		}
	}
	newCtx := func(objects ...client.Object) *controller.Context {
		copies := make([]client.Object, len(objects))
		for i, obj := range objects {
			copies[i] = obj.DeepCopyObject().(client.Object)
		}
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(copies...).Build()
		return controller.NewContext(context.Background(), k8sClient, destInstance.DeepCopy(), "provider-percona-postgresql")
	}

	t.Run("same instance pins backup set", func(t *testing.T) {
		t.Parallel()

		repo, options, pending, err := resolveBackupSource(newCtx(
			sourceInstance, sourceBackup, opBackup,
		), mkRestore("pg-src"), nil)

		require.NoError(t, err)
		require.Nil(t, pending)
		require.NotNil(t, repo)
		assert.Equal(t, "repo1", *repo)
		assert.Equal(t, []string{"--set=20260903-090125F", "--type=immediate"}, options)
	})

	t.Run("other instance is not supported", func(t *testing.T) {
		t.Parallel()

		_, _, pending, err := resolveBackupSource(newCtx(
			sourceInstance, destInstance, sourceBackup, opBackup,
		), mkRestore("pg-dest"), nil)

		require.NoError(t, err)
		require.NotNil(t, pending)
		assert.Equal(t, backupv1alpha1.RestoreStateFailed, pending.State)
		assert.Contains(t, pending.Message, "not supported")
	})
}
