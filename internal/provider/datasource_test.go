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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// seedingFixture is a source Instance with one succeeded backup and a PITR
// stream, plus the not yet created Instance that is seeded from them.
type seedingFixture struct {
	scheme  *runtime.Scheme
	objects []client.Object
}

func newSeedingFixture(t *testing.T) seedingFixture {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, backupv1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	require.NoError(t, pgv2.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	repo1 := "repo1"
	forcePathStyle := true
	return seedingFixture{
		scheme: scheme,
		objects: []client.Object{
			&corev1alpha1.Instance{
				ObjectMeta: metav1.ObjectMeta{Name: "pg-src", Namespace: "everest", UID: types.UID("src-uid")},
			},
			&pgv2.PerconaPGCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "pg-src",
					Namespace:   "everest",
					Annotations: map[string]string{repoSlotMapAnnotation: `{"minio":0}`},
				},
				Spec: pgv2.PerconaPGClusterSpec{
					Backups: pgv2.Backups{
						PGBackRest: pgv2.PGBackRestArchive{
							Repos: []upstreamv1beta1.PGBackRestRepo{{Name: "repo1"}},
						},
					},
				},
			},
			&backupv1alpha1.Backup{
				ObjectMeta: metav1.ObjectMeta{Name: "src-backup", Namespace: "everest"},
				Spec: backupv1alpha1.BackupSpec{
					Origin: backupv1alpha1.BackupOrigin{
						Type:        backupv1alpha1.BackupOriginTypeInstance,
						InstanceRef: &apicommon.ObjectRef{Name: "pg-src"},
					},
					StorageRef: apicommon.ObjectRef{Name: "minio"},
				},
				Status: backupv1alpha1.BackupStatus{State: backupv1alpha1.BackupStateSucceeded},
			},
			&pgv2.PerconaPGBackup{
				ObjectMeta: metav1.ObjectMeta{Name: "src-backup", Namespace: "everest"},
				Spec:       pgv2.PerconaPGBackupSpec{PGCluster: "pg-src", RepoName: &repo1},
				Status:     pgv2.PerconaPGBackupStatus{State: pgv2.BackupSucceeded, BackupName: "20260903-090125F"},
			},
			&backupv1alpha1.BackupStorage{
				ObjectMeta: metav1.ObjectMeta{Name: "minio", Namespace: "everest"},
				Spec: backupv1alpha1.BackupStorageSpec{
					Type: backupv1alpha1.BackupStorageTypeS3,
					S3: &backupv1alpha1.BackupStorageS3Spec{
						Bucket:               "backups",
						Region:               "us-east-1",
						EndpointURL:          "http://minio:9000",
						ForcePathStyle:       &forcePathStyle,
						CredentialsSecretRef: apicommon.SecretRef{Name: "minio-creds"},
					},
				},
			},
			&corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "minio-creds", Namespace: "everest"},
				Data: map[string][]byte{
					"AWS_ACCESS_KEY_ID":     []byte("key"),
					"AWS_SECRET_ACCESS_KEY": []byte("secret"),
				},
			},
		},
	}
}

func (f seedingFixture) ctx(instance *corev1alpha1.Instance, extra ...client.Object) *controller.Context {
	objects := make([]client.Object, 0, len(f.objects)+len(extra))
	for _, obj := range append(append([]client.Object{}, f.objects...), extra...) {
		objects = append(objects, obj.DeepCopyObject().(client.Object))
	}
	k8sClient := fake.NewClientBuilder().WithScheme(f.scheme).WithObjects(objects...).Build()
	return controller.NewContext(context.Background(), k8sClient, instance, "provider-percona-postgresql")
}

func seededInstance(ds *backupv1alpha1.DataSource) *corev1alpha1.Instance {
	return &corev1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "pg-clone", Namespace: "everest", UID: types.UID("clone-uid")},
		Spec:       corev1alpha1.InstanceSpec{DataSource: ds},
	}
}

func backupDataSource() *backupv1alpha1.DataSource {
	return &backupv1alpha1.DataSource{
		Type:   backupv1alpha1.DataSourceTypeBackup,
		Backup: &backupv1alpha1.DataSourceBackup{BackupRef: apicommon.ObjectRef{Name: "src-backup"}},
	}
}

func pitrDataSource(date *metav1.Time, target backupv1alpha1.RecoveryTarget) *backupv1alpha1.DataSource {
	return &backupv1alpha1.DataSource{
		Type: backupv1alpha1.DataSourceTypePointInTime,
		PointInTime: &backupv1alpha1.DataSourcePointInTime{
			Source: backupv1alpha1.StreamSource{
				InstanceRef: &apicommon.ObjectRef{Name: "pg-src"},
				StorageRef:  apicommon.ObjectRef{Name: "minio"},
			},
			RecoveryTarget: target,
			Date:           date,
		},
	}
}

func TestBuildDataSourceBootstrap(t *testing.T) {
	t.Parallel()

	f := newSeedingFixture(t)

	t.Run("backup of another instance reads the source repo and pins the set", func(t *testing.T) {
		t.Parallel()

		bootstrap, pending, err := buildDataSourceBootstrap(f.ctx(seededInstance(backupDataSource())))

		require.NoError(t, err)
		require.Nil(t, pending)
		require.NotNil(t, bootstrap)

		ds := bootstrap.DataSource.PGBackRest
		require.NotNil(t, ds)
		assert.Equal(t, "db", ds.Stanza)
		assert.Equal(t, "repo1", ds.Repo.Name)
		require.NotNil(t, ds.Repo.S3)
		assert.Equal(t, "backups", ds.Repo.S3.Bucket)
		assert.Equal(t, "us-east-1", ds.Repo.S3.Region)
		assert.Equal(t, "http://minio:9000", ds.Repo.S3.Endpoint)
		assert.Equal(t, []string{"--set=20260903-090125F", "--type=immediate"}, ds.Options)
		assert.Equal(t, "/pgbackrest/src-uid/repo1", ds.Global["repo1-path"])
		assert.Equal(t, "path", ds.Global["repo1-s3-uri-style"])

		require.Len(t, ds.Configuration, 1)
		assert.Equal(t, bootstrap.CredentialSecret.Name, ds.Configuration[0].Secret.Name)
		assert.Equal(t, "pg-clone-pgbackrest-datasource-creds", bootstrap.CredentialSecret.Name)
		assert.Contains(t, bootstrap.CredentialSecret.StringData["repo1-s3-credentials.conf"], "repo1-s3-key=key")
	})

	t.Run("point in time of another instance recovers to the target date", func(t *testing.T) {
		t.Parallel()

		date := metav1.NewTime(time.Date(2026, 8, 1, 15, 30, 0, 0, time.UTC))
		bootstrap, pending, err := buildDataSourceBootstrap(f.ctx(
			seededInstance(pitrDataSource(&date, backupv1alpha1.RecoveryTargetDate)),
		))

		require.NoError(t, err)
		require.Nil(t, pending)
		require.NotNil(t, bootstrap)

		ds := bootstrap.DataSource.PGBackRest
		assert.Equal(t, "repo1", ds.Repo.Name)
		assert.Equal(t, "/pgbackrest/src-uid/repo1", ds.Global["repo1-path"])
		assert.Equal(t, []string{"--type=time", `--target="2026-08-01 15:30:00+00:00"`}, ds.Options)
	})

	t.Run("no data source needs no bootstrap", func(t *testing.T) {
		t.Parallel()

		bootstrap, pending, err := buildDataSourceBootstrap(f.ctx(seededInstance(nil)))

		require.NoError(t, err)
		assert.Nil(t, pending)
		assert.Nil(t, bootstrap)
	})

	t.Run("source backup that has not succeeded waits", func(t *testing.T) {
		t.Parallel()

		running := &pgv2.PerconaPGBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "src-backup", Namespace: "everest"},
			Status:     pgv2.PerconaPGBackupStatus{State: pgv2.BackupRunning},
		}
		ctx := f.ctx(seededInstance(backupDataSource()))
		require.NoError(t, ctx.Client().Update(ctx.Context(), mergeResourceVersion(t, ctx, running)))

		bootstrap, pending, err := buildDataSourceBootstrap(ctx)

		require.NoError(t, err)
		assert.Nil(t, bootstrap)
		require.NotNil(t, pending)
		assert.Equal(t, backupv1alpha1.RestoreStatePending, pending.State)
	})
}

// mergeResourceVersion copies the stored resource version onto obj so the fake
// client accepts the update.
func mergeResourceVersion(t *testing.T, c *controller.Context, obj *pgv2.PerconaPGBackup) *pgv2.PerconaPGBackup {
	t.Helper()
	stored := &pgv2.PerconaPGBackup{}
	require.NoError(t, c.Client().Get(c.Context(), client.ObjectKeyFromObject(obj), stored))
	obj.ResourceVersion = stored.ResourceVersion
	obj.Spec = stored.Spec
	return obj
}

func TestSyncSeedingRestore(t *testing.T) {
	t.Parallel()

	f := newSeedingFixture(t)
	restore := &backupv1alpha1.Restore{
		ObjectMeta: metav1.ObjectMeta{Name: "pg-clone" + controller.DataSourceRestoreNameSuffix, Namespace: "everest"},
		Spec: backupv1alpha1.RestoreSpec{
			InstanceRef: apicommon.ObjectRef{Name: "pg-clone"},
			DataSource:  *backupDataSource(),
		},
	}

	t.Run("reports running until the bootstrapped cluster is ready and creates no operator restore", func(t *testing.T) {
		t.Parallel()

		ctx := f.ctx(seededInstance(backupDataSource()), restore.DeepCopy(), &pgv2.PerconaPGCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "pg-clone", Namespace: "everest"},
			Status:     pgv2.PerconaPGClusterStatus{State: pgv2.AppStateInit},
		})
		got := restore.DeepCopy()

		status, err := (&Provider{}).SyncRestore(ctx, got)

		require.NoError(t, err)
		assert.Equal(t, backupv1alpha1.RestoreStateRunning, status.State)
		opRestores := &pgv2.PerconaPGRestoreList{}
		require.NoError(t, ctx.Client().List(ctx.Context(), opRestores))
		assert.Empty(t, opRestores.Items)
	})

	t.Run("reports succeeded once the bootstrapped cluster is ready", func(t *testing.T) {
		t.Parallel()

		ctx := f.ctx(seededInstance(backupDataSource()), restore.DeepCopy(), &pgv2.PerconaPGCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "pg-clone", Namespace: "everest"},
			Status:     pgv2.PerconaPGClusterStatus{State: pgv2.AppStateReady},
		})

		status, err := (&Provider{}).SyncRestore(ctx, restore.DeepCopy())

		require.NoError(t, err)
		assert.Equal(t, backupv1alpha1.RestoreStateSucceeded, status.State)
	})
}
