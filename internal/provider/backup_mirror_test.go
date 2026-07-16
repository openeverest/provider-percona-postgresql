package provider

import (
	"context"
	"testing"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestMirrorScheduledBackupByAnnotations(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))

	instance := &corev1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "inst-pg", Namespace: "my-special-place"},
		Spec: corev1alpha1.InstanceSpec{
			Provider: "provider-percona-postgresql",
			Backup: &corev1alpha1.InstanceBackupSpec{
				ClassRef: corev1alpha1.BackupClassReference{Name: "pg"},
			},
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance).Build()

	repoName := "repo1"
	opBackup := &pgv2.PerconaPGBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cron-inst-pg-repo1-full-20260630141710",
			Namespace: "my-special-place",
			UID:       "11111111-1111-1111-1111-111111111111",
			Annotations: map[string]string{
				pgv2.PGBackrestAnnotationJobType:    "backup",
				pgv2.PGBackrestAnnotationBackupName: "nightly",
			},
		},
		Spec: pgv2.PerconaPGBackupSpec{
			PGCluster: "inst-pg",
			RepoName:  &repoName,
		},
	}

	mirror, err := (&Provider{}).Mirror(context.Background(), k8sClient, opBackup)
	require.NoError(t, err)
	require.NotNil(t, mirror)
	require.Equal(t, "nightly", mirror.Spec.ScheduleName)
	require.Equal(t, "repo1", mirror.Spec.StorageName)
	require.Len(t, mirror.OwnerReferences, 1)
	owner := mirror.OwnerReferences[0]
	require.Equal(t, pgv2.GroupVersion.String(), owner.APIVersion)
	require.Equal(t, "PerconaPGBackup", owner.Kind)
	require.Equal(t, opBackup.Name, owner.Name)
	require.Equal(t, opBackup.UID, owner.UID)
}

func TestMirrorSkipsOnDemandBackup(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))

	instance := &corev1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "inst-pg", Namespace: "my-special-place"},
		Spec: corev1alpha1.InstanceSpec{
			Provider: "provider-percona-postgresql",
			Backup: &corev1alpha1.InstanceBackupSpec{
				ClassRef: corev1alpha1.BackupClassReference{Name: "pg"},
			},
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance).Build()

	repoName := "repo1"
	opBackup := &pgv2.PerconaPGBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "manual-backup", Namespace: "my-special-place"},
		Spec: pgv2.PerconaPGBackupSpec{
			PGCluster: "inst-pg",
			RepoName:  &repoName,
		},
	}

	mirror, err := (&Provider{}).Mirror(context.Background(), k8sClient, opBackup)
	require.NoError(t, err)
	require.Nil(t, mirror)
}

func TestMirrorUsesJobTypeWhenBackupNameMissing(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))

	instance := &corev1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "inst-pg", Namespace: "my-special-place"},
		Spec: corev1alpha1.InstanceSpec{
			Provider: "provider-percona-postgresql",
			Backup:   &corev1alpha1.InstanceBackupSpec{ClassRef: corev1alpha1.BackupClassReference{Name: "pg"}},
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance).Build()

	repoName := "repo1"
	opBackup := &pgv2.PerconaPGBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cron-backup",
			Namespace: "my-special-place",
			Annotations: map[string]string{
				pgv2.PGBackrestAnnotationJobType: "backup",
			},
		},
		Spec: pgv2.PerconaPGBackupSpec{
			PGCluster: "inst-pg",
			RepoName:  &repoName,
		},
	}

	mirror, err := (&Provider{}).Mirror(context.Background(), k8sClient, opBackup)
	require.NoError(t, err)
	require.NotNil(t, mirror)
	require.Equal(t, "backup", mirror.Spec.ScheduleName)
}
