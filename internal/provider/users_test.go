package provider

import (
	"context"
	"testing"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	apicommon "github.com/openeverest/openeverest/v2/api/common/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestUsersForRestoredSource(t *testing.T) {
	t.Parallel()

	users := usersForRestoredSource("pg-dest", "pg-src")
	require.Len(t, users, 1)
	assert.Equal(t, "pg-dest", string(users[0].Name))
	require.Len(t, users[0].Databases, 1)
	assert.Equal(t, "pg-src", string(users[0].Databases[0]))
	require.NotNil(t, users[0].GrantPublicSchemaAccess)
	assert.True(t, *users[0].GrantPublicSchemaAccess)
}

func TestApplyPostRestoreUsers(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	require.NoError(t, backupv1alpha1.AddToScheme(scheme))
	require.NoError(t, pgv2.AddToScheme(scheme))

	instance := &corev1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "pg-dest", Namespace: "everest"},
		Spec: corev1alpha1.InstanceSpec{
			ProviderRef: apicommon.ObjectRef{Name: "provider-percona-postgresql"},
		},
	}

	t.Run("annotation on existing cluster", func(t *testing.T) {
		t.Parallel()

		existing := &pgv2.PerconaPGCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pg-dest",
				Namespace: "everest",
				Annotations: map[string]string{
					restoredFromInstanceAnnotation: "pg-src",
				},
			},
		}
		cluster := &pgv2.PerconaPGCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "pg-dest", Namespace: "everest"},
		}
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance.DeepCopy(), existing).Build()
		ctx := controller.NewContext(context.Background(), k8sClient, instance.DeepCopy(), "provider-percona-postgresql")

		require.NoError(t, applyPostRestoreUsers(ctx, cluster))
		assert.Equal(t, "pg-src", cluster.Annotations[restoredFromInstanceAnnotation])
		require.Len(t, cluster.Spec.Users, 1)
		assert.Equal(t, "pg-src", string(cluster.Spec.Users[0].Databases[0]))
	})

	t.Run("same-name source is a no-op", func(t *testing.T) {
		t.Parallel()

		existing := &pgv2.PerconaPGCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pg-dest",
				Namespace: "everest",
				Annotations: map[string]string{
					restoredFromInstanceAnnotation: "pg-dest",
				},
			},
		}
		cluster := &pgv2.PerconaPGCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "pg-dest", Namespace: "everest"},
		}
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance.DeepCopy(), existing).Build()
		ctx := controller.NewContext(context.Background(), k8sClient, instance.DeepCopy(), "provider-percona-postgresql")

		require.NoError(t, applyPostRestoreUsers(ctx, cluster))
		assert.Empty(t, cluster.Spec.Users)
	})

	t.Run("annotation already on desired cluster", func(t *testing.T) {
		t.Parallel()

		cluster := &pgv2.PerconaPGCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pg-dest",
				Namespace: "everest",
				Annotations: map[string]string{
					restoredFromInstanceAnnotation: "pg-src",
				},
			},
		}
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance.DeepCopy()).Build()
		ctx := controller.NewContext(context.Background(), k8sClient, instance.DeepCopy(), "provider-percona-postgresql")

		require.NoError(t, applyPostRestoreUsers(ctx, cluster))
		require.Len(t, cluster.Spec.Users, 1)
		assert.Equal(t, "pg-dest", string(cluster.Spec.Users[0].Name))
		assert.Equal(t, "pg-src", string(cluster.Spec.Users[0].Databases[0]))
	})
}
