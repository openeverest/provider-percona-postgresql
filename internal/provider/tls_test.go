// Copyright (C) 2026 The OpenEverest Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package provider

import (
	"context"
	"encoding/json"
	"testing"

	apicommon "github.com/openeverest/openeverest/v2/api/common/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/openeverest/provider-percona-postgresql/definition"
)

func tlsInt32Ptr(v int32) *int32 {
	return &v
}

func newTLSTestContext(t *testing.T, tls *definition.TLSParameters, objs ...client.Object) *controller.Context {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	require.NoError(t, pgv2.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	instance := &corev1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "pg-tls", Namespace: "everest"},
		Spec: corev1alpha1.InstanceSpec{
			ProviderRef: apicommon.ObjectRef{Name: "provider-percona-postgresql"},
		},
	}
	if tls != nil {
		raw, err := json.Marshal(definition.Parameters{TLS: tls})
		require.NoError(t, err)
		instance.Spec.Parameters = &runtime.RawExtension{Raw: raw}
	}

	allObjs := append([]client.Object{instance}, objs...)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(allObjs...).Build()
	return controller.NewContext(context.Background(), k8sClient, instance, "provider-percona-postgresql")
}

func certSecret(name, namespace string, ca []byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Data: map[string][]byte{
			"tls.crt": []byte("cert"),
			"tls.key": []byte("key"),
			"ca.crt":  ca,
		},
	}
}

func rootCASecret(name, namespace string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Data: map[string][]byte{
			"root.crt": []byte("ca-cert"),
			"root.key": []byte("ca-key"),
		},
	}
}

func TestTLSParameters_Defaults(t *testing.T) {
	t.Parallel()

	c := newTLSTestContext(t, nil)
	tls, err := tlsParameters(c)
	require.NoError(t, err)
	assert.Equal(t, definition.TLSModePrefer, tls.Mode)
}

func TestTLSParameters_Decode(t *testing.T) {
	t.Parallel()

	c := newTLSTestContext(t, &definition.TLSParameters{
		Mode:                  definition.TLSModeRequire,
		CertificateSecretName: "my-tls",
	})
	tls, err := tlsParameters(c)
	require.NoError(t, err)
	assert.Equal(t, definition.TLSModeRequire, tls.Mode)
	assert.Equal(t, "my-tls", tls.CertificateSecretName)
}

func TestClientCertificateSecretName(t *testing.T) {
	t.Parallel()

	c := newTLSTestContext(t, nil)

	assert.Equal(t, "pg-tls-cluster-cert", clientCertificateSecretName(c, definition.TLSParameters{}))
	assert.Equal(t, "custom", clientCertificateSecretName(c, definition.TLSParameters{CertificateSecretName: "custom"}))
}

func TestConfigureTLS(t *testing.T) {
	t.Parallel()

	t.Run("prefer mode leaves TLSOnly false", func(t *testing.T) {
		t.Parallel()
		cluster := &pgv2.PerconaPGCluster{Spec: pgv2.PerconaPGClusterSpec{
			Proxy: &pgv2.PGProxySpec{PGBouncer: &pgv2.PGBouncerSpec{}},
		}}
		configureTLS(cluster, definition.TLSParameters{Mode: definition.TLSModePrefer})
		assert.False(t, cluster.Spec.TLSOnly)
	})

	t.Run("require mode sets TLSOnly", func(t *testing.T) {
		t.Parallel()
		cluster := &pgv2.PerconaPGCluster{Spec: pgv2.PerconaPGClusterSpec{
			Proxy: &pgv2.PGProxySpec{PGBouncer: &pgv2.PGBouncerSpec{}},
		}}
		configureTLS(cluster, definition.TLSParameters{Mode: definition.TLSModeRequire})
		assert.True(t, cluster.Spec.TLSOnly)
	})

	t.Run("certificate secrets apply to postgres and pgbouncer", func(t *testing.T) {
		t.Parallel()
		cluster := &pgv2.PerconaPGCluster{Spec: pgv2.PerconaPGClusterSpec{
			Proxy: &pgv2.PGProxySpec{PGBouncer: &pgv2.PGBouncerSpec{}},
		}}
		configureTLS(cluster, definition.TLSParameters{
			CertificateSecretName:            "my-tls",
			ReplicationCertificateSecretName: "my-tls-repl",
		})
		require.NotNil(t, cluster.Spec.Secrets.CustomTLSSecret)
		assert.Equal(t, "my-tls", cluster.Spec.Secrets.CustomTLSSecret.Name)
		require.NotNil(t, cluster.Spec.Secrets.CustomReplicationClientTLSSecret)
		assert.Equal(t, "my-tls-repl", cluster.Spec.Secrets.CustomReplicationClientTLSSecret.Name)
		require.NotNil(t, cluster.Spec.Proxy.PGBouncer.CustomTLSSecret)
		assert.Equal(t, "my-tls", cluster.Spec.Proxy.PGBouncer.CustomTLSSecret.Name)
	})

	t.Run("root CA secret applies only to secrets spec", func(t *testing.T) {
		t.Parallel()
		cluster := &pgv2.PerconaPGCluster{Spec: pgv2.PerconaPGClusterSpec{
			Proxy: &pgv2.PGProxySpec{PGBouncer: &pgv2.PGBouncerSpec{}},
		}}
		configureTLS(cluster, definition.TLSParameters{RootCASecretName: "my-ca"})
		require.NotNil(t, cluster.Spec.Secrets.CustomRootCATLSSecret)
		assert.Equal(t, "my-ca", cluster.Spec.Secrets.CustomRootCATLSSecret.Name)
		assert.Nil(t, cluster.Spec.Secrets.CustomTLSSecret)
		assert.Nil(t, cluster.Spec.Proxy.PGBouncer.CustomTLSSecret)
	})
}

func TestValidateTLS_InvalidMode(t *testing.T) {
	t.Parallel()

	c := newTLSTestContext(t, &definition.TLSParameters{Mode: "bogus"})
	err := validateTLS(c)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mode must be one of")
}

func TestValidateTLS_CertificateAndRootCAMutuallyExclusive(t *testing.T) {
	t.Parallel()

	c := newTLSTestContext(t, &definition.TLSParameters{
		CertificateSecretName:            "my-tls",
		ReplicationCertificateSecretName: "my-tls-repl",
		RootCASecretName:                 "my-ca",
	})
	err := validateTLS(c)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not both")
}

func TestValidateTLS_ReplicationSecretRequiresCertificateSecret(t *testing.T) {
	t.Parallel()

	c := newTLSTestContext(t, &definition.TLSParameters{ReplicationCertificateSecretName: "my-tls-repl"})
	err := validateTLS(c)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires certificateSecretName")
}

func TestValidateTLS_CertificateSecretRequiresReplicationSecret(t *testing.T) {
	t.Parallel()

	c := newTLSTestContext(t, &definition.TLSParameters{CertificateSecretName: "my-tls"})
	err := validateTLS(c)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires replicationCertificateSecretName")
}

func TestValidateTLS_MissingCertificateSecret(t *testing.T) {
	t.Parallel()

	c := newTLSTestContext(t, &definition.TLSParameters{
		CertificateSecretName:            "my-tls",
		ReplicationCertificateSecretName: "my-tls-repl",
	})
	err := validateTLS(c)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get certificate secret")
}

func TestValidateTLS_CAMismatch(t *testing.T) {
	t.Parallel()

	c := newTLSTestContext(t, &definition.TLSParameters{
		CertificateSecretName:            "my-tls",
		ReplicationCertificateSecretName: "my-tls-repl",
	},
		certSecret("my-tls", "everest", []byte("ca-a")),
		certSecret("my-tls-repl", "everest", []byte("ca-b")),
	)
	err := validateTLS(c)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must share the same ca.crt")
}

func TestValidateTLS_ValidCertificates(t *testing.T) {
	t.Parallel()

	c := newTLSTestContext(t, &definition.TLSParameters{
		CertificateSecretName:            "my-tls",
		ReplicationCertificateSecretName: "my-tls-repl",
	},
		certSecret("my-tls", "everest", []byte("ca-shared")),
		certSecret("my-tls-repl", "everest", []byte("ca-shared")),
	)
	assert.NoError(t, validateTLS(c))
}

func TestValidateTLS_MissingRootCASecret(t *testing.T) {
	t.Parallel()

	c := newTLSTestContext(t, &definition.TLSParameters{RootCASecretName: "my-ca"})
	err := validateTLS(c)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get root CA secret")
}

func TestValidateTLS_ValidRootCA(t *testing.T) {
	t.Parallel()

	c := newTLSTestContext(t, &definition.TLSParameters{RootCASecretName: "my-ca"}, rootCASecret("my-ca", "everest"))
	assert.NoError(t, validateTLS(c))
}

func TestValidateTLS_NoChangeOnNewInstance(t *testing.T) {
	t.Parallel()

	c := newTLSTestContext(t, &definition.TLSParameters{Mode: definition.TLSModeRequire})
	assert.NoError(t, validateTLS(c))
}

func TestValidateTLS_RejectsCertificateChangeOnExistingInstance(t *testing.T) {
	t.Parallel()

	existing := &pgv2.PerconaPGCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "pg-tls", Namespace: "everest"},
		Spec: pgv2.PerconaPGClusterSpec{
			Secrets: pgv2.SecretsSpec{CustomTLSSecret: secretProjection("old-tls")},
		},
	}
	c := newTLSTestContext(t, &definition.TLSParameters{
		CertificateSecretName:            "new-tls",
		ReplicationCertificateSecretName: "new-tls-repl",
	},
		existing,
		certSecret("new-tls", "everest", []byte("ca")),
		certSecret("new-tls-repl", "everest", []byte("ca")),
	)
	err := validateTLS(c)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "can't change on an existing instance")
}

func TestValidateTLS_AllowsSameCertificateOnExistingInstance(t *testing.T) {
	t.Parallel()

	existing := &pgv2.PerconaPGCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "pg-tls", Namespace: "everest"},
		Spec: pgv2.PerconaPGClusterSpec{
			Secrets: pgv2.SecretsSpec{CustomTLSSecret: secretProjection("my-tls")},
		},
	}
	c := newTLSTestContext(t, &definition.TLSParameters{
		CertificateSecretName:            "my-tls",
		ReplicationCertificateSecretName: "my-tls-repl",
	},
		existing,
		certSecret("my-tls", "everest", []byte("ca")),
		certSecret("my-tls-repl", "everest", []byte("ca")),
	)
	assert.NoError(t, validateTLS(c))
}

func TestTLSConnection(t *testing.T) {
	t.Parallel()

	t.Run("prefer mode without pgbouncer", func(t *testing.T) {
		t.Parallel()
		cluster := &pgv2.PerconaPGCluster{Spec: pgv2.PerconaPGClusterSpec{
			Proxy: &pgv2.PGProxySpec{PGBouncer: &pgv2.PGBouncerSpec{Replicas: tlsInt32Ptr(0)}},
		}}
		c := newTLSTestContext(t, &definition.TLSParameters{Mode: definition.TLSModePrefer})
		ca, mode, err := tlsConnection(c, cluster)
		require.NoError(t, err)
		assert.Empty(t, ca)
		assert.Equal(t, "prefer", mode)
	})

	t.Run("prefer mode with pgbouncer still requires TLS", func(t *testing.T) {
		t.Parallel()
		cluster := &pgv2.PerconaPGCluster{Spec: pgv2.PerconaPGClusterSpec{
			Proxy: &pgv2.PGProxySpec{PGBouncer: &pgv2.PGBouncerSpec{}},
		}}
		c := newTLSTestContext(t, &definition.TLSParameters{Mode: definition.TLSModePrefer})
		_, mode, err := tlsConnection(c, cluster)
		require.NoError(t, err)
		assert.Equal(t, "require", mode)
	})

	t.Run("require mode always requires TLS", func(t *testing.T) {
		t.Parallel()
		cluster := &pgv2.PerconaPGCluster{Spec: pgv2.PerconaPGClusterSpec{
			Proxy: &pgv2.PGProxySpec{PGBouncer: &pgv2.PGBouncerSpec{Replicas: tlsInt32Ptr(0)}},
		}}
		c := newTLSTestContext(t, &definition.TLSParameters{Mode: definition.TLSModeRequire})
		_, mode, err := tlsConnection(c, cluster)
		require.NoError(t, err)
		assert.Equal(t, "require", mode)
	})

	t.Run("returns CA from the certificate secret", func(t *testing.T) {
		t.Parallel()
		cluster := &pgv2.PerconaPGCluster{Spec: pgv2.PerconaPGClusterSpec{
			Proxy: &pgv2.PGProxySpec{PGBouncer: &pgv2.PGBouncerSpec{Replicas: tlsInt32Ptr(0)}},
		}}
		c := newTLSTestContext(t, &definition.TLSParameters{CertificateSecretName: "my-tls"},
			certSecret("my-tls", "everest", []byte("ca-data")))
		ca, _, err := tlsConnection(c, cluster)
		require.NoError(t, err)
		assert.Equal(t, []byte("ca-data"), ca)
	})

	t.Run("missing certificate secret returns no CA", func(t *testing.T) {
		t.Parallel()
		cluster := &pgv2.PerconaPGCluster{Spec: pgv2.PerconaPGClusterSpec{
			Proxy: &pgv2.PGProxySpec{PGBouncer: &pgv2.PGBouncerSpec{Replicas: tlsInt32Ptr(0)}},
		}}
		c := newTLSTestContext(t, nil)
		ca, _, err := tlsConnection(c, cluster)
		require.NoError(t, err)
		assert.Empty(t, ca)
	})
}
