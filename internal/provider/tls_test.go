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

	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	apicommon "github.com/openeverest/openeverest/v2/api/common/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	pgv2 "github.com/percona/percona-postgresql-operator/v3/pkg/apis/pgv2.percona.com/v2"
	crunchyv1beta1 "github.com/percona/percona-postgresql-operator/v3/pkg/apis/upstream.pgv2.percona.com/v1beta1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
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
	tls, err := tlsParameters(c.Instance())
	require.NoError(t, err)
	assert.Equal(t, definition.TLSModePrefer, tls.Mode)
}

func TestTLSParameters_Decode(t *testing.T) {
	t.Parallel()

	c := newTLSTestContext(t, &definition.TLSParameters{
		Mode:                  definition.TLSModeRequire,
		CertificateSecretName: "my-tls",
	})
	tls, err := tlsParameters(c.Instance())
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
			CertificateSecretName:         "my-tls",
			InternalCertificateSecretName: "my-tls-repl",
		})
		require.NotNil(t, cluster.Spec.Secrets.CustomTLSSecret)
		assert.Equal(t, "my-tls", cluster.Spec.Secrets.CustomTLSSecret.Name)
		require.NotNil(t, cluster.Spec.Secrets.CustomReplicationClientTLSSecret)
		assert.Equal(t, "my-tls-repl", cluster.Spec.Secrets.CustomReplicationClientTLSSecret.Name)
		require.NotNil(t, cluster.Spec.Proxy.PGBouncer.CustomTLSSecret)
		assert.Equal(t, "my-tls", cluster.Spec.Proxy.PGBouncer.CustomTLSSecret.Name)
	})

	t.Run("issuer applies to issuerConf", func(t *testing.T) {
		t.Parallel()
		cluster := &pgv2.PerconaPGCluster{Spec: pgv2.PerconaPGClusterSpec{
			Proxy: &pgv2.PGProxySpec{PGBouncer: &pgv2.PGBouncerSpec{}},
		}}
		configureTLS(cluster, definition.TLSParameters{Issuer: &definition.IssuerReference{
			Name: "corp-pca", Kind: "AWSPCAClusterIssuer", Group: "awspca.cert-manager.io",
		}})
		require.NotNil(t, cluster.Spec.TLS)
		require.NotNil(t, cluster.Spec.TLS.IssuerConf)
		assert.Equal(t, "corp-pca", cluster.Spec.TLS.IssuerConf.Name)
		assert.Equal(t, "AWSPCAClusterIssuer", cluster.Spec.TLS.IssuerConf.Kind)
		assert.Equal(t, "awspca.cert-manager.io", cluster.Spec.TLS.IssuerConf.Group)
		assert.Empty(t, cluster.Spec.TLS.CertManagementPolicy, "left to the operator's default, auto")
		assert.Nil(t, cluster.Spec.Secrets.CustomRootCATLSSecret)
	})

	t.Run("no issuer leaves the operator's TLS spec unset", func(t *testing.T) {
		t.Parallel()
		cluster := &pgv2.PerconaPGCluster{Spec: pgv2.PerconaPGClusterSpec{
			Proxy: &pgv2.PGProxySpec{PGBouncer: &pgv2.PGBouncerSpec{}},
		}}
		configureTLS(cluster, definition.TLSParameters{Mode: definition.TLSModePrefer})
		assert.Nil(t, cluster.Spec.TLS)
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

func TestValidateTLSParameters_InvalidMode(t *testing.T) {
	t.Parallel()

	errs := validateTLSParameters(definition.TLSParameters{Mode: "bogus"}, field.NewPath("spec", "parameters", "tls"))
	require.Len(t, errs, 1)
	assert.Equal(t, "spec.parameters.tls.mode", errs[0].Field)
	assert.Contains(t, errs[0].Error(), "must be one of")
}

func TestValidateTLSParameters_CertificateAndRootCAMutuallyExclusive(t *testing.T) {
	t.Parallel()

	errs := validateTLSParameters(definition.TLSParameters{
		Mode:                          definition.TLSModePrefer,
		CertificateSecretName:         "my-tls",
		InternalCertificateSecretName: "my-tls-repl",
		RootCASecretName:              "my-ca",
	}, field.NewPath("spec", "parameters", "tls"))
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "not both")
}

func TestValidateTLSParameters_InternalSecretRequiresCertificateSecret(t *testing.T) {
	t.Parallel()

	errs := validateTLSParameters(definition.TLSParameters{
		Mode:                          definition.TLSModePrefer,
		InternalCertificateSecretName: "my-tls-repl",
	}, field.NewPath("spec", "parameters", "tls"))
	require.Len(t, errs, 1)
	assert.Equal(t, "spec.parameters.tls.certificateSecretName", errs[0].Field)
}

func TestValidateTLSParameters_CertificateSecretRequiresInternalSecret(t *testing.T) {
	t.Parallel()

	errs := validateTLSParameters(definition.TLSParameters{
		Mode:                  definition.TLSModePrefer,
		CertificateSecretName: "my-tls",
	}, field.NewPath("spec", "parameters", "tls"))
	require.Len(t, errs, 1)
	assert.Equal(t, "spec.parameters.tls.internalCertificateSecretName", errs[0].Field)
}

func TestValidateTLSParameters_Valid(t *testing.T) {
	t.Parallel()

	assert.Empty(t, validateTLSParameters(definition.TLSParameters{
		Mode:                          definition.TLSModePrefer,
		CertificateSecretName:         "my-tls",
		InternalCertificateSecretName: "my-tls-repl",
	}, field.NewPath("spec", "parameters", "tls")))
}

func TestTLSParameters_IssuerDefaults(t *testing.T) {
	t.Parallel()

	c := newTLSTestContext(t, &definition.TLSParameters{Issuer: &definition.IssuerReference{Name: "company-ca"}})
	tls, err := tlsParameters(c.Instance())
	require.NoError(t, err)
	require.NotNil(t, tls.Issuer)
	assert.Equal(t, definition.IssuerKindIssuer, tls.Issuer.Kind)
	assert.Equal(t, "cert-manager.io", tls.Issuer.Group)

	c = newTLSTestContext(t, &definition.TLSParameters{Issuer: &definition.IssuerReference{Kind: definition.IssuerKindClusterIssuer}})
	tls, err = tlsParameters(c.Instance())
	require.NoError(t, err)
	assert.Nil(t, tls.Issuer, "an issuer without a name, as the UI leaves it, is no issuer")
}

func TestValidateTLSParameters_Issuer(t *testing.T) {
	t.Parallel()

	path := field.NewPath("spec", "parameters", "tls")
	tests := []struct {
		name   string
		tls    definition.TLSParameters
		field  string
		substr string
	}{
		{
			name: "cert-manager ClusterIssuer",
			tls:  definition.TLSParameters{Issuer: &definition.IssuerReference{Name: "company-ca", Kind: "ClusterIssuer", Group: "cert-manager.io"}},
		},
		{
			name: "external issuer",
			tls:  definition.TLSParameters{Issuer: &definition.IssuerReference{Name: "corp-pca", Kind: "AWSPCAClusterIssuer", Group: "awspca.cert-manager.io"}},
		},
		{
			name:   "namespaced Issuer",
			tls:    definition.TLSParameters{Issuer: &definition.IssuerReference{Name: "company-ca", Kind: "Issuer", Group: "cert-manager.io"}},
			field:  "spec.parameters.tls.issuer.kind",
			substr: "takes it over",
		},
		{
			name:   "namespaced Issuer of an external group",
			tls:    definition.TLSParameters{Issuer: &definition.IssuerReference{Name: "corp-pca", Kind: "Issuer", Group: "awspca.cert-manager.io"}},
			field:  "spec.parameters.tls.issuer.kind",
			substr: "takes it over",
		},
		{
			name:   "unknown kind of cert-manager",
			tls:    definition.TLSParameters{Issuer: &definition.IssuerReference{Name: "x", Kind: "Bogus", Group: "cert-manager.io"}},
			field:  "spec.parameters.tls.issuer.kind",
			substr: "supported values",
		},
		{
			name:   "ClusterIssuer of an external group",
			tls:    definition.TLSParameters{Issuer: &definition.IssuerReference{Name: "x", Kind: "ClusterIssuer", Group: "awspca.cert-manager.io"}},
			field:  "spec.parameters.tls.issuer.kind",
			substr: "external issuer",
		},
		{
			name:  "invalid group",
			tls:   definition.TLSParameters{Issuer: &definition.IssuerReference{Name: "x", Kind: "AWSPCAClusterIssuer", Group: "Not A Group"}},
			field: "spec.parameters.tls.issuer.group",
		},
		{
			name: "issuer with certificate secrets",
			tls: definition.TLSParameters{
				CertificateSecretName:         "my-tls",
				InternalCertificateSecretName: "my-tls-repl",
				Issuer:                        &definition.IssuerReference{Name: "company-ca", Kind: "ClusterIssuer", Group: "cert-manager.io"},
			},
			field:  "spec.parameters.tls.issuer",
			substr: "not both",
		},
		{
			name: "issuer with root CA secret",
			tls: definition.TLSParameters{
				RootCASecretName: "my-ca",
				Issuer:           &definition.IssuerReference{Name: "company-ca", Kind: "ClusterIssuer", Group: "cert-manager.io"},
			},
			field:  "spec.parameters.tls.rootCASecretName",
			substr: "not both",
		},
		{
			name:   "additional names",
			tls:    definition.TLSParameters{AdditionalNames: []string{"pg.example.com"}},
			field:  "spec.parameters.tls.additionalNames",
			substr: "percona-postgresql-operator/issues/1811",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.tls.Mode = definition.TLSModePrefer
			errs := validateTLSParameters(tt.tls, path)
			if tt.field == "" {
				assert.Empty(t, errs)
				return
			}
			require.Len(t, errs, 1, errs)
			assert.Equal(t, tt.field, errs[0].Field)
			assert.Contains(t, errs[0].Error(), tt.substr)
		})
	}
}

func TestValidateReferences_IssuerChangeOnExistingInstance(t *testing.T) {
	t.Parallel()

	existing := func(conf *cmmeta.IssuerReference) *pgv2.PerconaPGCluster {
		cluster := &pgv2.PerconaPGCluster{ObjectMeta: metav1.ObjectMeta{Name: "pg-tls", Namespace: "everest"}}
		if conf != nil {
			cluster.Spec.TLS = &crunchyv1beta1.TLSSpec{IssuerConf: conf}
		}
		return cluster
	}
	companyCA := &definition.IssuerReference{Name: "company-ca", Kind: "ClusterIssuer"}

	tests := []struct {
		name    string
		current *pgv2.PerconaPGCluster
		issuer  *definition.IssuerReference
		changed bool
	}{
		{"same issuer, group spelled out", existing(&cmmeta.IssuerReference{Name: "company-ca", Kind: "ClusterIssuer"}),
			&definition.IssuerReference{Name: "company-ca", Kind: "ClusterIssuer", Group: "cert-manager.io"}, false},
		{"issuer added", existing(nil), companyCA, true},
		{"issuer removed", existing(&cmmeta.IssuerReference{Name: "company-ca", Kind: "ClusterIssuer", Group: "cert-manager.io"}), nil, true},
		{"other issuer", existing(&cmmeta.IssuerReference{Name: "other-ca", Kind: "ClusterIssuer", Group: "cert-manager.io"}), companyCA, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := newTLSTestContext(t, &definition.TLSParameters{Issuer: tt.issuer}, tt.current)
			errs, err := New().ValidateReferences(c)
			require.NoError(t, err)
			if !tt.changed {
				assert.Empty(t, errs)
				return
			}
			require.Len(t, errs, 1)
			assert.Contains(t, errs[0].Error(), "can't change on an existing instance")
		})
	}
}

func TestValidateReferences_RejectsInternalCertificateChangeOnExistingInstance(t *testing.T) {
	t.Parallel()

	existing := &pgv2.PerconaPGCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "pg-tls", Namespace: "everest"},
		Spec: pgv2.PerconaPGClusterSpec{
			Secrets: pgv2.SecretsSpec{
				CustomTLSSecret:                  secretProjection("my-tls"),
				CustomReplicationClientTLSSecret: secretProjection("old-repl"),
			},
		},
	}
	c := newTLSTestContext(t, &definition.TLSParameters{
		CertificateSecretName:         "my-tls",
		InternalCertificateSecretName: "new-repl",
	},
		existing,
		certSecret("my-tls", "everest", []byte("ca")),
		certSecret("new-repl", "everest", []byte("ca")),
	)
	errs, err := New().ValidateReferences(c)
	require.NoError(t, err)
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "can't change on an existing instance")
}

func TestValidateReferences_MissingCertificateSecret(t *testing.T) {
	t.Parallel()

	c := newTLSTestContext(t, &definition.TLSParameters{
		CertificateSecretName:         "my-tls",
		InternalCertificateSecretName: "my-tls-repl",
	})
	errs, err := New().ValidateReferences(c)
	require.NoError(t, err)
	require.Len(t, errs, 2)
	assert.Equal(t, "spec.parameters.tls.certificateSecretName", errs[0].Field)
	assert.Equal(t, "spec.parameters.tls.internalCertificateSecretName", errs[1].Field)
}

func TestValidateReferences_CAMismatch(t *testing.T) {
	t.Parallel()

	c := newTLSTestContext(t, &definition.TLSParameters{
		CertificateSecretName:         "my-tls",
		InternalCertificateSecretName: "my-tls-repl",
	},
		certSecret("my-tls", "everest", []byte("ca-a")),
		certSecret("my-tls-repl", "everest", []byte("ca-b")),
	)
	errs, err := New().ValidateReferences(c)
	require.NoError(t, err)
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "must share the same ca.crt")
}

func TestValidateReferences_ValidCertificates(t *testing.T) {
	t.Parallel()

	c := newTLSTestContext(t, &definition.TLSParameters{
		CertificateSecretName:         "my-tls",
		InternalCertificateSecretName: "my-tls-repl",
	},
		certSecret("my-tls", "everest", []byte("ca-shared")),
		certSecret("my-tls-repl", "everest", []byte("ca-shared")),
	)
	errs, err := New().ValidateReferences(c)
	require.NoError(t, err)
	assert.Empty(t, errs)
}

func TestValidateReferences_MissingRootCASecret(t *testing.T) {
	t.Parallel()

	c := newTLSTestContext(t, &definition.TLSParameters{RootCASecretName: "my-ca"})
	errs, err := New().ValidateReferences(c)
	require.NoError(t, err)
	require.Len(t, errs, 1)
	assert.Equal(t, "spec.parameters.tls.rootCASecretName", errs[0].Field)
}

func TestValidateReferences_ValidRootCA(t *testing.T) {
	t.Parallel()

	c := newTLSTestContext(t, &definition.TLSParameters{RootCASecretName: "my-ca"}, rootCASecret("my-ca", "everest"))
	errs, err := New().ValidateReferences(c)
	require.NoError(t, err)
	assert.Empty(t, errs)
}

func TestValidateReferences_NoChangeOnNewInstance(t *testing.T) {
	t.Parallel()

	c := newTLSTestContext(t, &definition.TLSParameters{Mode: definition.TLSModeRequire})
	errs, err := New().ValidateReferences(c)
	require.NoError(t, err)
	assert.Empty(t, errs)
}

func TestValidateReferences_RejectsCertificateChangeOnExistingInstance(t *testing.T) {
	t.Parallel()

	existing := &pgv2.PerconaPGCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "pg-tls", Namespace: "everest"},
		Spec: pgv2.PerconaPGClusterSpec{
			Secrets: pgv2.SecretsSpec{CustomTLSSecret: secretProjection("old-tls")},
		},
	}
	c := newTLSTestContext(t, &definition.TLSParameters{
		CertificateSecretName:         "new-tls",
		InternalCertificateSecretName: "new-tls-repl",
	},
		existing,
		certSecret("new-tls", "everest", []byte("ca")),
		certSecret("new-tls-repl", "everest", []byte("ca")),
	)
	errs, err := New().ValidateReferences(c)
	require.NoError(t, err)
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "can't change on an existing instance")
}

func TestValidateReferences_AllowsSameCertificateOnExistingInstance(t *testing.T) {
	t.Parallel()

	existing := &pgv2.PerconaPGCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "pg-tls", Namespace: "everest"},
		Spec: pgv2.PerconaPGClusterSpec{
			Secrets: pgv2.SecretsSpec{
				CustomTLSSecret:                  secretProjection("my-tls"),
				CustomReplicationClientTLSSecret: secretProjection("my-tls-repl"),
			},
		},
	}
	c := newTLSTestContext(t, &definition.TLSParameters{
		CertificateSecretName:         "my-tls",
		InternalCertificateSecretName: "my-tls-repl",
	},
		existing,
		certSecret("my-tls", "everest", []byte("ca")),
		certSecret("my-tls-repl", "everest", []byte("ca")),
	)
	errs, err := New().ValidateReferences(c)
	require.NoError(t, err)
	assert.Empty(t, errs)
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
