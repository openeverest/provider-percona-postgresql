package provider

import (
	"encoding/json"
	"testing"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
)

func proxyWithConfiguration(t *testing.T, configuration string) corev1alpha1.ComponentSpec {
	t.Helper()

	raw, err := json.Marshal(map[string]any{"configuration": configuration})
	require.NoError(t, err)

	return corev1alpha1.ComponentSpec{
		Parameters: &runtime.RawExtension{Raw: raw},
	}
}

func TestProxyConfigurationFromComponent_Empty(t *testing.T) {
	t.Parallel()

	got, err := proxyConfigurationFromComponent(corev1alpha1.ComponentSpec{})
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestProxyConfigurationFromComponent_BlankConfiguration(t *testing.T) {
	t.Parallel()

	got, err := proxyConfigurationFromComponent(proxyWithConfiguration(t, "   "))
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestProxyConfigurationFromComponent_ParsesArbitraryParameters(t *testing.T) {
	t.Parallel()

	proxy := proxyWithConfiguration(t, `
max_client_conn: "500"
default_pool_size: 20
ignore_startup_parameters: extra_float_digits
`)

	got, err := proxyConfigurationFromComponent(proxy)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "500", got["max_client_conn"])
	assert.Equal(t, "20", got["default_pool_size"])
	assert.Equal(t, "extra_float_digits", got["ignore_startup_parameters"])
}

func TestProxyConfigurationFromComponent_ErrorsDoNotMentionJSON(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"plain scalar (not a mapping)": "afsdasdfasd",
		"list instead of a mapping":    "- a\n- b\n",
		"bad indentation":              "foo: bar\n  baz: qux\n",
		"tab indentation":              "foo:\n\tbar: baz\n",
	}

	for name, configuration := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := proxyConfigurationFromComponent(proxyWithConfiguration(t, configuration))
			require.Error(t, err)
		})
	}
}

func TestValidateProxyConfiguration_NoConfiguration(t *testing.T) {
	t.Parallel()

	assert.NoError(t, validateProxyConfiguration(corev1alpha1.ComponentSpec{}))
}

func TestValidateProxyConfiguration_Invalid(t *testing.T) {
	t.Parallel()

	err := validateProxyConfiguration(proxyWithConfiguration(t, "- a\n- b\n"))
	require.Error(t, err)
}

func TestApplyProxyConfiguration(t *testing.T) {
	t.Parallel()

	t.Run("sets global settings when configuration is present", func(t *testing.T) {
		t.Parallel()

		cluster := &pgv2.PerconaPGCluster{Spec: defaultSpec()}
		proxy := proxyWithConfiguration(t, "max_client_conn: \"500\"\n")

		require.NoError(t, applyProxyConfiguration(cluster, proxy))
		assert.Equal(t, "500", cluster.Spec.Proxy.PGBouncer.Config.Global["max_client_conn"])
	})

	t.Run("no parameters leaves global settings nil", func(t *testing.T) {
		t.Parallel()

		cluster := &pgv2.PerconaPGCluster{Spec: defaultSpec()}

		require.NoError(t, applyProxyConfiguration(cluster, corev1alpha1.ComponentSpec{}))
		assert.Nil(t, cluster.Spec.Proxy.PGBouncer.Config.Global)
	})

	t.Run("no pgbouncer spec is a no-op", func(t *testing.T) {
		t.Parallel()

		cluster := &pgv2.PerconaPGCluster{}
		proxy := proxyWithConfiguration(t, "max_client_conn: \"500\"\n")

		require.NoError(t, applyProxyConfiguration(cluster, proxy))
	})
}
