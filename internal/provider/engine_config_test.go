package provider

import (
	"encoding/json"
	"testing"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
)

func engineWithConfiguration(t *testing.T, configuration string) corev1alpha1.ComponentSpec {
	t.Helper()

	raw, err := json.Marshal(map[string]any{"configuration": configuration})
	require.NoError(t, err)

	return corev1alpha1.ComponentSpec{
		Parameters: &runtime.RawExtension{Raw: raw},
	}
}

func TestEngineConfigurationFromComponent_Empty(t *testing.T) {
	t.Parallel()

	got, err := engineConfigurationFromComponent(corev1alpha1.ComponentSpec{})
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestEngineConfigurationFromComponent_BlankConfiguration(t *testing.T) {
	t.Parallel()

	got, err := engineConfigurationFromComponent(engineWithConfiguration(t, "   "))
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestEngineConfigurationFromComponent_ParsesArbitraryParameters(t *testing.T) {
	t.Parallel()

	engine := engineWithConfiguration(t, `
max_connections: "500"
shared_buffers: 1GB
some_future_parameter: true
`)

	got, err := engineConfigurationFromComponent(engine)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "500", got["max_connections"])
	assert.Equal(t, "1GB", got["shared_buffers"])
	assert.Equal(t, true, got["some_future_parameter"])
}

func TestValidateEngineConfiguration_ValidWALLevel(t *testing.T) {
	t.Parallel()

	engine := engineWithConfiguration(t, "wal_level: replica\n")
	assert.NoError(t, validateEngineConfiguration(engine))
}

func TestValidateEngineConfiguration_InvalidWALLevel(t *testing.T) {
	t.Parallel()

	engine := engineWithConfiguration(t, "wal_level: minimal\n")
	err := validateEngineConfiguration(engine)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "wal_level")
}

func TestValidateEngineConfiguration_NoConfiguration(t *testing.T) {
	t.Parallel()

	assert.NoError(t, validateEngineConfiguration(corev1alpha1.ComponentSpec{}))
}

func TestEngineConfigurationFromComponent_ErrorsDoNotMentionJSON(t *testing.T) {
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

			_, err := engineConfigurationFromComponent(engineWithConfiguration(t, configuration))
			require.Error(t, err)
		})
	}
}
