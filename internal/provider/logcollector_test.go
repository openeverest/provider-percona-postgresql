package provider

import (
	"testing"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	pgv2 "github.com/percona/percona-postgresql-operator/v3/pkg/apis/pgv2.percona.com/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestLogCollectorEnabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		params string
		want   *bool
	}{
		{name: "no parameters"},
		{name: "no logCollector", params: `{"configuration":"max_connections: 100"}`},
		{name: "enabled unset", params: `{"logCollector":{}}`},
		{name: "on", params: `{"logCollector":{"enabled":true}}`, want: new(true)},
		{name: "off", params: `{"logCollector":{"enabled":false}}`, want: new(false)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			engine := corev1alpha1.ComponentSpec{}
			if tt.params != "" {
				engine.Parameters = &runtime.RawExtension{Raw: []byte(tt.params)}
			}
			got, err := logCollectorEnabled(engine)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestApplyLogCollector(t *testing.T) {
	t.Parallel()

	const image = "percona/fluentbit:5.1.1-1"
	tests := []struct {
		name        string
		enabled     *bool
		image       string
		wantSpec    bool
		wantEnabled *bool
		wantErr     string
	}{
		{name: "unset leaves the default to the operator", image: image, wantSpec: true},
		{name: "on", enabled: new(true), image: image, wantSpec: true, wantEnabled: new(true)},
		{name: "off", enabled: new(false), image: image, wantSpec: true, wantEnabled: new(false)},
		{name: "no image and unset leaves it out", wantSpec: false},
		{name: "no image and off leaves it out", enabled: new(false), wantSpec: false},
		{name: "no image and on fails", enabled: new(true), wantErr: "cannot resolve the fluentbit image"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cluster := &pgv2.PerconaPGCluster{}
			err := applyLogCollector(cluster, tt.enabled, tt.image)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			if !tt.wantSpec {
				assert.Nil(t, cluster.Spec.LogCollector)
				return
			}
			require.NotNil(t, cluster.Spec.LogCollector)
			assert.Equal(t, tt.wantEnabled, cluster.Spec.LogCollector.Enabled)
			assert.Equal(t, image, cluster.Spec.LogCollector.Image)
			assert.Equal(t, "50m", cluster.Spec.LogCollector.Resources.Requests.Cpu().String())
			assert.Equal(t, "200Mi", cluster.Spec.LogCollector.Resources.Limits.Memory().String())
		})
	}
}
