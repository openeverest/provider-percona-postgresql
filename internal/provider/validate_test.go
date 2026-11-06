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
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller/validationtest"

	"github.com/openeverest/provider-percona-postgresql/internal/common"
)

const (
	providerSpecPath = "../../charts/provider-percona-postgresql/generated/provider-spec.yaml"
	clusterFixture   = "testdata/validate/cluster.yaml"
)

// component applies mutate to the named component of the Instance.
func component(name string, mutate func(*corev1alpha1.ComponentSpec)) func(*corev1alpha1.Instance) {
	return func(in *corev1alpha1.Instance) {
		c := in.Spec.Components[name]
		mutate(&c)
		in.Spec.Components[name] = c
	}
}

// TestValidate runs the checks the runtime runs before every Sync: the
// schemas, supportedFields and catalog the provider publishes, then Validate.
func TestValidate(t *testing.T) {
	t.Parallel()
	v := validationtest.New(t, New(), providerSpecPath)

	tests := []struct {
		name   string
		mutate func(*corev1alpha1.Instance)
		want   []string
	}{
		{name: "valid cluster"},
		{
			name:   "no topology type",
			mutate: func(in *corev1alpha1.Instance) { in.Spec.Topology = nil },
		},
		{
			name:   "proxy scaled to zero",
			mutate: component(common.ComponentProxy, func(c *corev1alpha1.ComponentSpec) { c.Replicas = new(int32(0)) }),
		},
		{
			name:   "unknown version",
			mutate: func(in *corev1alpha1.Instance) { in.Spec.Version = "9.0" },
			want:   []string{"spec.version"},
		},
		{
			name:   "missing engine",
			mutate: func(in *corev1alpha1.Instance) { delete(in.Spec.Components, common.ComponentEngine) },
			want:   []string{"spec.components[engine]"},
		},
		{
			name: "missing engine without a topology type",
			mutate: func(in *corev1alpha1.Instance) {
				in.Spec.Topology = nil
				delete(in.Spec.Components, common.ComponentEngine)
			},
			want: []string{"spec.components[engine]"},
		},
		{
			name:   "engine type other than postgresql",
			mutate: component(common.ComponentEngine, func(c *corev1alpha1.ComponentSpec) { c.Type = "mysql" }),
			want:   []string{"spec.components[engine].type"},
		},
		{
			name:   "unknown engine version",
			mutate: component(common.ComponentEngine, func(c *corev1alpha1.ComponentSpec) { c.Version = "9.0-1" }),
			want:   []string{"spec.components[engine].version"},
		},
		{
			name:   "engine replicas unset",
			mutate: component(common.ComponentEngine, func(c *corev1alpha1.ComponentSpec) { c.Replicas = nil }),
			want:   []string{"spec.components[engine].replicas"},
		},
		{
			name:   "zero engine replicas",
			mutate: component(common.ComponentEngine, func(c *corev1alpha1.ComponentSpec) { c.Replicas = new(int32(0)) }),
			want:   []string{"spec.components[engine].replicas"},
		},
		{
			name:   "negative proxy replicas",
			mutate: component(common.ComponentProxy, func(c *corev1alpha1.ComponentSpec) { c.Replicas = new(int32(-1)) }),
			want:   []string{"spec.components[proxy].replicas"},
		},
		{
			name: "wal_level the operator cannot run",
			mutate: component(common.ComponentEngine, func(c *corev1alpha1.ComponentSpec) {
				c.Parameters = &runtime.RawExtension{Raw: []byte(`{"configuration":"wal_level: minimal"}`)}
			}),
			want: []string{"spec.components[engine].parameters.configuration"},
		},
		{
			name: "proxy configuration that is not key/value pairs",
			mutate: component(common.ComponentProxy, func(c *corev1alpha1.ComponentSpec) {
				c.Parameters = &runtime.RawExtension{Raw: []byte(`{"configuration":"- a\n- b\n"}`)}
			}),
			want: []string{"spec.components[proxy].parameters.configuration"},
		},
		{
			name: "spread constraints without the operator's own",
			mutate: component(common.ComponentEngine, func(c *corev1alpha1.ComponentSpec) {
				c.SchedulingPolicy = spreadPolicy()
			}),
			want: []string{"spec.components[engine].schedulingPolicy.topologySpreadConstraints"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			in := validationtest.Load(t, clusterFixture)
			if tt.mutate != nil {
				tt.mutate(in)
			}

			res := v.Run(in, nil)

			var got []string
			for _, err := range res.Errors {
				got = append(got, err.Field)
			}
			assert.Equal(t, tt.want, got, "errors: %v", res.Errors)
		})
	}
}
