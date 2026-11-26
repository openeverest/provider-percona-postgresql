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

	apicommon "github.com/openeverest/openeverest/v2/api/common/v1alpha1"
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

// schedules gives the Instance one backup storage with the given schedules.
func schedules(list ...corev1alpha1.InstanceBackupSchedule) func(*corev1alpha1.Instance) {
	return func(in *corev1alpha1.Instance) {
		in.Spec.Backup = &corev1alpha1.InstanceBackupSpec{
			Enabled:  true,
			ClassRef: apicommon.ObjectRef{Name: "pg"},
			Storages: []corev1alpha1.InstanceBackupStorage{{
				StorageRef: apicommon.ObjectRef{Name: "minio"},
				Schedules:  list,
			}},
		}
	}
}

// keep is count retention of n backups.
func keep(n int32) *corev1alpha1.BackupScheduleRetention {
	return &corev1alpha1.BackupScheduleRetention{Type: corev1alpha1.BackupScheduleRetentionTypeCount, Count: &n}
}

// window is time-based retention over duration d (e.g. "7d", "4w").
func window(d string) *corev1alpha1.BackupScheduleRetention {
	return &corev1alpha1.BackupScheduleRetention{Type: corev1alpha1.BackupScheduleRetentionTypeTime, Duration: d}
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
			name: "pg_hba record with the trust method",
			mutate: component(common.ComponentEngine, func(c *corev1alpha1.ComponentSpec) {
				c.Parameters = &runtime.RawExtension{Raw: []byte(`{"pgHBA":"hostssl app app 10.0.0.0/8 scram-sha-256\nhost all all all trust"}`)}
			}),
			want: []string{"spec.components[engine].parameters.pgHBA"},
		},
		{
			name: "log collector turned off",
			mutate: component(common.ComponentEngine, func(c *corev1alpha1.ComponentSpec) {
				c.Parameters = &runtime.RawExtension{Raw: []byte(`{"logCollector":{"enabled":false}}`)}
			}),
		},
		{
			name: "log collector turned on",
			mutate: component(common.ComponentEngine, func(c *corev1alpha1.ComponentSpec) {
				c.Parameters = &runtime.RawExtension{Raw: []byte(`{"logCollector":{"enabled":true}}`)}
			}),
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
		{
			name: "schedule for each backup type",
			mutate: schedules(
				corev1alpha1.InstanceBackupSchedule{Name: "weekly-full", Enabled: true, Cron: "0 1 * * 0", Parameters: raw(`{"type":"full"}`)},
				corev1alpha1.InstanceBackupSchedule{Name: "weekday-diff", Enabled: true, Cron: "0 1 * * 1-6", Retention: keep(3), Parameters: raw(`{"type":"diff"}`)},
				corev1alpha1.InstanceBackupSchedule{Name: "hourly-incr", Enabled: true, Cron: "0 * * * *", Parameters: raw(`{"type":"incr"}`)},
			),
		},
		{
			name: "schedule name is an arbitrary user-facing identifier, not a type",
			mutate: schedules(
				corev1alpha1.InstanceBackupSchedule{Name: "backup-g9y", Enabled: true, Cron: "0 1 * * *", Parameters: raw(`{"type":"full"}`)},
			),
		},
		{
			name: "two enabled schedules of the same effective type on a storage",
			mutate: schedules(
				corev1alpha1.InstanceBackupSchedule{Name: "weekly-full", Enabled: true, Cron: "0 1 * * 0"},
				corev1alpha1.InstanceBackupSchedule{Name: "monthly-full", Enabled: true, Cron: "0 1 1 * *"},
			),
			want: []string{"spec.backup.storages[0].schedules[1].parameters.type"},
		},
		{
			name: "a disabled duplicate-type schedule is not flagged",
			mutate: schedules(
				corev1alpha1.InstanceBackupSchedule{Name: "weekly-full", Enabled: true, Cron: "0 1 * * 0"},
				corev1alpha1.InstanceBackupSchedule{Name: "monthly-full", Enabled: false, Cron: "0 1 1 * *"},
			),
		},
		{
			name: "duplicate schedule name",
			mutate: schedules(
				corev1alpha1.InstanceBackupSchedule{Name: "backup-g9y", Enabled: true, Cron: "0 1 * * *"},
				corev1alpha1.InstanceBackupSchedule{Name: "backup-g9y", Enabled: true, Cron: "0 1 * * 0", Parameters: raw(`{"type":"diff"}`)},
			),
			want: []string{"spec.backup.storages[0].schedules[1].name"},
		},
		{
			name: "schedule parameters with an unknown type",
			mutate: schedules(corev1alpha1.InstanceBackupSchedule{
				Name: "backup-g9y", Enabled: true, Cron: "0 1 * * *", Parameters: raw(`{"type":"weekly"}`),
			}),
			want: []string{"spec.backup.storages[0].schedules[0].parameters"},
		},
		{
			name: "schedule parameters with an undeclared field",
			mutate: schedules(corev1alpha1.InstanceBackupSchedule{
				Name: "backup-g9y", Enabled: true, Cron: "0 1 * * *", Parameters: raw(`{"compress":true}`),
			}),
			want: []string{"spec.backup.storages[0].schedules[0].parameters"},
		},
		{
			name: "time retention on a diff schedule",
			mutate: schedules(
				corev1alpha1.InstanceBackupSchedule{Name: "nightly-diff", Enabled: true, Cron: "0 1 * * *", Retention: window("7d"), Parameters: raw(`{"type":"diff"}`)},
			),
			want: []string{"spec.backup.storages[0].schedules[0].retention.type"},
		},
		{
			name: "count retention on a diff schedule is fine",
			mutate: schedules(
				corev1alpha1.InstanceBackupSchedule{Name: "nightly-diff", Enabled: true, Cron: "0 1 * * *", Retention: keep(5), Parameters: raw(`{"type":"diff"}`)},
			),
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
