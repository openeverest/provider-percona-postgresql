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

	apicommon "github.com/openeverest/openeverest/v2/api/common/v1alpha1"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func spreadPolicy(constraints ...corev1.TopologySpreadConstraint) *apicommon.SchedulingPolicy {
	return &apicommon.SchedulingPolicy{TopologySpreadConstraints: &constraints}
}

func TestValidateSpreadConstraints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		policy  *apicommon.SchedulingPolicy
		wantErr string
	}{
		{name: "unset leaves the operator default"},
		{name: "policy without spread constraints", policy: &apicommon.SchedulingPolicy{}},
		{
			name:    "empty list cannot turn the operator default off",
			policy:  spreadPolicy(),
			wantErr: "empty topologySpreadConstraints list is not supported",
		},
		{
			name:    "soft hostname spread duplicates the operator's",
			policy:  spreadPolicy(corev1.TopologySpreadConstraint{MaxSkew: 1, TopologyKey: corev1.LabelHostname, WhenUnsatisfiable: corev1.ScheduleAnyway}),
			wantErr: "kubernetes.io/hostname with ScheduleAnyway is already set by the operator",
		},
		{
			name:    "soft zone spread duplicates the operator's",
			policy:  spreadPolicy(corev1.TopologySpreadConstraint{MaxSkew: 1, TopologyKey: corev1.LabelTopologyZone, WhenUnsatisfiable: corev1.ScheduleAnyway}),
			wantErr: "topology.kubernetes.io/zone with ScheduleAnyway is already set by the operator",
		},
		{
			name:   "strict hostname spread adds to the operator's",
			policy: spreadPolicy(corev1.TopologySpreadConstraint{MaxSkew: 1, TopologyKey: corev1.LabelHostname, WhenUnsatisfiable: corev1.DoNotSchedule}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateSpreadConstraints(tt.policy)
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestInstanceSpreadConstraints(t *testing.T) {
	t.Parallel()

	assert.Nil(t, instanceSpreadConstraints(nil, "db", "instance1"), "unset leaves spreading to the operator")

	strict := corev1.TopologySpreadConstraint{MaxSkew: 1, TopologyKey: corev1.LabelHostname, WhenUnsatisfiable: corev1.DoNotSchedule}
	got := instanceSpreadConstraints(spreadPolicy(strict), "db", "instance1")

	strict.LabelSelector = &metav1.LabelSelector{MatchLabels: map[string]string{
		labelCluster:     "db",
		labelInstanceSet: "instance1",
	}}
	assert.Equal(t, []corev1.TopologySpreadConstraint{strict}, got)
}
