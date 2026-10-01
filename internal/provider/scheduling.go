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
	"errors"
	"fmt"

	apicommon "github.com/openeverest/openeverest/v2/api/common/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	corev1 "k8s.io/api/core/v1"
)

// Labels the operator puts on instance pods; its naming package is internal.
const (
	labelCluster     = "postgres-operator.crunchydata.com/cluster"
	labelInstanceSet = "postgres-operator.crunchydata.com/instance-set"
)

// validateSpreadConstraints rejects spread constraints the operator cannot
// honour. It always adds soft spread across nodes and zones to instance pods,
// with no way to turn it off, and Kubernetes rejects two constraints with the
// same topologyKey and whenUnsatisfiable.
func validateSpreadConstraints(policy *apicommon.SchedulingPolicy) error {
	if policy == nil || policy.TopologySpreadConstraints == nil {
		return nil
	}
	constraints := *policy.TopologySpreadConstraints
	if len(constraints) == 0 {
		return errors.New("an empty topologySpreadConstraints list is not supported: the operator always spreads instances across nodes and zones")
	}
	for _, c := range constraints {
		if c.WhenUnsatisfiable == corev1.ScheduleAnyway &&
			(c.TopologyKey == corev1.LabelHostname || c.TopologyKey == corev1.LabelTopologyZone) {
			return fmt.Errorf("topologySpreadConstraints: %s with ScheduleAnyway is already set by the operator", c.TopologyKey)
		}
	}
	return nil
}

// instanceSpreadConstraints returns the user's spread constraints for the
// instance pods, or nil to leave spreading to the operator's own default.
func instanceSpreadConstraints(policy *apicommon.SchedulingPolicy, clusterName, instanceSet string) []corev1.TopologySpreadConstraint {
	if policy == nil || policy.TopologySpreadConstraints == nil {
		return nil
	}
	return controller.TopologySpreadConstraints(policy, map[string]string{
		labelCluster:     clusterName,
		labelInstanceSet: instanceSet,
	})
}
