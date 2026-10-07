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
	"fmt"
	"slices"

	apicommon "github.com/openeverest/openeverest/v2/api/common/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	"github.com/openeverest/provider-percona-postgresql/internal/common"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	crunchyv1beta1 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/upstream.pgv2.percona.com/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
)

// Labels the operator puts on instance pods; its naming package is internal.
const (
	labelCluster     = "postgres-operator.crunchydata.com/cluster"
	labelInstanceSet = "postgres-operator.crunchydata.com/instance-set"
)

// operatorSpreadKeys are the topology keys of the soft spread the operator
// always adds to instance pods, with no way to turn it off.
var operatorSpreadKeys = []string{corev1.LabelHostname, corev1.LabelTopologyZone}

func operatorSpread(topologyKey string) corev1.TopologySpreadConstraint {
	return corev1.TopologySpreadConstraint{
		MaxSkew:           1,
		TopologyKey:       topologyKey,
		WhenUnsatisfiable: corev1.ScheduleAnyway,
	}
}

func collidesWithOperatorSpread(c corev1.TopologySpreadConstraint) bool {
	return c.WhenUnsatisfiable == corev1.ScheduleAnyway && slices.Contains(operatorSpreadKeys, c.TopologyKey)
}

// validateSpreadConstraints makes a user list state every constraint that
// applies: it must include the operator's soft spread across nodes and zones
// as is, since Kubernetes rejects two constraints with the same topologyKey and
// whenUnsatisfiable.
func validateSpreadConstraints(policy *apicommon.SchedulingPolicy) error {
	if policy == nil || policy.TopologySpreadConstraints == nil {
		return nil
	}
	found := map[string]bool{}
	for _, c := range *policy.TopologySpreadConstraints {
		if !collidesWithOperatorSpread(c) {
			continue
		}
		if !equality.Semantic.DeepEqual(c, operatorSpread(c.TopologyKey)) {
			return fmt.Errorf("topologySpreadConstraints: %s with ScheduleAnyway is fixed by the operator as {maxSkew: 1} without a selector", c.TopologyKey)
		}
		found[c.TopologyKey] = true
	}
	for _, key := range operatorSpreadKeys {
		if !found[key] {
			return fmt.Errorf("topologySpreadConstraints must include {topologyKey: %s, whenUnsatisfiable: ScheduleAnyway, maxSkew: 1}: the operator always applies it", key)
		}
	}
	return nil
}

// instanceSpreadConstraints returns the spread constraints to hand to the
// operator: the user's list without the two it adds itself, or nil to leave
// spreading to its default.
func instanceSpreadConstraints(policy *apicommon.SchedulingPolicy, clusterName, instanceSet string) []corev1.TopologySpreadConstraint {
	if policy == nil || policy.TopologySpreadConstraints == nil {
		return nil
	}
	var own []corev1.TopologySpreadConstraint
	for _, c := range *policy.TopologySpreadConstraints {
		if !collidesWithOperatorSpread(c) {
			own = append(own, c)
		}
	}
	return controller.TopologySpreadConstraints(&apicommon.SchedulingPolicy{TopologySpreadConstraints: &own}, map[string]string{
		labelCluster:     clusterName,
		labelInstanceSet: instanceSet,
	})
}

// labelPods labels every component's pods so the runtime reports on them in
// the Instance status. The operator adds them to the pod templates only, never
// to the StatefulSet or Deployment selectors.
func labelPods(c *controller.Context, cluster *pgv2.PerconaPGCluster) {
	for i := range cluster.Spec.InstanceSets {
		cluster.Spec.InstanceSets[i].Metadata = &crunchyv1beta1.Metadata{Labels: c.PodLabels(common.ComponentEngine)}
	}
	cluster.Spec.Proxy.PGBouncer.Metadata = &crunchyv1beta1.Metadata{Labels: c.PodLabels(common.ComponentProxy)}
}
