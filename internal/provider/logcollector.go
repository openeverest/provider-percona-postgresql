package provider

import (
	"encoding/json"
	"fmt"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	pgv2 "github.com/percona/percona-postgresql-operator/v3/pkg/apis/pgv2.percona.com/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/openeverest/provider-percona-postgresql/definition/components"
)

// componentTypeFluentBit is the catalog entry for the log collector image.
const componentTypeFluentBit = "fluentbit"

// logCollectorEnabled returns the engine's logCollector.enabled parameter,
// nil when unset.
func logCollectorEnabled(engine corev1alpha1.ComponentSpec) (*bool, error) {
	if engine.Parameters == nil || len(engine.Parameters.Raw) == 0 {
		return nil, nil
	}
	params := &components.PostgresqlParameters{}
	if err := json.Unmarshal(engine.Parameters.Raw, params); err != nil {
		return nil, fmt.Errorf("decode engine component parameters: %w", err)
	}
	if params.LogCollector == nil {
		return nil, nil
	}
	return params.LogCollector.Enabled, nil
}

// applyLogCollector configures the operator's log collector. With enabled
// unset, the operator turns it on for a new cluster and keeps an existing
// cluster's state, so upgrading the provider doesn't restart existing
// instances. Without an image in the catalog, it's left out (off) unless
// explicitly enabled, which Validate rejects.
func applyLogCollector(cluster *pgv2.PerconaPGCluster, enabled *bool, image string) error {
	if image == "" {
		if enabled != nil && *enabled {
			return fmt.Errorf("cannot resolve the %s image from the versions catalog", componentTypeFluentBit)
		}
		return nil
	}
	cluster.Spec.LogCollector = &pgv2.LogCollectorSpec{
		Enabled: enabled,
		Image:   image,
		// The logs and logrotate sidecars (and the init container) share these.
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("50m"),
				corev1.ResourceMemory: resource.MustParse("64Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("350m"),
				corev1.ResourceMemory: resource.MustParse("200Mi"),
			},
		},
	}
	return nil
}
