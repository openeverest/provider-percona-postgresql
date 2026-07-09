package provider

import (
	corev1 "k8s.io/api/core/v1"

	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	"k8s.io/apimachinery/pkg/api/resource"
)

// defaultSpec provides a minimal valid starting point for PerconaPGCluster.
func defaultSpec() pgv2.PerconaPGClusterSpec {
	return pgv2.PerconaPGClusterSpec{
		InstanceSets: pgv2.PGInstanceSets{
			{
				Name: "instance1",
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{},
				},
			},
		},
		PMM: nil,
		Proxy: &pgv2.PGProxySpec{
			PGBouncer: &pgv2.PGBouncerSpec{
				Resources: corev1.ResourceRequirements{
					// XXX: Remove this once templates will be available
					Limits: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("128Mi"),
						corev1.ResourceCPU:    resource.MustParse("200m"),
					},
				},
			},
		},
	}
}
