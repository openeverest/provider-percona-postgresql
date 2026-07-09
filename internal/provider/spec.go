package provider

import (
	corev1 "k8s.io/api/core/v1"

	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	"k8s.io/apimachinery/pkg/api/resource"
)

// defaultSpec provides a minimal valid starting point for PerconaPGCluster.
func defaultSpec() pgv2.PerconaPGClusterSpec {
	backupsEnabled := false

	return pgv2.PerconaPGClusterSpec{
		PostgresVersion: 16,
		InstanceSets: pgv2.PGInstanceSets{
			{
				Name: "instance1",
				DataVolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("1Gi"),
						},
					},
				},
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
		Backups: pgv2.Backups{
			Enabled: &backupsEnabled,
		},
	}
}
