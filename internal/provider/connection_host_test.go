package provider

import (
	"testing"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
)

func clusterWithPGBouncerExpose(exposeType string) *pgv2.PerconaPGCluster {
	cluster := &pgv2.PerconaPGCluster{}
	cluster.Spec.Proxy = &pgv2.PGProxySpec{PGBouncer: &pgv2.PGBouncerSpec{}}
	if exposeType != "" {
		cluster.Spec.Proxy.PGBouncer.ServiceExpose = &pgv2.ServiceExpose{Type: exposeType}
	}
	return cluster
}

func TestPGBouncerLoadBalancerExposed(t *testing.T) {
	t.Parallel()

	assert.True(t, pgBouncerLoadBalancerExposed(clusterWithPGBouncerExpose("LoadBalancer")))
	assert.False(t, pgBouncerLoadBalancerExposed(clusterWithPGBouncerExpose("ClusterIP")))
	assert.False(t, pgBouncerLoadBalancerExposed(clusterWithPGBouncerExpose("")))
	assert.False(t, pgBouncerLoadBalancerExposed(&pgv2.PerconaPGCluster{}))
}

func TestPGBouncerEnabled(t *testing.T) {
	t.Parallel()

	withReplicas := func(replicas *int32) *pgv2.PerconaPGCluster {
		cluster := clusterWithPGBouncerExpose("")
		cluster.Spec.Proxy.PGBouncer.Replicas = replicas
		return cluster
	}
	zero, two := int32(0), int32(2)

	assert.False(t, pgBouncerEnabled(&pgv2.PerconaPGCluster{}))
	assert.True(t, pgBouncerEnabled(withReplicas(nil)))
	assert.False(t, pgBouncerEnabled(withReplicas(&zero)))
	assert.True(t, pgBouncerEnabled(withReplicas(&two)))
}

func TestReplaceURIHost(t *testing.T) {
	t.Parallel()

	got := replaceURIHost(
		"postgresql://app:p%40ss@db-pgbouncer.ns.svc:5432/app?sslmode=require",
		"203.0.113.10",
	)
	assert.Equal(t, "postgresql://app:p%40ss@203.0.113.10:5432/app?sslmode=require", got)
}

func TestReplaceURIHost_KeepsURIWhenUnparseable(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "://bad", replaceURIHost("://bad", "203.0.113.10"))
	assert.Empty(t, replaceURIHost("", "203.0.113.10"))
}

func TestApplyServiceExpose(t *testing.T) {
	t.Parallel()

	service := func(serviceType corev1.ServiceType) *corev1alpha1.Service {
		return &corev1alpha1.Service{ServiceType: serviceType}
	}

	tests := []struct {
		name   string
		engine *corev1alpha1.Service
		proxy  *corev1alpha1.Service
		want   *pgv2.ServiceExpose
	}{
		{
			name: "no service leaves the operator default",
		},
		{
			name:  "proxy ClusterIP",
			proxy: service(corev1.ServiceTypeClusterIP),
			want:  &pgv2.ServiceExpose{Type: "ClusterIP"},
		},
		{
			name:  "proxy NodePort",
			proxy: service(corev1.ServiceTypeNodePort),
			want:  &pgv2.ServiceExpose{Type: "NodePort"},
		},
		{
			name: "proxy LoadBalancer with annotations and source ranges",
			proxy: &corev1alpha1.Service{
				ServiceType: corev1.ServiceTypeLoadBalancer,
				Annotations: map[string]string{"a": "b"},
				LoadBalancerService: &corev1alpha1.LoadBalancerService{
					SourceRanges: corev1alpha1.SourceRanges{"10.0.0.0/8"},
				},
			},
			want: &pgv2.ServiceExpose{
				Type:                     "LoadBalancer",
				Annotations:              map[string]string{"a": "b"},
				LoadBalancerSourceRanges: []string{"10.0.0.0/8"},
			},
		},
		{
			name:   "engine service applies when proxy has none",
			engine: service(corev1.ServiceTypeLoadBalancer),
			want:   &pgv2.ServiceExpose{Type: "LoadBalancer"},
		},
		{
			name:   "proxy service wins over engine service",
			engine: service(corev1.ServiceTypeLoadBalancer),
			proxy:  service(corev1.ServiceTypeClusterIP),
			want:   &pgv2.ServiceExpose{Type: "ClusterIP"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cluster := clusterWithPGBouncerExpose("")
			applyServiceExpose(cluster,
				corev1alpha1.ComponentSpec{Service: tt.engine},
				corev1alpha1.ComponentSpec{Service: tt.proxy},
			)
			assert.Equal(t, tt.want, cluster.Spec.Proxy.PGBouncer.ServiceExpose)
		})
	}
}
