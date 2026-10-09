package provider

import (
	"testing"

	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	"github.com/stretchr/testify/assert"
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
