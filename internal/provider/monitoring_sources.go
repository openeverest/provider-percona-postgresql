package provider

import (
	"fmt"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/openeverest/provider-percona-postgresql/internal/common"
)

var _ controller.MonitoringSourcesProvider = (*Provider)(nil)

// MonitoringSources implements controller.MonitoringSourcesProvider. While
// the exporters run, one endpoint per component is published, selected by the
// labels labelPods sets, once the operator reports a ready pod for that
// component, so a rolling restart or scale keeps serving endpoints published.
func (p *Provider) MonitoringSources(c *controller.Context) (*corev1alpha1.MonitoringSources, error) {
	cluster, err := getLiveCluster(c)
	if err != nil {
		return nil, err
	}
	s := &corev1alpha1.MonitoringSources{}
	if hasPostgresExporter(cluster) && cluster.Status.Postgres.Ready > 0 {
		s.Metrics = append(s.Metrics, metricsEndpoint(c, common.ComponentEngine, componentTypePostgreSQL))
	}
	if hasPGBouncerExporter(cluster) && cluster.Status.PGBouncer.Ready > 0 {
		s.Metrics = append(s.Metrics, metricsEndpoint(c, common.ComponentProxy, componentTypePGBouncer))
	}
	return s, nil
}

// metricsEndpoint is where the exporter of a component listens. The runtime
// overwrites kind from the component type the Provider declares.
func metricsEndpoint(c *controller.Context, component, kind string) corev1alpha1.MetricsEndpoint {
	return corev1alpha1.MetricsEndpoint{
		Component: component,
		Kind:      kind,
		PodSelector: map[string]string{
			controller.InstanceLabel:  c.Name(),
			controller.ComponentLabel: component,
		},
		Port:   intstr.FromString(metricsPortName),
		Path:   metricsPath,
		Scheme: "http",
	}
}

// getLiveCluster returns the PerconaPGCluster as it is now, empty before it
// exists.
func getLiveCluster(c *controller.Context) (*pgv2.PerconaPGCluster, error) {
	live := &pgv2.PerconaPGCluster{}
	if err := c.Get(live, c.Name()); err != nil && !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("get PerconaPGCluster %q: %w", c.Name(), err)
	}
	return live, nil
}
