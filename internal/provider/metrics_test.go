package provider

import (
	"context"
	"errors"
	"testing"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	monitoringv1alpha1 "github.com/openeverest/openeverest/v2/api/monitoring/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	"github.com/openeverest/provider-percona-postgresql/internal/common"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	upstreamv1beta1 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/upstream.pgv2.percona.com/v1beta1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const (
	testPostgresExporterImage  = "quay.io/prometheuscommunity/postgres-exporter:v0.20.1"
	testPGBouncerExporterImage = "quay.io/prometheuscommunity/pgbouncer-exporter:v0.12.1"
)

// newMetricsTestContext returns a Context for in whose client holds objs and
// the Provider, which pins the exporter images and declares the pmm
// integration.
func newMetricsTestContext(t *testing.T, in *corev1alpha1.Instance, objs ...client.Object) *controller.Context {
	t.Helper()
	k8s := fake.NewClientBuilder().WithScheme(metricsTestScheme(t)).WithObjects(append(objs, metricsTestProvider())...).Build()
	return controller.NewContext(t.Context(), k8s, in, common.ProviderName)
}

func metricsTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(s))
	require.NoError(t, corev1alpha1.AddToScheme(s))
	require.NoError(t, monitoringv1alpha1.AddToScheme(s))
	require.NoError(t, pgv2.AddToScheme(s))
	return s
}

func metricsTestProvider() *corev1alpha1.Provider {
	image := func(version, image string) corev1alpha1.ComponentType {
		return corev1alpha1.ComponentType{
			DefaultVersion: version,
			Versions:       []corev1alpha1.ComponentVersion{{Version: version, Image: image}},
		}
	}
	return &corev1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: common.ProviderName},
		Spec: corev1alpha1.ProviderSpec{
			ComponentTypes: map[string]corev1alpha1.ComponentType{
				postgresExporterComponentType:  image("0.20.1", testPostgresExporterImage),
				pgbouncerExporterComponentType: image("0.12.1", testPGBouncerExporterImage),
			},
			Monitoring: &corev1alpha1.ProviderMonitoring{
				Integrations: map[string]corev1alpha1.MonitoringIntegration{"pmm": {AgentVersions: ">=3.0.0 <4.0.0"}},
				ComponentTypes: map[string]corev1alpha1.MonitoringComponentType{
					componentTypePostgreSQL: {Kind: "postgresql", Metrics: corev1alpha1.MetricsSourceExporter},
					componentTypePGBouncer:  {Kind: "pgbouncer", Metrics: corev1alpha1.MetricsSourceExporter},
				},
			},
		},
	}
}

// metricsClass returns an ExtensionManaged class that requires metrics, like
// the prometheus and victoriametrics classes.
func metricsClass(name string) *monitoringv1alpha1.MonitoringClass {
	return &monitoringv1alpha1.MonitoringClass{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: monitoringv1alpha1.MonitoringClassSpec{
			ExtensionManaged: &monitoringv1alpha1.ExtensionManagedFulfilment{
				Requires: monitoringv1alpha1.SourceRequirements{Metrics: true},
			},
		},
	}
}

func extensionBinding(entry string) *monitoringv1alpha1.MonitoringBinding {
	return acceptedBinding(entry, entry, corev1alpha1.MonitoringExecutionModeExtensionManaged)
}

// desiredCluster returns the cluster as Sync has built it before the
// exporters are configured.
func desiredCluster() *pgv2.PerconaPGCluster {
	cluster := &pgv2.PerconaPGCluster{
		ObjectMeta: metav1.ObjectMeta{Name: pmmTestInstance, Namespace: pmmTestNamespace},
		Spec:       defaultSpec(),
	}
	cluster.Spec.Users = []upstreamv1beta1.PostgresUserSpec{{Name: defaultUsername}}
	return cluster
}

// liveClusterWithExporters returns the cluster as the operator runs it with
// the exporters on and the given number of ready pods per component.
func liveClusterWithExporters(postgresReady, pgbouncerReady int32) *pgv2.PerconaPGCluster {
	cluster := desiredCluster()
	cluster.Spec.InstanceSets[0].Sidecars = []corev1.Container{{Name: postgresExporterContainerName}}
	cluster.Spec.Proxy.PGBouncer.Sidecars = []corev1.Container{{Name: pgbouncerExporterContainerName}}
	cluster.Status.Postgres.Size = 3
	cluster.Status.Postgres.Ready = postgresReady
	cluster.Status.PGBouncer.Size = 2
	cluster.Status.PGBouncer.Ready = pgbouncerReady
	return cluster
}

func liveClusterWithoutExporters() *pgv2.PerconaPGCluster {
	cluster := desiredCluster()
	cluster.Status.Postgres.Ready = 3
	cluster.Status.PGBouncer.Ready = 2
	return cluster
}

func TestMetricsExportersRequired(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		entries []string
		objs    []client.Object
		want    bool
	}{
		"an ExtensionManaged binding whose class requires metrics requires them": {
			entries: []string{"prometheus"},
			objs:    []client.Object{metricsClass("prometheus"), extensionBinding("prometheus")},
			want:    true,
		},
		"a ProviderManaged pmm binding alone does not": {
			entries: []string{"pmm"},
			objs: []client.Object{pmmClass(), pmmDestination(), pmmTokenSecret(),
				acceptedBinding("pmm", "pmm", corev1alpha1.MonitoringExecutionModeProviderManaged)},
		},
		"no entries means no exporters, even when they run": {
			objs: []client.Object{liveClusterWithExporters(3, 2)},
		},
		"while an entry has no binding yet the live exporters are held": {
			entries: []string{"prometheus"},
			objs:    []client.Object{liveClusterWithExporters(3, 2)},
			want:    true,
		},
		"while an entry has no binding yet and nothing runs, nothing is added": {
			entries: []string{"prometheus"},
			objs:    []client.Object{liveClusterWithoutExporters()},
		},
		"switching backends keeps the live exporters until the new entry has a binding": {
			entries: []string{"victoriametrics"},
			objs: []client.Object{
				metricsClass("prometheus"), extensionBinding("prometheus"),
				metricsClass("victoriametrics"), liveClusterWithExporters(3, 2),
			},
			want: true,
		},
		"dropping the last metrics entry removes them": {
			entries: []string{"pmm"},
			objs: []client.Object{pmmClass(), pmmDestination(), pmmTokenSecret(),
				acceptedBinding("pmm", "pmm", corev1alpha1.MonitoringExecutionModeProviderManaged),
				metricsClass("prometheus"), extensionBinding("prometheus"), liveClusterWithExporters(3, 2)},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			c := newMetricsTestContext(t, monitoredInstance(tt.entries...), tt.objs...)

			got, err := metricsExportersRequired(c)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestConfigureMetricsExportersRendersBothExporters(t *testing.T) {
	t.Parallel()

	c := newMetricsTestContext(t, monitoredInstance("prometheus"), metricsClass("prometheus"), extensionBinding("prometheus"))
	cluster := desiredCluster()
	cluster.Spec.Port = new(int32(5433))
	cluster.Spec.Proxy.PGBouncer.Port = new(int32(6432))

	require.NoError(t, configureMetricsExporters(c, cluster))

	assert.Equal(t, []upstreamv1beta1.PostgresUserSpec{
		{Name: defaultUsername},
		{
			Name:     "everest-monitor",
			Options:  "SUPERUSER",
			Password: &upstreamv1beta1.PostgresPasswordSpec{Type: upstreamv1beta1.PostgresPasswordTypeAlphaNumeric},
		},
	}, cluster.Spec.Users)

	require.Len(t, cluster.Spec.InstanceSets[0].Sidecars, 1)
	pg := cluster.Spec.InstanceSets[0].Sidecars[0]
	assert.Equal(t, postgresExporterContainerName, pg.Name)
	assert.Equal(t, testPostgresExporterImage, pg.Image)
	assert.Equal(t, []string{"--web.listen-address=:9187"}, pg.Args)
	assert.Equal(t, []corev1.EnvVar{
		{Name: "DATA_SOURCE_URI", Value: "127.0.0.1:5433/postgres?sslmode=require"},
		secretEnv("DATA_SOURCE_USER", "orders-pguser-everest-monitor", "user"),
		secretEnv("DATA_SOURCE_PASS", "orders-pguser-everest-monitor", "password"),
	}, pg.Env)
	assert.Equal(t, []corev1.ContainerPort{{Name: "metrics", ContainerPort: 9187, Protocol: corev1.ProtocolTCP}}, pg.Ports)

	bouncer := cluster.Spec.Proxy.PGBouncer
	require.Len(t, bouncer.Sidecars, 1)
	pgb := bouncer.Sidecars[0]
	assert.Equal(t, pgbouncerExporterContainerName, pgb.Name)
	assert.Equal(t, testPGBouncerExporterImage, pgb.Image)
	assert.Equal(t, []string{
		"--pgBouncer.connectionString=postgres://127.0.0.1:6432/pgbouncer?sslmode=require",
		"--web.listen-address=:9127",
	}, pgb.Args)
	assert.Equal(t, []corev1.EnvVar{
		secretEnv("PGUSER", "orders-pguser-everest-monitor", "user"),
		secretEnv("PGPASSWORD", "orders-pguser-everest-monitor", "password"),
	}, pgb.Env)
	assert.Equal(t, []corev1.ContainerPort{{Name: "metrics", ContainerPort: 9127, Protocol: corev1.ProtocolTCP}}, pgb.Ports)
	assert.Equal(t, map[string]string{"stats_users": "everest-monitor", "auth_dbname": "postgres"}, bouncer.Config.Global)

	for _, sidecar := range []corev1.Container{pg, pgb} {
		sc := sidecar.SecurityContext
		require.NotNil(t, sc, sidecar.Name)
		assert.Equal(t, new(true), sc.RunAsNonRoot, sidecar.Name)
		assert.Equal(t, new(int64(65534)), sc.RunAsUser, "%s: the images run as the non-numeric user nobody", sidecar.Name)
		assert.Equal(t, new(false), sc.AllowPrivilegeEscalation, sidecar.Name)
		assert.Equal(t, new(true), sc.ReadOnlyRootFilesystem, sidecar.Name)
		assert.Equal(t, []corev1.Capability{"ALL"}, sc.Capabilities.Drop, sidecar.Name)
		assert.Equal(t, corev1.SeccompProfileTypeRuntimeDefault, sc.SeccompProfile.Type, sidecar.Name)
		assert.Equal(t, "64Mi", sidecar.Resources.Requests.Memory().String(), sidecar.Name)
	}
}

func TestConfigureMetricsExportersUsesDefaultPorts(t *testing.T) {
	t.Parallel()

	c := newMetricsTestContext(t, monitoredInstance("prometheus"), metricsClass("prometheus"), extensionBinding("prometheus"))
	cluster := desiredCluster()

	require.NoError(t, configureMetricsExporters(c, cluster))

	assert.Contains(t, cluster.Spec.InstanceSets[0].Sidecars[0].Env,
		corev1.EnvVar{Name: "DATA_SOURCE_URI", Value: "127.0.0.1:5432/postgres?sslmode=require"})
	assert.Contains(t, cluster.Spec.Proxy.PGBouncer.Sidecars[0].Args,
		"--pgBouncer.connectionString=postgres://127.0.0.1:5432/pgbouncer?sslmode=require")
}

func TestConfigureMetricsExportersLeavesTheClusterAloneWhenNotRequired(t *testing.T) {
	t.Parallel()

	c := newMetricsTestContext(t, monitoredInstance("pmm"), pmmClass(), pmmDestination(), pmmTokenSecret(),
		acceptedBinding("pmm", "pmm", corev1alpha1.MonitoringExecutionModeProviderManaged), liveClusterWithExporters(3, 2))
	cluster := desiredCluster()

	require.NoError(t, configureMetricsExporters(c, cluster))

	assert.Equal(t, desiredCluster().Spec, cluster.Spec)
}

func TestConfigureMetricsExportersNeedsTheImages(t *testing.T) {
	t.Parallel()

	provider := metricsTestProvider()
	delete(provider.Spec.ComponentTypes, pgbouncerExporterComponentType)
	k8s := fake.NewClientBuilder().WithScheme(metricsTestScheme(t)).
		WithObjects(provider, metricsClass("prometheus"), extensionBinding("prometheus")).Build()
	c := controller.NewContext(t.Context(), k8s, monitoredInstance("prometheus"), common.ProviderName)

	err := configureMetricsExporters(c, desiredCluster())
	assert.EqualError(t, err, `provider spec has no image for component type "pgbouncerExporter"`)
}

func TestMonitoringSources(t *testing.T) {
	t.Parallel()

	p := New()
	engine := corev1alpha1.MetricsEndpoint{
		Component: "engine",
		Kind:      "postgresql",
		PodSelector: map[string]string{
			"core.openeverest.io/instance":  pmmTestInstance,
			"core.openeverest.io/component": "engine",
		},
		Port:   intstr.FromString("metrics"),
		Path:   "/metrics",
		Scheme: "http",
	}
	proxy := corev1alpha1.MetricsEndpoint{
		Component: "proxy",
		Kind:      "pgbouncer",
		PodSelector: map[string]string{
			"core.openeverest.io/instance":  pmmTestInstance,
			"core.openeverest.io/component": "proxy",
		},
		Port:   intstr.FromString("metrics"),
		Path:   "/metrics",
		Scheme: "http",
	}

	tests := map[string]struct {
		live *pgv2.PerconaPGCluster
		want []corev1alpha1.MetricsEndpoint
	}{
		"one endpoint per component once both serve": {
			live: liveClusterWithExporters(3, 2),
			want: []corev1alpha1.MetricsEndpoint{engine, proxy},
		},
		"a rolling restart keeps the endpoints of components with a ready pod": {
			live: liveClusterWithExporters(2, 1),
			want: []corev1alpha1.MetricsEndpoint{engine, proxy},
		},
		"PgBouncer without ready pods, e.g. scaled to 0": {
			live: liveClusterWithExporters(3, 0),
			want: []corev1alpha1.MetricsEndpoint{engine},
		},
		"no ready PostgreSQL pod": {
			live: liveClusterWithExporters(0, 2),
			want: []corev1alpha1.MetricsEndpoint{proxy},
		},
		"no exporters": {
			live: liveClusterWithoutExporters(),
		},
		"before the cluster exists": {},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var objs []client.Object
			if tt.live != nil {
				objs = append(objs, tt.live)
			}
			c := newMetricsTestContext(t, monitoredInstance("prometheus"), objs...)

			got, err := p.MonitoringSources(c)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got.Metrics)
		})
	}

	t.Run("an API error reading the cluster is returned, not published as no endpoints", func(t *testing.T) {
		t.Parallel()

		k8s := fake.NewClientBuilder().WithScheme(metricsTestScheme(t)).WithObjects(liveClusterWithExporters(3, 2)).Build()
		funcs := interceptor.Funcs{Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*pgv2.PerconaPGCluster); ok {
				return errors.New("etcd unavailable")
			}
			return cl.Get(ctx, key, obj, opts...)
		}}
		c := controller.NewContext(t.Context(), interceptor.NewClient(k8s, funcs), monitoredInstance("prometheus"), common.ProviderName)

		got, err := p.MonitoringSources(c)
		require.ErrorContains(t, err, "etcd unavailable")
		assert.Nil(t, got)
	})
}
