package provider

import (
	"fmt"

	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	upstreamv1beta1 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/upstream.pgv2.percona.com/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	// Component types in versions.yaml that pin the exporter images.
	postgresExporterComponentType  = "postgresExporter"
	pgbouncerExporterComponentType = "pgbouncerExporter"

	postgresExporterContainerName  = "postgres-exporter"
	pgbouncerExporterContainerName = "pgbouncer-exporter"
	postgresExporterPort           = 9187
	pgbouncerExporterPort          = 9127
	metricsPortName                = "metrics"
	metricsPath                    = "/metrics"

	// monitorUsername is the login both exporters use. The operator reserves
	// "monitor" for PMM and only creates it while PMM is enabled, and it
	// applies user options with ALTER ROLE, which cannot grant pg_monitor, so
	// this is a superuser like the operator's own PMM user.
	monitorUsername = "everest-monitor"

	// The PgBouncer admin console has no backing database to run auth_query
	// in, so it is pointed at "postgres", where the operator installs the
	// auth function as in every database. stats_users lets the exporter run
	// the read-only SHOW commands.
	pgbouncerStatsUsersSetting = "stats_users"
	pgbouncerAuthDBNameSetting = "auth_dbname"
	pgbouncerAuthDBName        = "postgres"

	defaultPostgresPort  int32 = 5432
	defaultPGBouncerPort int32 = 5432

	// exporterUID is the "nobody" user both exporter images run as. It is set
	// numerically so runAsNonRoot can be verified.
	exporterUID int64 = 65534
)

var metricsExporterResources = corev1.ResourceRequirements{
	Requests: corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("50m"),
		corev1.ResourceMemory: resource.MustParse("64Mi"),
	},
}

// monitorUserSecretName is the Secret the operator generates for the monitor
// user, following its <cluster>-pguser-<user> convention.
func monitorUserSecretName(clusterName string) string {
	return clusterName + "-pguser-" + monitorUsername
}

// configureMetricsExporters runs postgres_exporter next to every PostgreSQL
// pod and pgbouncer_exporter next to every PgBouncer pod while a monitoring
// class bound to the Instance requires metrics, together with the login they
// share. It must run after cluster.Spec.Users is set.
func configureMetricsExporters(c *controller.Context, cluster *pgv2.PerconaPGCluster) error {
	required, err := metricsExportersRequired(c)
	if err != nil || !required {
		return err
	}

	spec, err := c.ProviderSpec()
	if err != nil {
		return fmt.Errorf("get provider spec: %w", err)
	}
	postgresImage := controller.GetDefaultImage(spec, postgresExporterComponentType)
	if postgresImage == "" {
		return fmt.Errorf("provider spec has no image for component type %q", postgresExporterComponentType)
	}
	pgbouncerImage := controller.GetDefaultImage(spec, pgbouncerExporterComponentType)
	if pgbouncerImage == "" {
		return fmt.Errorf("provider spec has no image for component type %q", pgbouncerExporterComponentType)
	}

	secretName := monitorUserSecretName(c.Name())
	cluster.Spec.Users = append(cluster.Spec.Users, upstreamv1beta1.PostgresUserSpec{
		Name:    monitorUsername,
		Options: "SUPERUSER",
		Password: &upstreamv1beta1.PostgresPasswordSpec{
			Type: upstreamv1beta1.PostgresPasswordTypeAlphaNumeric,
		},
	})

	postgresPort := defaultPostgresPort
	if cluster.Spec.Port != nil {
		postgresPort = *cluster.Spec.Port
	}
	for i := range cluster.Spec.InstanceSets {
		set := &cluster.Spec.InstanceSets[i]
		set.Sidecars = append(set.Sidecars, postgresExporterSidecar(postgresImage, secretName, postgresPort))
	}

	if cluster.Spec.Proxy == nil || cluster.Spec.Proxy.PGBouncer == nil {
		return nil
	}
	bouncer := cluster.Spec.Proxy.PGBouncer
	bouncerPort := defaultPGBouncerPort
	if bouncer.Port != nil {
		bouncerPort = *bouncer.Port
	}
	bouncer.Sidecars = append(bouncer.Sidecars, pgbouncerExporterSidecar(pgbouncerImage, secretName, bouncerPort))
	if bouncer.Config.Global == nil {
		bouncer.Config.Global = map[string]string{}
	}
	bouncer.Config.Global[pgbouncerStatsUsersSetting] = monitorUsername
	bouncer.Config.Global[pgbouncerAuthDBNameSetting] = pgbouncerAuthDBName
	return nil
}

// metricsExportersRequired says whether the exporters run this pass: when a
// bound class requires metrics or, while that is unresolved, when the live
// cluster already runs them, since removing and re-adding a sidecar would roll
// the pods twice for nothing.
func metricsExportersRequired(c *controller.Context) (bool, error) {
	req, err := c.RequiredSources()
	if err != nil {
		return false, err
	}
	live, err := getLiveCluster(c)
	if err != nil {
		return false, err
	}
	return req.Metrics.Else(hasPostgresExporter(live)), nil
}

// hasPostgresExporter reports whether the cluster runs postgres_exporter. Both
// exporters are always added together, so this answers for both.
func hasPostgresExporter(cluster *pgv2.PerconaPGCluster) bool {
	for _, set := range cluster.Spec.InstanceSets {
		if hasContainer(set.Sidecars, postgresExporterContainerName) {
			return true
		}
	}
	return false
}

// hasPGBouncerExporter reports whether the cluster runs pgbouncer_exporter.
func hasPGBouncerExporter(cluster *pgv2.PerconaPGCluster) bool {
	if cluster.Spec.Proxy == nil || cluster.Spec.Proxy.PGBouncer == nil {
		return false
	}
	return hasContainer(cluster.Spec.Proxy.PGBouncer.Sidecars, pgbouncerExporterContainerName)
}

func hasContainer(containers []corev1.Container, name string) bool {
	for _, container := range containers {
		if container.Name == name {
			return true
		}
	}
	return false
}

// postgresExporterSidecar scrapes the PostgreSQL server in its own pod. The
// operator's pg_hba only admits TCP clients over TLS with a password, so the
// exporter connects to the loopback address with sslmode=require.
func postgresExporterSidecar(image, secretName string, port int32) corev1.Container {
	return exporterSidecar(postgresExporterContainerName, image, postgresExporterPort,
		[]string{fmt.Sprintf("--web.listen-address=:%d", postgresExporterPort)},
		[]corev1.EnvVar{
			{Name: "DATA_SOURCE_URI", Value: fmt.Sprintf("127.0.0.1:%d/postgres?sslmode=require", port)},
			secretEnv("DATA_SOURCE_USER", secretName, "user"),
			secretEnv("DATA_SOURCE_PASS", secretName, "password"),
		},
	)
}

// pgbouncerExporterSidecar scrapes the PgBouncer admin console in its own pod.
// The operator configures PgBouncer to require TLS from clients.
func pgbouncerExporterSidecar(image, secretName string, port int32) corev1.Container {
	return exporterSidecar(pgbouncerExporterContainerName, image, pgbouncerExporterPort,
		[]string{
			fmt.Sprintf("--pgBouncer.connectionString=postgres://127.0.0.1:%d/pgbouncer?sslmode=require", port),
			fmt.Sprintf("--web.listen-address=:%d", pgbouncerExporterPort),
		},
		// libpq-style variables fill in the login the connection string
		// leaves out.
		[]corev1.EnvVar{
			secretEnv("PGUSER", secretName, "user"),
			secretEnv("PGPASSWORD", secretName, "password"),
		},
	)
}

// exporterSidecar builds an exporter container with the restricted security
// context the operator gives its own containers, which it does not apply to
// sidecars.
func exporterSidecar(name, image string, port int32, args []string, env []corev1.EnvVar) corev1.Container {
	return corev1.Container{
		Name:  name,
		Image: image,
		Args:  args,
		// Credentials go through the environment so they do not show in the
		// process list.
		Env: env,
		Ports: []corev1.ContainerPort{{
			Name:          metricsPortName,
			ContainerPort: port,
			Protocol:      corev1.ProtocolTCP,
		}},
		Resources: metricsExporterResources,
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: new(false),
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
			Privileged:               new(false),
			ReadOnlyRootFilesystem:   new(true),
			RunAsNonRoot:             new(true),
			RunAsUser:                new(exporterUID),
			SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
		},
	}
}

func secretEnv(name, secretName, key string) corev1.EnvVar {
	return corev1.EnvVar{
		Name: name,
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
				Key:                  key,
			},
		},
	}
}
