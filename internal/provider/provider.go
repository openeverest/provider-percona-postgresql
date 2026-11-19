package provider

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	"github.com/openeverest/provider-percona-postgresql/definition/backupclasses"
	"github.com/openeverest/provider-percona-postgresql/internal/common"
	pgv2 "github.com/percona/percona-postgresql-operator/v3/pkg/apis/pgv2.percona.com/v2"
	upstreamv1beta1 "github.com/percona/percona-postgresql-operator/v3/pkg/apis/upstream.pgv2.percona.com/v1beta1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	componentTypePostgreSQL = "postgresql"
	componentTypePGBouncer  = "pgbouncer"
	componentTypePGBackRest = "pgbackrest"

	// Finalizers applied to the PerconaPGCluster resource to ensure proper
	// cleanup of dependent resources when the cluster is deleted.
	// NOTE: percona.com/delete-backups is intentionally omitted. Backup data
	// deletion is managed per-Backup via CleanupBackup, respecting each
	// Backup's DeletionPolicy (Delete vs Retain).
	finalizerDeleteSSL = "percona.com/delete-ssl"
	finalizerDeletePVC = "percona.com/delete-pvc"
	defaultUsername    = "postgres"
)

// Compile-time check that Provider implements the required interface.
var _ controller.ProviderInterface = (*Provider)(nil)

// Provider implements controller.ProviderInterface for the provider-percona-postgresql provider.
type Provider struct {
	controller.BaseProvider
}

// New creates a new Provider instance.
func New() *Provider {
	p := &Provider{}

	p.BaseProvider = controller.BaseProvider{
		ProviderName: common.ProviderName,
		SchemeFuncs: []func(*runtime.Scheme) error{
			pgv2.AddToScheme,
			// StatefulSets and Deployments are read to tell whether the
			// cluster has rolled out, EndpointSlices whether it has a primary.
			appsv1.AddToScheme,
			discoveryv1.AddToScheme,
		},
		WatchConfigs: []controller.WatchConfig{
			controller.WatchOwned(&pgv2.PerconaPGCluster{}),
			// Watch operator backups so the PITR window published by
			// BackupStorageStatuses refreshes as the operator stamps
			// latestRestorableTime on them. Operator backups are not owned by
			// the Instance, so map them to the parent via spec.pgCluster.
			controller.WatchExternal(&pgv2.PerconaPGBackup{},
				handler.EnqueueRequestsFromMapFunc(enqueueOperatorBackupInstance()),
			),
			// Watch Restores so the Instance stops reporting a restore as soon
			// as one reaches a terminal state. Restores are not owned by the
			// Instance, so map them via spec.instanceRef; without this the
			// Instance would keep reporting it until an unrelated event
			// arrived.
			controller.WatchExternal(&backupv1alpha1.Restore{},
				handler.EnqueueRequestsFromMapFunc(enqueueRestoreInstance()),
			),
			// Watch the cluster's StatefulSets and PGBouncer Deployment: the
			// Instance has converged only once they have rolled out, which the
			// operator's own status can report too early. Watch the
			// EndpointSlices of the Patroni leader Service: Patroni moves them
			// to a new primary without the operator's status changing. None of
			// them is owned by the Instance, so map them by cluster.
			controller.WatchExternal(&appsv1.StatefulSet{},
				handler.EnqueueRequestsFromMapFunc(enqueueClusterInstance()),
			),
			controller.WatchExternal(&appsv1.Deployment{},
				handler.EnqueueRequestsFromMapFunc(enqueueClusterInstance()),
			),
			controller.WatchExternal(&discoveryv1.EndpointSlice{},
				handler.EnqueueRequestsFromMapFunc(enqueueClusterInstance()),
			),
		},
	}
	p.SetShippedBackupClasses(backupclasses.Shipped())

	return p
}

// Sync ensures all required resources exist and are configured correctly.
//
// This is the main reconciliation logic. Create or update your
// operator's custom resource(s) based on the Instance spec.
func (p *Provider) Sync(c *controller.Context) error {
	l := log.FromContext(c.Context())
	l.Info("Syncing instance", "name", c.Name())

	defer l.Info("PostgreSQL cluster synced", "cluster", c.Name())

	meta := c.ObjectMeta(c.Name())
	meta.Finalizers = []string{
		finalizerDeleteSSL,
		finalizerDeletePVC,
	}
	cluster := &pgv2.PerconaPGCluster{
		ObjectMeta: meta,
		Spec:       defaultSpec(),
	}

	providerSpec, err := c.ProviderSpec()
	if err != nil {
		return err
	}

	bundleComponents := map[string]string{}
	bundleName := controller.EffectiveVersionBundleName(providerSpec, c.Instance())
	if bundleName != "" {
		bundle, err := controller.ResolveVersionBundle(providerSpec, bundleName)
		if err != nil {
			return err
		}
		bundleComponents = bundle.Components
	}

	// Get the engine component spec
	engine := c.Instance().Spec.Components[common.ComponentEngine]
	engineReplicas, ok := componentReplicas(c.Instance(), common.ComponentEngine)
	if !ok {
		return fmt.Errorf("instance spec missing %q component replicas", common.ComponentEngine)
	}
	if len(cluster.Spec.InstanceSets) == 0 {
		cluster.Spec.InstanceSets = pgv2.PGInstanceSets{{Name: "instance1"}}
	}
	cluster.Spec.InstanceSets[0].Replicas = &engineReplicas
	if engine.Resources != nil {
		cluster.Spec.InstanceSets[0].Resources = *engine.Resources
	}
	defaultRequestsToLimits(&cluster.Spec.InstanceSets[0].Resources)
	if engine.Storage != nil {
		if cluster.Spec.InstanceSets[0].DataVolumeClaimSpec.Resources.Requests == nil {
			cluster.Spec.InstanceSets[0].DataVolumeClaimSpec.Resources.Requests = corev1.ResourceList{}
		}
		if !engine.Storage.Size.IsZero() {
			cluster.Spec.InstanceSets[0].DataVolumeClaimSpec.Resources.Requests[corev1.ResourceStorage] = engine.Storage.Size
		}
		if engine.Storage.StorageClass != nil && *engine.Storage.StorageClass != "" {
			cluster.Spec.InstanceSets[0].DataVolumeClaimSpec.StorageClassName = engine.Storage.StorageClass
		}
	}
	if engine.SchedulingPolicy != nil && engine.SchedulingPolicy.Affinity != nil {
		cluster.Spec.InstanceSets[0].Affinity = engine.SchedulingPolicy.Affinity
	}
	cluster.Spec.InstanceSets[0].TopologySpreadConstraints = instanceSpreadConstraints(engine.SchedulingPolicy, c.Name(), cluster.Spec.InstanceSets[0].Name)
	engineVersion := engine.Version
	if engineVersion == "" {
		engineVersion = bundleComponents[common.ComponentEngine]
	}

	if engine.Image != "" {
		cluster.Spec.Image = engine.Image
	}
	if parsedMajor, ok := parseMajorVersion(engineVersion); ok {
		cluster.Spec.PostgresVersion = parsedMajor
	}
	if cluster.Spec.Image == "" {
		if engineVersion != "" {
			cluster.Spec.Image = controller.GetImageForVersion(providerSpec, common.ComponentEngine, engineVersion)
		}
		if cluster.Spec.Image == "" {
			cluster.Spec.Image = controller.GetDefaultImage(providerSpec, componentTypePostgreSQL)
			if cluster.Spec.Image == "" {
				return fmt.Errorf("cannot resolve default postgres image from versions catalog")
			}
		}
	}
	if cluster.Spec.Backups.PGBackRest.Image == "" {
		if image := controller.GetDefaultImage(providerSpec, componentTypePGBackRest); image != "" {
			cluster.Spec.Backups.PGBackRest.Image = image
		} else {
			return fmt.Errorf("cannot resolve default pgbackrest image from versions catalog")
		}
	}

	proxy := c.Instance().Spec.Components[common.ComponentProxy]
	proxyReplicas, ok := componentReplicas(c.Instance(), common.ComponentProxy)
	if !ok {
		return fmt.Errorf("instance spec has invalid %q component; this should be caught by Validate", common.ComponentProxy)
	}

	proxyType := proxy.Type
	if proxyType == "" {
		proxyType = controller.GetComponentType(providerSpec, common.ComponentProxy)
	}
	if proxyType != controller.GetComponentType(providerSpec, common.ComponentProxy) {
		return fmt.Errorf("instance spec has unsupported %q component type %q; only %q is supported", common.ComponentProxy, proxyType, controller.GetComponentType(providerSpec, common.ComponentProxy))
	}
	cluster.Spec.Proxy.PGBouncer.Replicas = &proxyReplicas
	if proxy.Resources != nil {
		cluster.Spec.Proxy.PGBouncer.Resources = *proxy.Resources
	}
	defaultRequestsToLimits(&cluster.Spec.Proxy.PGBouncer.Resources)
	applyProxyScheduling(cluster.Spec.Proxy.PGBouncer, proxy.SchedulingPolicy, c.PodLabels(common.ComponentProxy))
	if proxy.Image != "" {
		cluster.Spec.Proxy.PGBouncer.Image = proxy.Image
	} else if cluster.Spec.Proxy.PGBouncer.Image == "" {
		proxyVersion := proxy.Version
		if proxyVersion == "" {
			proxyVersion = bundleComponents[common.ComponentProxy]
		}
		if proxyVersion != "" {
			cluster.Spec.Proxy.PGBouncer.Image = controller.GetImageForVersion(providerSpec, common.ComponentProxy, proxyVersion)
		}
		if cluster.Spec.Proxy.PGBouncer.Image == "" {
			image := controller.GetDefaultImage(providerSpec, componentTypePGBouncer)
			cluster.Spec.Proxy.PGBouncer.Image = image
		}
		if cluster.Spec.Proxy.PGBouncer.Image == "" {
			return fmt.Errorf("cannot resolve default pgbouncer image from versions catalog")
		}
	}
	labelPods(c, cluster)
	applyServiceExpose(cluster, engine, proxy)

	tls, err := tlsParameters(c.Instance())
	if err != nil {
		return err
	}
	configureTLS(cluster, tls)

	if err := applyEngineConfiguration(c, cluster); err != nil {
		return err
	}

	pgHBA, err := engineHBA(engine)
	if err != nil {
		return err
	}
	if err := applyPgHBA(cluster, pgHBA, tls); err != nil {
		return err
	}

	if err := applyProxyConfiguration(cluster, proxy); err != nil {
		return err
	}

	if err := applyPMM(c, cluster); err != nil {
		return err
	}

	if cluster.Spec.Proxy != nil && cluster.Spec.Proxy.PGBouncer != nil {
		cluster.Spec.Proxy.PGBouncer.ExposeSuperusers = true
	}
	cluster.Spec.Users = []upstreamv1beta1.PostgresUserSpec{
		{
			Name: defaultUsername,
			// need to specify it in oder to have secrets generated and pgbouncer-uri populated
			Databases: []upstreamv1beta1.PostgresIdentifier{"postgres"},
		},
	}

	// Automatically remove storages that have no schedules and no Backup CRs
	// referencing them. This frees repo slots for reuse.
	if _, err := pruneUnreferencedStorages(c); err != nil {
		return err
	}

	// applyBackupSettings may return a BackupConfigError when the backup
	// configuration is incomplete (e.g. enabled=true but no storages). We
	// capture this error and defer returning it until after the cluster has
	// been applied so that the reconciler can still call Status() and update
	// the Instance status. The reconciler treats BackupConfigError specially
	// by setting the BackupConfigured condition to False rather than failing
	// the pass.
	var backupConfigErr error
	if err := applyBackupSettings(c, cluster); err != nil {
		if controller.AsBackupConfigError(err) == nil {
			return err
		}
		backupConfigErr = err
		// Preserve the existing cluster's backup configuration so we
		// don't apply an inconsistent spec (e.g. enabled=true with
		// zero repos) or accidentally wipe a previously working setup.
		existing := &pgv2.PerconaPGCluster{}
		if getErr := c.Get(existing, c.Name()); getErr == nil {
			cluster.Spec.Backups = existing.Spec.Backups
			// The operator's backup/restore triggers must stay operator-owned.
			cluster.Spec.Backups.PGBackRest.Manual = nil
			cluster.Spec.Backups.PGBackRest.Restore = nil
			// Dropping it from the apply would delete it and reshuffle repo slots.
			if slots := loadRepoSlotMap(existing); slots != nil {
				saveRepoSlotMap(cluster, slots)
			}
		}
		// If the cluster doesn't exist yet, defaultSpec() already has
		// backups disabled which is safe.
	}

	// Skip applying the cluster spec while a restore is actively running.
	// The Percona restore controller and the upstream Crunchy operator need
	// exclusive control over the cluster during the restore process (shutdown,
	// data directory replacement, restart). Applying our desired spec during
	// this window can conflict with their changes and cause the restore to
	// appear successful without actually changing the data.
	restoring, err := isRestoreRunning(c)
	if err != nil {
		return err
	}
	if restoring {
		l.Info("Restore is in progress, skipping cluster apply", "cluster", c.Name())
		if _, err := c.ReconcileDataSource(); err != nil {
			return err
		}
		return backupConfigErr
	}

	if err := applyDataSourceBootstrap(c, cluster); err != nil {
		return err
	}

	if err := c.Apply(cluster); err != nil {
		return err
	}

	if _, err := c.ReconcileDataSource(); err != nil {
		return err
	}

	return backupConfigErr
}

// Status reports what the provider observes about the cluster.
func (p *Provider) Status(c *controller.Context) (controller.Status, error) {
	return observeStatus(c)
}

// isRestoreRunning reports whether a Restore of the Instance has not ended,
// including the one that seeds a new Instance from its data source.
func isRestoreRunning(c *controller.Context) (bool, error) {
	restores, err := c.RestoresForInstance()
	if err != nil {
		return false, fmt.Errorf("list restores for instance %q: %w", c.Name(), err)
	}

	for i := range restores {
		if restoreRunning(restores[i].Status.State) {
			return true, nil
		}
	}

	return false, nil
}

func defaultRequestsToLimits(res *corev1.ResourceRequirements) {
	if res == nil || len(res.Limits) == 0 {
		return
	}
	for name, limit := range res.Limits {
		if _, ok := res.Requests[name]; ok {
			continue
		}
		if res.Requests == nil {
			res.Requests = corev1.ResourceList{}
		}
		res.Requests[name] = limit
	}
}

func applyServiceExpose(cluster *pgv2.PerconaPGCluster, engine, proxy corev1alpha1.ComponentSpec) {
	if expose := buildServiceExpose(engine.Service); expose != nil {
		cluster.Spec.Expose = expose
	}
	if expose := buildServiceExpose(engine.Service); expose != nil {
		cluster.Spec.ExposeReplicas = expose
	}

	if expose := buildServiceExpose(proxy.Service); expose != nil {
		if cluster.Spec.Proxy != nil && cluster.Spec.Proxy.PGBouncer != nil {
			cluster.Spec.Proxy.PGBouncer.ServiceExpose = expose
		}
	}
}

func buildServiceExpose(svc *corev1alpha1.Service) *pgv2.ServiceExpose {
	if svc == nil {
		return nil
	}

	expose := &pgv2.ServiceExpose{
		Type: string(svc.ServiceType),
	}
	if len(svc.Annotations) > 0 {
		expose.Annotations = svc.Annotations
	}
	if svc.LoadBalancerService != nil {
		ranges := svc.LoadBalancerService.SourceRanges.NormalizedSourceRanges()
		if len(ranges) > 0 {
			expose.LoadBalancerSourceRanges = ranges
		}
	}
	return expose
}

func parseMajorVersion(version string) (int, bool) {
	if version == "" {
		return 0, false
	}

	majorPart := strings.SplitN(version, ".", 2)[0]
	major, err := strconv.Atoi(majorPart)
	if err != nil {
		return 0, false
	}

	return major, true
}

// pgBouncerEnabled reports whether PGBouncer is configured to run. It mirrors
// PGProxySpec.PGBouncerEnabled from newer versions of the PG operator.
func pgBouncerEnabled(cluster *pgv2.PerconaPGCluster) bool {
	proxy := cluster.Spec.Proxy
	if proxy == nil || proxy.PGBouncer == nil {
		return false
	}
	return proxy.PGBouncer.Replicas == nil || *proxy.PGBouncer.Replicas != 0
}

// pgBouncerLoadBalancerExposed reports whether the operator publishes a load
// balancer address for PGBouncer in status.host.
func pgBouncerLoadBalancerExposed(cluster *pgv2.PerconaPGCluster) bool {
	proxy := cluster.Spec.Proxy
	if proxy == nil || proxy.PGBouncer == nil || proxy.PGBouncer.ServiceExpose == nil {
		return false
	}
	return proxy.PGBouncer.ServiceExpose.Type == string(corev1.ServiceTypeLoadBalancer)
}

// replaceURIHost swaps the host of a connection URI and keeps its port,
// credentials, path and query.
func replaceURIHost(rawURI, host string) string {
	if rawURI == "" {
		return rawURI
	}
	parsed, err := url.Parse(rawURI)
	if err != nil {
		return rawURI
	}
	if port := parsed.Port(); port != "" {
		parsed.Host = net.JoinHostPort(host, port)
	} else {
		parsed.Host = host
	}
	return parsed.String()
}

// ensureSSLMode sets sslmode in the URI query parameters to mode, unless
// already set explicitly. PGBouncer deployed by the Percona PG operator
// always requires TLS; without this parameter clients attempt a plain-text
// connection first and get rejected with "no such user" / "SSL required".
func ensureSSLMode(rawURI, mode string) string {
	if rawURI == "" {
		return rawURI
	}
	parsed, err := url.Parse(rawURI)
	if err != nil {
		return rawURI
	}
	q := parsed.Query()
	if q.Get("sslmode") != "" {
		return rawURI
	}
	q.Set("sslmode", mode)
	parsed.RawQuery = q.Encode()
	return parsed.String()
}

// Cleanup handles deletion of provider-managed resources.
//
// Called when the Instance has a deletion timestamp set.
// Delete any resources that are not automatically cleaned up
// via owner references.
func (p *Provider) Cleanup(c *controller.Context) error {
	l := log.FromContext(c.Context())
	l.Info("Cleaning up instance", "name", c.Name())

	cluster := &pgv2.PerconaPGCluster{}
	err := c.Get(cluster, c.Name())
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Cluster is gone. Delete any leftover secrets that survived the
			// operator's finalizers due to race conditions between the Percona
			// operator's cleanup and the upstream controller recreating them.
			return p.deleteLeftoverSecrets(c)
		}
		return fmt.Errorf("get PerconaPGCluster %q: %w", c.Name(), err)
	}

	if cluster.GetDeletionTimestamp().IsZero() {
		if err := c.Delete(cluster); err != nil {
			return fmt.Errorf("delete PerconaPGCluster %q: %w", c.Name(), err)
		}
		l.Info("Issued delete for PerconaPGCluster", "cluster", c.Name())
	}

	// Keep the Instance finalizer until the managed PG CR is fully removed.
	return controller.WaitFor("waiting for PerconaPGCluster to be deleted")
}

// deleteLeftoverSecrets removes secrets created by the PG operator that may
// survive cluster deletion due to a race condition between the operator's
// finalizer cleanup and the upstream Crunchy controller reconciling.
//
// TODO: Remove this workaround once Percona PG operator 3.1.0 is adopted,
// which fixes the race upstream: https://perconadev.atlassian.net/browse/K8SPG-1010
func (p *Provider) deleteLeftoverSecrets(c *controller.Context) error {
	l := log.FromContext(c.Context())

	secretList := &corev1.SecretList{}
	if err := c.List(secretList, client.MatchingLabels{
		"postgres-operator.crunchydata.com/cluster": c.Name(),
	}); err != nil {
		return fmt.Errorf("list leftover secrets for cluster %q: %w", c.Name(), err)
	}

	for i := range secretList.Items {
		l.Info("Deleting leftover secret", "secret", secretList.Items[i].Name)
		if err := c.Delete(&secretList.Items[i]); err != nil {
			return fmt.Errorf("delete leftover secret %q: %w", secretList.Items[i].Name, err)
		}
	}

	return nil
}
