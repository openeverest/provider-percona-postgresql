package provider

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	"github.com/openeverest/provider-percona-postgresql/internal/common"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	upstreamv1beta1 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/upstream.pgv2.percona.com/v1beta1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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
)

// Compile-time check that Provider implements the required interface.
var _ controller.ProviderInterface = (*Provider)(nil)

// Provider implements controller.ProviderInterface for the provider-percona-postgresql provider.
type Provider struct {
	controller.BaseProvider
}

// New creates a new Provider instance.
func New() *Provider {
	return &Provider{
		BaseProvider: controller.BaseProvider{
			ProviderName: common.ProviderName,
			SchemeFuncs: []func(*runtime.Scheme) error{
				pgv2.AddToScheme,
			},
			WatchConfigs: []controller.WatchConfig{
				controller.WatchOwned(&pgv2.PerconaPGCluster{}),
			},
		},
	}
}

// Validate checks if the Instance spec is valid.
//
// Add your provider-specific validation logic here.
// Return an error if the spec is invalid.
//
// +kubebuilder:rbac:groups=<operator-api-group>,resources=<operator-resources>,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=<operator-api-group>,resources=<operator-resources>/status,verbs=get
func (p *Provider) Validate(c *controller.Context) error {
	l := log.FromContext(c.Context())
	l.Info("Validating instance", "name", c.Name())

	providerSpec, err := c.ProviderSpec()
	if err != nil {
		return err
	}

	bundleComponents := map[string]string{}
	bundleName := selectedVersionBundleName(c, providerSpec)
	if bundleName != "" {
		bundle, err := controller.ResolveVersionBundle(providerSpec, bundleName)
		if err != nil {
			return fmt.Errorf("invalid version bundle %q: %w", bundleName, err)
		}
		bundleComponents = bundle.Components
	}

	var errs []string

	engine, ok := c.Instance().Spec.Components[common.ComponentEngine]
	if !ok {
		errs = append(errs, fmt.Sprintf("missing %q component", common.ComponentEngine))
	} else {
		expectedEngineType := controller.GetComponentType(providerSpec, common.ComponentEngine)
		if engine.Type != "" && engine.Type != expectedEngineType {
			errs = append(errs, fmt.Sprintf("unsupported %q component type %q; only %q is supported", common.ComponentEngine, engine.Type, expectedEngineType))
		}
		if engine.Replicas == nil {
			errs = append(errs, fmt.Sprintf("%q component replicas must be set", common.ComponentEngine))
		} else if *engine.Replicas < 1 {
			errs = append(errs, fmt.Sprintf("%q component replicas must be >= 1", common.ComponentEngine))
		}

		if engine.Version != "" {
			image := controller.GetImageForVersion(providerSpec, common.ComponentEngine, engine.Version)
			if image == "" && engine.Image == "" {
				errs = append(errs, fmt.Sprintf("unsupported or image-less %q component version %q and no image override is set", common.ComponentEngine, engine.Version))
			}
		}

		engineVersion := engine.Version
		if engineVersion == "" {
			engineVersion = bundleComponents[common.ComponentEngine]
		}

		if engine.Image == "" && engineVersion == "" && controller.GetDefaultImage(providerSpec, componentTypePostgreSQL) == "" {
			errs = append(errs, "cannot resolve postgres image: set engine.image or engine.version, or configure a default postgresql image in provider versions catalog")
		}
	}

	proxy, ok := c.Instance().Spec.Components[common.ComponentProxy]
	if !ok {
		errs = append(errs, fmt.Sprintf("missing %q component", common.ComponentProxy))
	} else {
		expectedProxyType := controller.GetComponentType(providerSpec, common.ComponentProxy)
		proxyType := proxy.Type
		if proxyType == "" {
			proxyType = expectedProxyType
		}
		if proxyType != expectedProxyType {
			errs = append(errs, fmt.Sprintf("unsupported %q component type %q; only %q is supported", common.ComponentProxy, proxyType, expectedProxyType))
		}
		if proxy.Replicas == nil {
			errs = append(errs, fmt.Sprintf("%q component replicas must be set", common.ComponentProxy))
		} else if *proxy.Replicas < 0 {
			errs = append(errs, fmt.Sprintf("%q component replicas must be >= 0", common.ComponentProxy))
		}

		if proxy.Version != "" {
			image := controller.GetImageForVersion(providerSpec, common.ComponentProxy, proxy.Version)
			if image == "" && proxy.Image == "" {
				errs = append(errs, fmt.Sprintf("unsupported or image-less %q component version %q and no image override is set", common.ComponentProxy, proxy.Version))
			}
		}

		proxyVersion := proxy.Version
		if proxyVersion == "" {
			proxyVersion = bundleComponents[common.ComponentProxy]
		}
		if proxy.Image == "" && proxyVersion == "" && controller.GetDefaultImage(providerSpec, componentTypePGBouncer) == "" {
			errs = append(errs, "cannot resolve pgbouncer image: set proxy.image or proxy.version, or configure a default pgbouncer image in provider versions catalog")
		}
	}

	if controller.GetDefaultImage(providerSpec, componentTypePGBackRest) == "" {
		errs = append(errs, "cannot resolve default pgbackrest image from provider versions catalog")
	}

	if len(errs) > 0 {
		return fmt.Errorf("invalid instance spec: %s", strings.Join(errs, "; "))
	}

	return nil
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
	bundleName := selectedVersionBundleName(c, providerSpec)
	if bundleName != "" {
		bundle, err := controller.ResolveVersionBundle(providerSpec, bundleName)
		if err != nil {
			return err
		}
		bundleComponents = bundle.Components
	}

	// Get the engine component spec
	engine, ok := c.Instance().Spec.Components[common.ComponentEngine]
	if !ok || engine.Replicas == nil {
		return fmt.Errorf("instance spec missing %q component replicas", common.ComponentEngine)
	}
	if len(cluster.Spec.InstanceSets) == 0 {
		cluster.Spec.InstanceSets = pgv2.PGInstanceSets{{Name: "instance1"}}
	}
	cluster.Spec.InstanceSets[0].Replicas = engine.Replicas
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

	proxy, ok := c.Instance().Spec.Components[common.ComponentProxy]
	if !ok || proxy.Replicas == nil {
		return fmt.Errorf("instance spec has invalid %q component; this should be caught by Validate", common.ComponentProxy)
	}

	proxyType := proxy.Type
	if proxyType == "" {
		proxyType = controller.GetComponentType(providerSpec, common.ComponentProxy)
	}
	if proxyType != controller.GetComponentType(providerSpec, common.ComponentProxy) {
		return fmt.Errorf("instance spec has unsupported %q component type %q; only %q is supported", common.ComponentProxy, proxyType, controller.GetComponentType(providerSpec, common.ComponentProxy))
	}
	cluster.Spec.Proxy.PGBouncer.Replicas = proxy.Replicas
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

	// Configure the default database user for client connections. The user
	// must NOT be a SUPERUSER because the operator's pgbouncer.get_auth()
	// function explicitly excludes superusers and replication roles from
	// PGBouncer authentication.
	cluster.Spec.Users = []upstreamv1beta1.PostgresUserSpec{
		{
			Name:    upstreamv1beta1.PostgresIdentifier(c.Name()),
			Options: "CREATEDB",
			Databases: []upstreamv1beta1.PostgresIdentifier{
				upstreamv1beta1.PostgresIdentifier(c.Name()),
			},
			Password: &upstreamv1beta1.PostgresPasswordSpec{
				Type: upstreamv1beta1.PostgresPasswordTypeAlphaNumeric,
			},
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
	// the Instance phase. The reconciler treats BackupConfigError specially
	// by setting the BackupConfigured condition to False without marking the
	// Instance as Failed.
	var backupConfigErr error
	if err := applyBackupSettings(c, cluster); err != nil {
		if controller.AsBackupConfigError(err) != nil {
			backupConfigErr = err
			// Preserve the existing cluster's backup configuration so we
			// don't apply an inconsistent spec (e.g. enabled=true with
			// zero repos) or accidentally wipe a previously working setup.
			existing := &pgv2.PerconaPGCluster{}
			if getErr := c.Get(existing, c.Name()); getErr == nil {
				cluster.Spec.Backups = existing.Spec.Backups
			}
			// If the cluster doesn't exist yet, defaultSpec() already has
			// backups disabled which is safe.
		} else {
			return err
		}
	}

	// Preserve backup-related fields set by the PG operator (manual backup
	// triggers and annotations). Without this the provider would overwrite
	// them on every reconciliation, preventing on-demand backups from ever
	// starting.
	if err := preserveBackupTrigger(c, cluster); err != nil {
		return err
	}

	// Preserve the DataSource field set by the PG restore operator. Without
	// this the provider would overwrite it on every reconciliation, preventing
	// restores from ever progressing past "Starting".
	if err := preserveRestoreDataSource(c, cluster); err != nil {
		return err
	}

	if err := c.Apply(cluster); err != nil {
		return err
	}

	return backupConfigErr
}

// Status computes the current status of the database instance.
//
// Query the operator's resource(s) and translate their status
// into the provider-runtime's Status type.
func (p *Provider) Status(c *controller.Context) (controller.Status, error) {
	l := log.FromContext(c.Context())
	l.Info("Computing status", "name", c.Name())

	cluster := &pgv2.PerconaPGCluster{}
	if err := c.Get(cluster, c.Name()); err != nil {
		if apierrors.IsNotFound(err) {
			return controller.Provisioning("waiting for PerconaPGCluster to be created"), nil
		}
		return controller.Status{}, fmt.Errorf("get PerconaPGCluster %q: %w", c.Name(), err)
	}

	restoring, err := isRestoreRunning(c)
	if err != nil {
		return controller.Status{}, err
	}
	if restoring {
		return controller.Restoring("restore is in progress"), nil
	}

	resizing, err := isPVCResizing(cluster)
	if err != nil {
		return controller.Status{}, err
	}
	if resizing {
		return controller.Updating("resizing persistent volumes"), nil
	}

	switch cluster.Status.State {
	case pgv2.AppStateInit:
		return controller.Initializing("database is initializing"), nil
	case pgv2.AppStateStopping:
		return controller.Suspending("database is stopping"), nil
	case pgv2.AppStatePaused:
		if cluster.Status.Postgres.Size == 0 && cluster.Status.PGBouncer.Size == 0 {
			return controller.Suspended(), nil
		}
		return controller.Suspending("database is paused"), nil
	}

	if cluster.Status.ObservedGeneration < cluster.Generation {
		return controller.Updating("applying latest configuration"), nil
	}

	if cluster.Status.Postgres.Size == 0 {
		return controller.Provisioning("waiting for postgres replicas to be created"), nil
	}
	if cluster.Status.Postgres.Ready < cluster.Status.Postgres.Size {
		return controller.Provisioning(
			fmt.Sprintf("waiting for postgres replicas (%d/%d ready)", cluster.Status.Postgres.Ready, cluster.Status.Postgres.Size),
		), nil
	}
	if cluster.Status.PGBouncer.Size > 0 && cluster.Status.PGBouncer.Ready < cluster.Status.PGBouncer.Size {
		return controller.Provisioning(
			fmt.Sprintf("waiting for pgbouncer replicas (%d/%d ready)", cluster.Status.PGBouncer.Ready, cluster.Status.PGBouncer.Size),
		), nil
	}

	port := int32(5432)
	if cluster.Spec.Port != nil {
		port = *cluster.Spec.Port
	}

	// Read the user secret created by the PG operator to obtain credentials.
	// The secret follows the naming convention: <cluster-name>-pguser-<username>.
	// The PG operator populates this secret with all connection details including
	// properly URL-encoded URIs for both direct and PGBouncer connections.
	var username, password, uri string
	userSecret := &corev1.Secret{}
	secretName := c.Name() + "-pguser-" + c.Name()
	if err := c.Get(userSecret, secretName); err != nil {
		if !apierrors.IsNotFound(err) {
			return controller.Status{}, fmt.Errorf("get user secret %q: %w", secretName, err)
		}
		l.Info("User secret not found, connection details will not include credentials", "secret", secretName)
	} else {
		username = string(userSecret.Data["user"])
		password = string(userSecret.Data["password"])
		// Prefer pgbouncer-uri when PGBouncer is enabled, fall back to direct uri.
		if v := userSecret.Data["pgbouncer-uri"]; len(v) > 0 {
			uri = string(v)
		} else if v := userSecret.Data["uri"]; len(v) > 0 {
			uri = string(v)
		}
		// Ensure the URI includes sslmode=require so that clients connect
		// over TLS. PGBouncer is configured with SSL by the Percona PG
		// operator and rejects plain-text connections.
		uri = ensureSSLMode(uri)
	}

	// Use the pgbouncer-host from the secret if available (it includes the
	// correct service FQDN for PGBouncer), otherwise fall back to cluster status.
	host := cluster.Status.Host
	if v := userSecret.Data["pgbouncer-host"]; len(v) > 0 {
		host = string(v)
	}

	return controller.ReadyWithConnectionDetails(controller.ConnectionDetails{
		Type:     "postgresql",
		Provider: common.ProviderName,
		Host:     host,
		Port:     strconv.Itoa(int(port)),
		Username: username,
		Password: password,
		URI:      uri,
	}), nil
}

func isRestoreRunning(c *controller.Context) (bool, error) {
	restores, err := c.RestoresForInstance()
	if err != nil {
		return false, fmt.Errorf("list restores for instance %q: %w", c.Name(), err)
	}

	for i := range restores {
		state := restores[i].Status.State
		if state == "" || state == backupv1alpha1.RestoreStatePending || state == backupv1alpha1.RestoreStateRunning {
			return true, nil
		}
	}

	return false, nil
}

func isPVCResizing(cluster *pgv2.PerconaPGCluster) (bool, error) {
	for _, condition := range cluster.Status.Conditions {
		if condition.Type == upstreamv1beta1.PersistentVolumeResizing && condition.Status == metav1.ConditionTrue {
			return true, nil
		}
	}

	return false, nil
}

// Backup-related annotations set by the Percona PG operator's backup
// controller. We must preserve these during Sync so that on-demand backups
// triggered via PerconaPGBackup are not cancelled by the provider
// overwriting the cluster spec.
var backupAnnotationKeys = []string{
	"pgv2.percona.com/pgbackrest-backup",                  // AnnotationPGBackrestBackup
	"pgv2.percona.com/backup-in-progress",                 // AnnotationBackupInProgress
	"postgres-operator.crunchydata.com/pgbackrest-backup", // upstream PGBackRestBackup
}

// preserveBackupTrigger reads the existing PerconaPGCluster and copies
// backup-related annotations and the Manual backup trigger into the
// cluster object that is about to be applied. This prevents the provider
// from overwriting the PG operator's backup trigger on every Sync.
func preserveBackupTrigger(c *controller.Context, cluster *pgv2.PerconaPGCluster) error {
	existing := &pgv2.PerconaPGCluster{}
	if err := c.Get(existing, c.Name()); err != nil {
		// If cluster doesn't exist yet, nothing to preserve.
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("get existing PerconaPGCluster for backup trigger: %w", err)
	}

	// Preserve backup annotations.
	for _, key := range backupAnnotationKeys {
		if val, ok := existing.Annotations[key]; ok {
			if cluster.Annotations == nil {
				cluster.Annotations = make(map[string]string)
			}
			cluster.Annotations[key] = val
		}
	}

	// Preserve the Manual backup trigger if one is set.
	if existing.Spec.Backups.PGBackRest.Manual != nil {
		cluster.Spec.Backups.PGBackRest.Manual = existing.Spec.Backups.PGBackRest.Manual
	}

	return nil
}

// restoreAnnotationKey is the annotation set by the Percona PG restore
// controller on the PerconaPGCluster to signal an in-place pgBackRest restore.
const restoreAnnotationKey = "postgres-operator.crunchydata.com/pgbackrest-restore"

// preserveRestoreDataSource reads the existing PerconaPGCluster and copies
// restore-related fields into the cluster object that is about to be applied.
// The Percona PG restore operator sets spec.backups.pgBackRest.restore and
// the pgbackrest-restore annotation to trigger an in-place restore; without
// preserving these the provider would wipe them on every Sync, leaving the
// restore stuck in "Starting".
func preserveRestoreDataSource(c *controller.Context, cluster *pgv2.PerconaPGCluster) error {
	existing := &pgv2.PerconaPGCluster{}
	if err := c.Get(existing, c.Name()); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("get existing PerconaPGCluster for restore state: %w", err)
	}

	// Preserve the DataSource field (used for bootstrap restores).
	if existing.Spec.DataSource != nil {
		cluster.Spec.DataSource = existing.Spec.DataSource
	}

	// Preserve the pgBackRest Restore field (used for in-place restores).
	if existing.Spec.Backups.PGBackRest.Restore != nil {
		cluster.Spec.Backups.PGBackRest.Restore = existing.Spec.Backups.PGBackRest.Restore
	}

	// Preserve the restore annotation.
	if val, ok := existing.Annotations[restoreAnnotationKey]; ok {
		if cluster.Annotations == nil {
			cluster.Annotations = make(map[string]string)
		}
		cluster.Annotations[restoreAnnotationKey] = val
	}

	return nil
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

func selectedVersionBundleName(c *controller.Context, spec *corev1alpha1.ProviderSpec) string {
	if c.Instance().Spec.Version != "" {
		return c.Instance().Spec.Version
	}
	if c.Instance().Status.Version != "" {
		return c.Instance().Status.Version
	}
	return controller.GetDefaultVersionBundleName(spec)
}

// ensureSSLMode appends sslmode=require to the URI query parameters if no
// sslmode is already specified. PGBouncer deployed by the Percona PG operator
// mandates TLS; without this parameter clients attempt a plain-text connection
// first and get rejected with "no such user" / "SSL required".
func ensureSSLMode(rawURI string) string {
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
	q.Set("sslmode", "require")
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
			// Cluster is already gone; cleanup is complete.
			return nil
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
