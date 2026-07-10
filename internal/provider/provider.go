package provider

import (
	"fmt"
	"strconv"
	"strings"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	"github.com/openeverest/provider-percona-postgresql/definition"
	"github.com/openeverest/provider-percona-postgresql/internal/common"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	defaultPGBackRestImage = "docker.io/percona/percona-pgbackrest:2.58.0-1"
)

// Compile-time check that Provider implements the required interface.
var _ controller.ProviderInterface = (*Provider)(nil)
var _ controller.FieldIndexProvider = (*Provider)(nil)

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

// FieldIndexes registers indexes required by helper queries used in status computation.
func (p *Provider) FieldIndexes() []controller.FieldIndex {
	return []controller.FieldIndex{
		{
			Object:    &backupv1alpha1.Restore{},
			FieldPath: controller.IndexRestoreInstanceName,
			Extractor: func(obj client.Object) []string {
				restore, ok := obj.(*backupv1alpha1.Restore)
				if !ok || restore.Spec.InstanceName == "" {
					return nil
				}
				return []string{restore.Spec.InstanceName}
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

	// TODO: Implement validation logic.
	// Examples:
	//   - Check that required components are present
	//   - Validate storage sizes, replica counts
	//   - Ensure version compatibility
	return nil
}

// Sync ensures all required resources exist and are configured correctly.
//
// This is the main reconciliation logic. Create or update your operator
// operator's custom resource(s) based on the Instance spec.
func (p *Provider) Sync(c *controller.Context) error {
	l := log.FromContext(c.Context())
	l.Info("Syncing instance", "name", c.Name())

	defer l.Info("PostgreSQL cluster synced", "cluster", c.Name())

	meta := c.ObjectMeta(c.Name())
	meta.Finalizers = []string{
		"percona.com/delete-ssl",
		"percona.com/delete-pvc",
	}
	cluster := &pgv2.PerconaPGCluster{
		ObjectMeta: meta,
		Spec:       defaultSpec(),
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
	major := cluster.Spec.PostgresVersion
	if engine.Image != "" {
		cluster.Spec.Image = engine.Image
	}
	if parsedMajor, ok := parseMajorVersion(engine.Version); ok {
		major = parsedMajor
		cluster.Spec.PostgresVersion = major
	}
	if cluster.Spec.Image == "" {
		if engine.Version != "" {
			if image, ok := definition.PostgreSQLImageForVersion(engine.Version); ok {
				cluster.Spec.Image = image
			}
		}
		if cluster.Spec.Image == "" {
			cluster.Spec.Image = defaultPostgresImageForMajor(major)
			if cluster.Spec.Image == "" {
				return fmt.Errorf("cannot resolve default postgres image from versions catalog")
			}
		}
	}
	if cluster.Spec.Backups.PGBackRest.Image == "" {
		cluster.Spec.Backups.PGBackRest.Image = defaultPGBackRestImage
	}

	proxy, ok := c.Instance().Spec.Components[common.ComponentProxy]
	if !ok || proxy.Type == "" || proxy.Replicas == nil {
		return fmt.Errorf("instance spec has invalid %q component; this should be caught by Validate", common.ComponentProxy)
	}

	proxyType := proxy.Type
	if proxyType == "" {
		proxyType = common.ComponentTypePgbouncer
	}
	if proxyType != common.ComponentTypePgbouncer {
		return fmt.Errorf("instance spec has unsupported %q component type %q; only %q is supported", common.ComponentProxy, proxyType, common.ComponentTypePgbouncer)
	}
	cluster.Spec.Proxy.PGBouncer.Replicas = proxy.Replicas
	if proxy.Image != "" {
		cluster.Spec.Proxy.PGBouncer.Image = proxy.Image
	} else if cluster.Spec.Proxy.PGBouncer.Image == "" {
		if image, ok := definition.DefaultPGBouncerImage(); ok {
			cluster.Spec.Proxy.PGBouncer.Image = image
		} else {
			return fmt.Errorf("cannot resolve default pgbouncer image from versions catalog")
		}
	}

	if err := c.Apply(cluster); err != nil {
		return err
	}

	return nil
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

	resizing, err := isPVCResizing(c, cluster)
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

	return controller.ReadyWithConnectionDetails(controller.ConnectionDetails{
		Type:     "postgresql",
		Provider: common.ProviderName,
		Host:     cluster.Status.Host,
		Port:     strconv.Itoa(int(port)),
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

func isPVCResizing(c *controller.Context, cluster *pgv2.PerconaPGCluster) (bool, error) {
	if !isConditionTrue(cluster.Status.Conditions, "PersistentVolumeResizing") {
		return false, nil
	}

	// Work around operator lag in clearing the resize condition by verifying PVC conditions directly.
	return verifyPVCResizingStatus(c, cluster.GetName())
}

func verifyPVCResizingStatus(c *controller.Context, instanceName string) (bool, error) {
	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := c.List(pvcList, client.MatchingLabels{"app.kubernetes.io/instance": instanceName}); err != nil {
		return false, fmt.Errorf("list PVCs for instance %q: %w", instanceName, err)
	}

	for i := range pvcList.Items {
		for _, condition := range pvcList.Items[i].Status.Conditions {
			if (condition.Type == corev1.PersistentVolumeClaimResizing ||
				condition.Type == corev1.PersistentVolumeClaimFileSystemResizePending) &&
				condition.Status == corev1.ConditionTrue {
				return true, nil
			}
		}
	}

	return false, nil
}

func isConditionTrue(conditions []metav1.Condition, conditionType string) bool {
	for _, cond := range conditions {
		if cond.Type == conditionType && cond.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
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

func defaultPostgresImageForMajor(major int) string {
	if image, ok := definition.PostgreSQLDefaultImageForMajor(major); ok {
		return image
	}
	if image, ok := definition.DefaultPostgreSQLImage(); ok {
		return image
	}

	return ""
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
