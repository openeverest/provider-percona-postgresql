package provider

import (
	"fmt"

	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	"github.com/openeverest/provider-percona-postgresql/internal/common"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"
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

	// TODO: Implement status logic.
	// Typical pattern:
	//   1. Get the operator CR using c.Get()
	//   2. Translate its status to a controller.Status
	//
	// Example:
	//   cr := &operatorv1.MyDatabase{}
	//   if err := c.Get(cr, c.Name()); err != nil {
	//       return controller.Status{}, err
	//   }
	//   if cr.Status.Ready {
	//       return controller.ReadyWithConnectionDetails(
	//           controller.ConnectionDetails{
	//           // Populate connection details.
	//           },
	//       ), nil
	//   }
	//   return controller.Provisioning("waiting for database to be ready"), nil

	return controller.Provisioning("initializing"), nil
}

// Cleanup handles deletion of provider-managed resources.
//
// Called when the Instance has a deletion timestamp set.
// Delete any resources that are not automatically cleaned up
// via owner references.
func (p *Provider) Cleanup(c *controller.Context) error {
	l := log.FromContext(c.Context())
	l.Info("Cleaning up instance", "name", c.Name())

	// TODO: Implement cleanup logic if needed.
	// Resources with owner references set via c.Apply() are automatically
	// garbage collected. Only implement this if you need custom cleanup.
	return nil
}
