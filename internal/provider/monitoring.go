package provider

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	monitoringv1alpha1 "github.com/openeverest/openeverest/v2/api/monitoring/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	"github.com/openeverest/provider-percona-postgresql/definition/components"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	monitoringConfigRefFieldPath = "spec.components.monitoring.parameters.monitoringConfigName"
	// pmmIntegration is the integration declared in definition/monitoring.yaml.
	pmmIntegration         = "pmm"
	pmmDestinationTokenKey = "apiKey"
	pgPMMServerToken       = "PMM_SERVER_TOKEN"
)

// MonitoringIntegrations implements controller.MonitoringIntegrationsProvider.
func (p *Provider) MonitoringIntegrations() []string {
	return []string{pmmIntegration}
}

var _ controller.MonitoringIntegrationsProvider = (*Provider)(nil)

// pmmDestinationParameters is MonitoringDestination.spec.parameters of the
// pmm class.
type pmmDestinationParameters struct {
	URL string `json:"url"`
}

// pmmInstanceParameters is Instance.spec.monitoring.destinations[].parameters
// of the pmm class.
type pmmInstanceParameters struct {
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`
}

// applyPMM renders spec.pmm from the Instance's pmm binding while one
// requires it and disables it once none does. Otherwise, and when the
// binding cannot be rendered, it keeps what the live cluster runs; a render
// failure is reported on the binding and never fails Sync.
func applyPMM(c *controller.Context, cluster *pgv2.PerconaPGCluster) error {
	req, err := c.RequiredSources()
	if err != nil {
		return err
	}
	live := &pgv2.PerconaPGCluster{}
	if err := c.Get(live, c.Name()); client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("get PerconaPGCluster %q: %w", c.Name(), err)
	}

	pmm := req.Integrations[pmmIntegration]
	if !pmm.Else(live.PMMEnabled()) {
		cluster.Spec.PMM = nil
		return nil
	}
	cluster.Spec.PMM = live.Spec.PMM.DeepCopy()
	if pmm != controller.Required {
		return nil
	}

	bindings, err := c.MonitoringBindings()
	if err != nil {
		return err
	}
	for _, rb := range bindings {
		if !rb.Apply || rb.Integration() != pmmIntegration {
			continue
		}
		spec, err := renderPMM(c, rb)
		if err != nil {
			log.FromContext(c.Context()).Error(err, "PMM binding could not be rendered", "binding", rb.Binding.Name)
			c.SetMonitoringBindingResult(rb.Binding.Name, monitoringv1alpha1.ReasonRenderFailed, err.Error())
			return nil
		}
		cluster.Spec.PMM = spec
		return nil
	}
	return nil
}

// renderPMM builds spec.pmm for an Apply-able pmm binding and mirrors the
// destination's token into the Secret the operator reads it from.
func renderPMM(c *controller.Context, rb controller.ResolvedBinding) (*pgv2.PMMSpec, error) {
	dst := pmmDestinationParameters{}
	if rb.Destination.Spec.Parameters != nil {
		if err := json.Unmarshal(rb.Destination.Spec.Parameters.Raw, &dst); err != nil {
			return nil, fmt.Errorf("decode MonitoringDestination %q parameters: %w", rb.Destination.Name, err)
		}
	}
	u, err := url.Parse(dst.URL)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("MonitoringDestination %q parameters.url %q is not an absolute URL", rb.Destination.Name, dst.URL)
	}
	if rb.Destination.Spec.CredentialsSecretRef == nil {
		return nil, fmt.Errorf("MonitoringDestination %q sets no credentialsSecretRef", rb.Destination.Name)
	}
	image := rb.Class.Spec.ProviderManaged.AgentImage
	if image == "" {
		return nil, fmt.Errorf("MonitoringClass %q sets no providerManaged.agentImage", rb.Class.Name)
	}
	params := pmmInstanceParameters{}
	if rb.Binding.Spec.Parameters != nil {
		if err := json.Unmarshal(rb.Binding.Spec.Parameters.Raw, &params); err != nil {
			return nil, fmt.Errorf("decode MonitoringBinding %q parameters: %w", rb.Binding.Name, err)
		}
	}

	pmmSecretName := c.Name() + "-pmm-secret"
	if err := syncPMMCredentials(c, rb.Destination.Spec.CredentialsSecretRef.Name, pmmSecretName); err != nil {
		return nil, err
	}

	spec := &pgv2.PMMSpec{
		Enabled:           true,
		ServerHost:        u.Host,
		Image:             image,
		Secret:            pmmSecretName,
		CustomClusterName: c.Name(),
		PostgresParams:    pmmAdminParams(c),
		ImagePullPolicy:   corev1.PullIfNotPresent,
	}
	if params.Resources != nil {
		spec.Resources = *params.Resources
	}
	return spec, nil
}

// pmmAdminParams returns the pmm-admin flags that label the instance's metrics
// in PMM, so consumers can scope queries to one instance. Label names follow
// the Prometheus form of the pod labels (core_openeverest_io_instance), and
// namespace matches the label on Kubernetes metrics in PMM. The operator runs
// pmm-admin through a shell; Kubernetes names need no quoting. Only PostgreSQL
// pods run a PMM client, so no component label is needed.
func pmmAdminParams(c *controller.Context) string {
	return "--custom-labels=namespace=" + c.Namespace() +
		"," + prometheusLabelName(controller.InstanceLabel) + "=" + c.Name()
}

var invalidPrometheusLabelChars = regexp.MustCompile(`[^a-zA-Z0-9_]`)

// prometheusLabelName converts a Kubernetes label key to a valid Prometheus
// label name the way Prometheus does for Kubernetes service discovery.
func prometheusLabelName(key string) string {
	return invalidPrometheusLabelChars.ReplaceAllString(key, "_")
}

func monitoringConfigNameFromComponent(component corev1alpha1.ComponentSpec) (string, error) {
	if component.Parameters == nil || len(component.Parameters.Raw) == 0 {
		return "", nil
	}

	cfg := &components.PmmParameters{}
	if err := json.Unmarshal(component.Parameters.Raw, cfg); err != nil {
		return "", fmt.Errorf("decode monitoring component parameters: %w", err)
	}
	if cfg.MonitoringConfigName == nil {
		return "", nil
	}

	return *cfg.MonitoringConfigName, nil
}

func syncPMMCredentials(c *controller.Context, credentialsSecretName, pmmSecretName string) error {
	credentialsSecret := &corev1.Secret{}
	if err := c.Client().Get(c.Context(), client.ObjectKey{Namespace: c.Namespace(), Name: credentialsSecretName}, credentialsSecret); err != nil {
		return fmt.Errorf("get PMM credentials Secret %q: %w", credentialsSecretName, err)
	}
	apiKey, ok := credentialsSecret.Data[pmmDestinationTokenKey]
	if !ok || len(apiKey) == 0 {
		return fmt.Errorf("PMM credentials Secret %q must contain non-empty %q key", credentialsSecretName, pmmDestinationTokenKey)
	}

	pmmSecret := &corev1.Secret{}
	err := c.Client().Get(c.Context(), client.ObjectKey{Namespace: c.Namespace(), Name: pmmSecretName}, pmmSecret)
	if err != nil {
		if apierrors.IsNotFound(err) {
			pmmSecret = &corev1.Secret{}
			pmmSecret.Name = pmmSecretName
			pmmSecret.Namespace = c.Namespace()
			pmmSecret.Data = map[string][]byte{
				pgPMMServerToken: append([]byte(nil), apiKey...),
			}
			if err := c.Client().Create(c.Context(), pmmSecret); err != nil {
				return fmt.Errorf("create PMM secret %q: %w", pmmSecretName, err)
			}
			return nil
		}
		return fmt.Errorf("get PMM Secret %q: %w", pmmSecretName, err)
	}

	if pmmSecret.Data != nil && bytes.Equal(pmmSecret.Data[pgPMMServerToken], apiKey) {
		return nil
	}

	orig := pmmSecret.DeepCopy()
	if pmmSecret.Data == nil {
		pmmSecret.Data = map[string][]byte{}
	}
	pmmSecret.Data[pgPMMServerToken] = append([]byte(nil), apiKey...)

	if err := c.Client().Patch(c.Context(), pmmSecret, client.MergeFrom(orig)); err != nil {
		return fmt.Errorf("sync PMM credentials to Secret %q: %w", pmmSecretName, err)
	}

	return nil
}
