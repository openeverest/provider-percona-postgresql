package provider

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"regexp"
	"slices"

	monitoringv1alpha1 "github.com/openeverest/openeverest/v2/api/monitoring/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// pmmIntegration is the integration declared in definition/monitoring.yaml.
	pmmIntegration         = "pmm"
	pmmDestinationTokenKey = "token"
	pgPMMServerToken       = "PMM_SERVER_TOKEN"
	pmmSecretSuffix        = "-pmm-secret"
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

// renderFailedError marks inputs the binding cannot be rendered from. It is
// reported on the binding; any other error fails Sync and is retried.
type renderFailedError struct{ error }

func renderFailed(format string, args ...any) error {
	return renderFailedError{fmt.Errorf(format, args...)}
}

// applyPMM renders spec.pmm from the Instance's pmm binding while one
// requires it and disables it once none does, deleting the token Secret once
// the live cluster has PMM off. Otherwise, and when the binding cannot be
// rendered, it keeps what the live cluster runs; a render failure is reported
// on the binding and never fails Sync.
func applyPMM(c *controller.Context, cluster *pgv2.PerconaPGCluster) error {
	req, err := c.RequiredSources()
	if err != nil {
		return err
	}
	live, err := getLiveCluster(c)
	if err != nil {
		return err
	}

	pmm := req.Integrations[pmmIntegration]
	if !pmm.Else(live.PMMEnabled()) {
		cluster.Spec.PMM = nil
		if pmm == controller.NotRequired && !live.PMMEnabled() {
			return deletePMMSecret(c)
		}
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
		if _, ok := errors.AsType[renderFailedError](err); ok {
			log.FromContext(c.Context()).Error(err, "PMM binding could not be rendered", "binding", rb.Binding.Name)
			c.SetMonitoringBindingResult(rb.Binding.Name, monitoringv1alpha1.ReasonRenderFailed, err.Error())
			return nil
		}
		if err != nil {
			return err
		}
		cluster.Spec.PMM = spec
		c.SetMonitoringBindingResult(rb.Binding.Name, monitoringv1alpha1.ReasonAgentTLSNotVerified,
			"pmm-client does not verify the PMM server certificate, whatever the destination's tls says: the Percona operator runs it with PMM_AGENT_SERVER_INSECURE_TLS=1")
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
			return nil, renderFailed("decode MonitoringDestination %q parameters: %w", rb.Destination.Name, err)
		}
	}
	u, err := url.Parse(dst.URL)
	if err != nil || u.Host == "" {
		return nil, renderFailed("MonitoringDestination %q parameters.url %q is not an absolute URL", rb.Destination.Name, dst.URL)
	}
	// A missing Secret or key is a render failure; other API errors are retried.
	mode, data, err := c.MonitoringCredentials(rb)
	if _, isAPIErr := errors.AsType[*apierrors.StatusError](err); isAPIErr && !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("read PMM credentials: %w", err)
	}
	if err != nil {
		return nil, renderFailedError{err}
	}
	token := data[pmmDestinationTokenKey]
	if len(token) == 0 {
		return nil, renderFailed("MonitoringDestination %q authentication mode %q provides no %q credential", rb.Destination.Name, mode, pmmDestinationTokenKey)
	}
	image := rb.Class.Spec.ProviderManaged.AgentImage
	if image == "" {
		return nil, renderFailed("MonitoringClass %q sets no providerManaged.agentImage", rb.Class.Name)
	}
	params := pmmInstanceParameters{}
	if rb.Binding.Spec.Parameters != nil {
		if err := json.Unmarshal(rb.Binding.Spec.Parameters.Raw, &params); err != nil {
			return nil, renderFailed("decode MonitoringBinding %q parameters: %w", rb.Binding.Name, err)
		}
	}
	if r := params.Resources; r != nil {
		for _, name := range slices.Sorted(maps.Keys(r.Requests)) {
			request := r.Requests[name]
			if limit, ok := r.Limits[name]; ok && request.Cmp(limit) > 0 {
				return nil, renderFailed("MonitoringBinding %q parameters.resources: %s request %s exceeds its limit %s", rb.Binding.Name, name, request.String(), limit.String())
			}
		}
	}

	pmmSecretName := c.Name() + pmmSecretSuffix
	if err := mirrorPMMToken(c, token, pmmSecretName); err != nil {
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

// mirrorPMMToken applies the destination token to the Secret the operator
// reads it from, owned by the Instance so it is deleted with it.
func mirrorPMMToken(c *controller.Context, token []byte, pmmSecretName string) error {
	if err := c.Apply(&corev1.Secret{
		ObjectMeta: c.ObjectMeta(pmmSecretName),
		Data:       map[string][]byte{pgPMMServerToken: token},
	}); err != nil {
		return fmt.Errorf("apply PMM Secret %q: %w", pmmSecretName, err)
	}
	return nil
}

func deletePMMSecret(c *controller.Context) error {
	secret := &corev1.Secret{}
	exists, err := c.Exists(secret, c.Name()+pmmSecretSuffix)
	if err != nil || !exists {
		return err
	}
	if err := c.Delete(secret); err != nil {
		return fmt.Errorf("delete PMM Secret %q: %w", secret.Name, err)
	}
	return nil
}
