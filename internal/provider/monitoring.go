package provider

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	monitoringv1alpha1 "github.com/openeverest/openeverest/v2/api/monitoring/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	"github.com/openeverest/provider-percona-postgresql/internal/common"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	monitoringConfigRefFieldPath = "spec.components.monitoring.parameters.monitoringConfigName"
	monitoringConfigAPIKeyKey    = "apiKey"
	pgPMMServerToken             = "PMM_SERVER_TOKEN"
)

type pmmCustomSpec struct {
	MonitoringConfigName *string `json:"monitoringConfigName,omitempty"`
}

func applyMonitoringSettings(c *controller.Context, cluster *pgv2.PerconaPGCluster, providerSpec *corev1alpha1.ProviderSpec) error {
	monitoringComponent, ok := c.Instance().Spec.Components[common.ComponentMonitoring]
	if !ok {
		cluster.Spec.PMM = nil
		return nil
	}
	monitoringType := monitoringComponent.Type
	if monitoringType == "" {
		monitoringType = common.MonitoringTypePMM
	}

	if monitoringType != common.MonitoringTypePMM {
		return fmt.Errorf("unsupported monitoring component type %q", monitoringComponent.Type)
	}

	monitoringConfigName, err := monitoringConfigNameFromComponent(monitoringComponent)
	if err != nil {
		return err
	}
	if monitoringConfigName == "" {
		cluster.Spec.PMM = nil
		return nil
	}

	monitoringCfg := &monitoringv1alpha1.MonitoringConfig{}
	if err := c.Client().Get(c.Context(), client.ObjectKey{Namespace: c.Namespace(), Name: monitoringConfigName}, monitoringCfg); err != nil {
		return fmt.Errorf("get MonitoringConfig %q: %w", monitoringConfigName, err)
	}
	if monitoringCfg.Spec.Type != monitoringv1alpha1.PMMMonitoringType || monitoringCfg.Spec.PMM == nil {
		return fmt.Errorf("MonitoringConfig %q must be type %q", monitoringConfigName, monitoringv1alpha1.PMMMonitoringType)
	}
	if monitoringCfg.Spec.PMM.CredentialsSecretRef.Name == "" {
		return fmt.Errorf("MonitoringConfig %q must set spec.pmm.credentialsSecretRef.name", monitoringConfigName)
	}

	serverHost, err := pmmServerHostFromURL(monitoringCfg.Spec.PMM.URL)
	if err != nil {
		return fmt.Errorf("resolve PMM server host from MonitoringConfig %q URL: %w", monitoringConfigName, err)
	}

	pmmImage := monitoringImageForComponent(c, providerSpec, monitoringType, monitoringComponent)
	if pmmImage == "" {
		return fmt.Errorf("cannot resolve PMM image for component %q", common.ComponentMonitoring)
	}

	pmmSecretName := c.Name() + "-pmm-secret"
	if err := syncPMMCredentials(c, monitoringCfg.Spec.PMM.CredentialsSecretRef.Name, pmmSecretName); err != nil {
		return err
	}

	cluster.Spec.PMM = &pgv2.PMMSpec{
		Enabled:           true,
		ServerHost:        serverHost,
		Image:             pmmImage,
		Secret:            pmmSecretName,
		CustomClusterName: c.Name(),
		ImagePullPolicy:   corev1.PullIfNotPresent,
	}
	return nil
}

func monitoringConfigNameFromComponent(component corev1alpha1.ComponentSpec) (string, error) {
	if component.Parameters == nil || len(component.Parameters.Raw) == 0 {
		return "", nil
	}

	cfg := &pmmCustomSpec{}
	if err := json.Unmarshal(component.Parameters.Raw, cfg); err != nil {
		return "", fmt.Errorf("decode monitoring component parameters: %w", err)
	}
	if cfg.MonitoringConfigName == nil {
		return "", nil
	}

	return *cfg.MonitoringConfigName, nil
}

func pmmServerHostFromURL(rawURL string) (string, error) {
	if rawURL == "" {
		return "", fmt.Errorf("url is empty")
	}
	if !strings.Contains(rawURL, "://") {
		return strings.TrimSuffix(rawURL, "/"), nil
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	if u.Host == "" {
		return "", fmt.Errorf("missing host")
	}
	return u.Host, nil
}

func syncPMMCredentials(c *controller.Context, credentialsSecretName, pmmSecretName string) error {
	credentialsSecret := &corev1.Secret{}
	if err := c.Client().Get(c.Context(), client.ObjectKey{Namespace: c.Namespace(), Name: credentialsSecretName}, credentialsSecret); err != nil {
		return fmt.Errorf("get PMM credentials Secret %q: %w", credentialsSecretName, err)
	}
	apiKey, ok := credentialsSecret.Data[monitoringConfigAPIKeyKey]
	if !ok || len(apiKey) == 0 {
		return fmt.Errorf("PMM credentials Secret %q must contain non-empty %q key", credentialsSecretName, monitoringConfigAPIKeyKey)
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

func monitoringImageForComponent(c *controller.Context, providerSpec *corev1alpha1.ProviderSpec, monitoringType string, component corev1alpha1.ComponentSpec) string {
	if component.Image != "" {
		return component.Image
	}
	if component.Version != "" {
		if image := controller.GetImageForVersion(providerSpec, common.ComponentMonitoring, component.Version); image != "" {
			return image
		}
	}

	selectedBundle := c.Instance().Spec.Version
	if selectedBundle == "" {
		selectedBundle = c.Instance().Status.Version
	}
	if selectedBundle == "" {
		selectedBundle = controller.GetDefaultVersionBundleName(providerSpec)
	}
	if selectedBundle != "" {
		if bundle, err := controller.ResolveVersionBundle(providerSpec, selectedBundle); err == nil {
			if version, ok := bundle.Components[common.ComponentMonitoring]; ok {
				if image := controller.GetImageForVersion(providerSpec, common.ComponentMonitoring, version); image != "" {
					return image
				}
			}
		}
	}

	return controller.GetDefaultImage(providerSpec, monitoringType)
}
