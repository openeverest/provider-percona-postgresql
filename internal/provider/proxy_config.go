package provider

import (
	"encoding/json"
	"fmt"
	"strings"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	"sigs.k8s.io/yaml"

	"github.com/openeverest/provider-percona-postgresql/definition/components"
	"github.com/openeverest/provider-percona-postgresql/internal/common"
)

// proxyConfigurationFromComponent extracts the pgbouncer component's custom
// configuration as a flat key/value map, suitable for
// PGBouncerSpec.Config.Global. Unlike the engine's configuration (which is
// forwarded to Patroni as arbitrary typed values), PgBouncer's "global"
// settings are strictly string-valued, so every parsed value is rendered back
// to its JSON text form.
func proxyConfigurationFromComponent(proxy corev1alpha1.ComponentSpec) (map[string]string, error) {
	if proxy.Parameters == nil || len(proxy.Parameters.Raw) == 0 {
		return nil, nil
	}

	params := &components.PgbouncerParameters{}
	if err := json.Unmarshal(proxy.Parameters.Raw, params); err != nil {
		return nil, fmt.Errorf("decode %q component parameters: %w", common.ComponentProxy, err)
	}
	if strings.TrimSpace(params.Configuration) == "" {
		return nil, nil
	}

	var proxyParams map[string]any
	if err := yaml.Unmarshal([]byte(params.Configuration), &proxyParams); err != nil {
		return nil, fmt.Errorf("parse %q component configuration: %w", common.ComponentProxy, err)
	}
	if len(proxyParams) == 0 {
		return nil, nil
	}

	global := make(map[string]string, len(proxyParams))
	for key, value := range proxyParams {
		if s, ok := value.(string); ok {
			global[key] = s
			continue
		}

		rendered, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("encode %q component configuration value for %q: %w", common.ComponentProxy, key, err)
		}
		global[key] = string(rendered)
	}

	return global, nil
}

// validateProxyConfiguration checks that the proxy component's custom
// configuration, if any, is well-formed.
func validateProxyConfiguration(proxy corev1alpha1.ComponentSpec) error {
	_, err := proxyConfigurationFromComponent(proxy)
	return err
}

// applyProxyConfiguration maps the proxy component's custom configuration
// onto the operator CR's pgbouncer "global" settings.
func applyProxyConfiguration(cluster *pgv2.PerconaPGCluster, proxy corev1alpha1.ComponentSpec) error {
	global, err := proxyConfigurationFromComponent(proxy)
	if err != nil {
		return err
	}
	if cluster.Spec.Proxy == nil || cluster.Spec.Proxy.PGBouncer == nil {
		return nil
	}

	cluster.Spec.Proxy.PGBouncer.Config.Global = global
	return nil
}
