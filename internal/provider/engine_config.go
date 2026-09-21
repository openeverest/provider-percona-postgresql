package provider

import (
	"encoding/json"
	"fmt"
	"strings"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	"github.com/openeverest/provider-percona-postgresql/definition/components"
	"github.com/openeverest/provider-percona-postgresql/internal/common"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	crunchyv1beta1 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/upstream.pgv2.percona.com/v1beta1"
	"sigs.k8s.io/yaml"
)

var allowedWALLevels = map[string]bool{
	"logical": true,
	"replica": true,
}

func engineConfigurationFromComponent(engine corev1alpha1.ComponentSpec) (map[string]any, error) {
	if engine.Parameters == nil || len(engine.Parameters.Raw) == 0 {
		return nil, nil
	}

	params := &components.PostgresqlParameters{}
	if err := json.Unmarshal(engine.Parameters.Raw, params); err != nil {
		return nil, fmt.Errorf("decode %q component parameters: %w", common.ComponentEngine, err)
	}
	if strings.TrimSpace(params.Configuration) == "" {
		return nil, nil
	}

	var pgParams map[string]any
	if err := yaml.Unmarshal([]byte(params.Configuration), &pgParams); err != nil {
		return nil, fmt.Errorf("parse %q component configuration: %w", common.ComponentEngine, err)
	}

	return pgParams, nil
}

func validateEngineConfiguration(engine corev1alpha1.ComponentSpec) error {
	pgParams, err := engineConfigurationFromComponent(engine)
	if err != nil {
		return err
	}
	if pgParams == nil {
		return nil
	}

	if walLevel, ok := pgParams["wal_level"]; ok {
		walLevelStr, ok := walLevel.(string)
		if !ok || !allowedWALLevels[walLevelStr] {
			return fmt.Errorf("invalid value for %q component configuration.wal_level: %v; must be 'logical' or 'replica'", common.ComponentEngine, walLevel)
		}
	}

	return nil
}

func applyEngineConfiguration(c *controller.Context, cluster *pgv2.PerconaPGCluster) error {
	engine, ok := c.Instance().Spec.Components[common.ComponentEngine]
	if !ok {
		return nil
	}

	pgParams, err := engineConfigurationFromComponent(engine)
	if err != nil {
		return err
	}
	if pgParams == nil {
		return nil
	}

	if cluster.Spec.Patroni == nil {
		cluster.Spec.Patroni = &crunchyv1beta1.PatroniSpec{}
	}
	if cluster.Spec.Patroni.DynamicConfiguration == nil {
		cluster.Spec.Patroni.DynamicConfiguration = crunchyv1beta1.SchemalessObject{}
	}

	postgresql, _ := cluster.Spec.Patroni.DynamicConfiguration["postgresql"].(map[string]any)
	if postgresql == nil {
		postgresql = map[string]any{}
	}
	postgresql["parameters"] = pgParams
	cluster.Spec.Patroni.DynamicConfiguration["postgresql"] = postgresql

	return nil
}
