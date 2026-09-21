// Package components contains parameters types for provider component types.
//
// Each struct here corresponds to a component type defined in versions.yaml
// and is converted to an OpenAPI schema during generation.
// Add fields when a component type needs custom parameters beyond
// what the base Instance spec provides.
//
// +k8s:openapi-gen=true
package components

// PgbouncerParameters defines custom parameters for pgbouncer components.
// Add fields here when the pgbouncer component type needs custom parameters
// beyond what the base Instance spec provides.
type PgbouncerParameters struct{}

// PostgresqlParameters defines custom parameters for postgresql components.
type PostgresqlParameters struct {
	// Configuration allows specifying custom PostgreSQL configuration
	// parameters as a YAML document of flat key/value pairs, e.g.:
	//
	//   max_connections: "500"
	//   shared_buffers: 1GB
	//   wal_level: replica
	//
	// applied on top of, and overrides, the operator's built-in defaults.
	// +optional
	Configuration string `json:"configuration,omitempty"`
}

// PmmParameters defines custom parameters for PMM monitoring.
type PmmParameters struct {
	// MonitoringConfigName specifies the name of the MonitoringConfig resource
	// to use for configuring PMM monitoring.
	// If not specified, monitoring will not be configured.
	MonitoringConfigName *string `json:"monitoringConfigName,omitempty"`
}
