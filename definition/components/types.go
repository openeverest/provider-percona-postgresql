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
type PgbouncerParameters struct {
	// Configuration allows specifying custom PgBouncer configuration
	// parameters as a YAML document of flat key/value pairs, e.g.:
	//
	//   max_client_conn: "500"
	//   default_pool_size: 20
	//   query_wait_timeout: 30
	//
	// applied on top of, and overriding, the operator's built-in defaults.
	// More info: https://www.pgbouncer.org/config.html
	// +optional
	Configuration string `json:"configuration,omitempty"`
}

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

	// LogCollector configures the operator's log collector: a Fluent Bit
	// "logs" sidecar on each instance pod that streams PostgreSQL and
	// pgBackRest logs to its stdout as JSON, plus a "logrotate" sidecar that
	// caps the log files on the data volume.
	// +optional
	LogCollector *LogCollectorParameters `json:"logCollector,omitempty"`
}

// LogCollectorParameters configures the log collector.
type LogCollectorParameters struct {
	// Enabled turns the log collector on or off. Unset turns it on for new
	// instances and leaves existing ones as they are (off for instances
	// created before the log collector was available). Changing it restarts
	// the instance pods.
	// +optional
	Enabled *bool `json:"enabled,omitempty"`
}
