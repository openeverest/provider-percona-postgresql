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

	// PgHBA declares which clients may connect to which databases as which
	// users: pg_hba.conf records, one per line, such as
	//
	//   hostssl orders orders_app 10.0.0.0/8 scram-sha-256
	//   host    orders all        all        reject
	//
	// Blank lines and # comments are ignored. Records are checked in order,
	// after the operator's own rules (replication, pgBouncer, monitoring) and
	// before the default rule that lets every user in over TLS with a
	// password, so end with a reject record to allow only what is listed.
	// Records may be host, hostssl or hostnossl with the methods
	// scram-sha-256, md5, cert, ldap, radius or reject; trust is not allowed.
	// With tls.mode requireTLS, unencrypted connections stay rejected.
	// +optional
	PgHBA string `json:"pgHBA,omitempty"`
}
