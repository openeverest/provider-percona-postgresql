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
// Add fields here when the postgresql component type needs custom parameters
// beyond what the base Instance spec provides.
type PostgresqlParameters struct{}
