// Package components contains custom spec types for provider component types.
//
// Each struct here corresponds to a component type defined in versions.yaml
// and is converted to an OpenAPI schema during generation.
// Add fields when a component type needs custom configuration beyond
// what the base Instance spec provides.
//
// +k8s:openapi-gen=true
package components

// PgbouncerCustomSpec defines custom configuration for pgbouncer components.
// Add fields here when the pgbouncer component type needs custom configuration
// beyond what the base Instance spec provides.
type PgbouncerCustomSpec struct{}

// PostgresqlCustomSpec defines custom configuration for postgresql components.
// Add fields here when the postgresql component type needs custom configuration
// beyond what the base Instance spec provides.
type PostgresqlCustomSpec struct{}
