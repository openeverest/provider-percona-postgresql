// Package common defines shared constants used across the provider.
package common

const (
	// ProviderName is the canonical name of this provider.
	ProviderName = "provider-percona-postgresql"

	ComponentEngine     = "engine"
	ComponentProxy      = "proxy"
	ComponentMonitoring = "monitoring"
)

// Monitoring type constants supported by the provider.
const (
	MonitoringTypePMM = "pmm"
)
