// Package pg contains the schema-bearing Go types for the
// "pg" BackupClass. Each struct here is converted to an OpenAPI
// v3 schema by `provider-sdk generate` and inlined into the generated
// BackupClass manifest.
//
// +k8s:openapi-gen=true
package pg

// PgBackupType defines the type of backup to take.
// Values match the pgBackRest --type flag: "full", "diff", "incr".
// +kubebuilder:validation:Enum=full;diff;incr
type PgBackupType string

// PgBackupConfig describes the configuration accepted by Backup CRs that
// target this class (spec.parameters). Add fields the user can set per backup.
type PgBackupConfig struct {
	// Type selects the pgBackRest backup type. "full" (default) creates a
	// self-contained backup. "diff" backs up pages changed since the
	// last full backup. "incr" backs up pages changed since the last
	// backup of any type (full, diff, or incr).
	// +kubebuilder:default=full
	// +kubebuilder:validation:Enum=full;diff;incr
	// +optional
	Type PgBackupType `json:"type,omitempty"`
}

// PgRestoreConfig describes the configuration accepted by Restore CRs that
// target this class (spec.config). Add fields the user can set per restore.
type PgRestoreConfig struct{}

// PgPITRConfig describes the per-storage PITR custom config exposed to
// Instance.spec.backup.storages[].pitr.config. Add fields a provider needs
// to fine-tune its PITR pipeline (WAL archiving settings, etc.).
type PgPITRConfig struct {
	// ArchiveTimeoutSeconds controls the archive_timeout parameter for WAL segment
	// switching. PostgreSQL will force a WAL segment switch after this many seconds
	// of inactivity, ensuring WAL files are archived regularly for PITR.
	// +kubebuilder:default=60
	// +kubebuilder:validation:Minimum=1
	ArchiveTimeoutSeconds *float64 `json:"archiveTimeoutSeconds,omitempty"`
}
