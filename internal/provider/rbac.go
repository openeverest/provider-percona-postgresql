package provider

// Run `make manifests` to regenerate config/rbac/role.yaml from these markers.
// This file contains kubebuilder RBAC markers for controller-gen.
// See: https://book.kubebuilder.io/reference/markers/rbac

// Base RBAC (required by all providers):
// +kubebuilder:rbac:groups=core.openeverest.io,resources=instances,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=core.openeverest.io,resources=instances/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core.openeverest.io,resources=instances/finalizers,verbs=update
// +kubebuilder:rbac:groups=core.openeverest.io,resources=providers,verbs=get;list;watch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// =============================================================================
// PROVIDER-SPECIFIC RBAC — Add markers for your operator's resources.
// =============================================================================
// Examples:
//
//   - Watch/manage operator CRs:
//   // +kubebuilder:rbac:groups=<operator-api-group>,resources=<operator-resources>,verbs=get;list;watch;create;update;patch;delete
//   // +kubebuilder:rbac:groups=<operator-api-group>,resources=<operator-resources>/status,verbs=get
//   // +kubebuilder:rbac:groups=<operator-api-group>,resources=<operator-resources>/finalizers,verbs=update
//
//   - Access Kubernetes core resources:
//   // +kubebuilder:rbac:groups="",resources=secrets;configmaps,verbs=get;list;watch
//   // +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
//
//   - Access PVCs (if managing storage):
//   // +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete

// =============================================================================
// PROVIDER-SPECIFIC RBAC — Percona PostgreSQL operator resources.
// =============================================================================
// Allow reading Secrets referenced by managed resources.
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=pgv2.percona.com,resources=perconapgclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=pgv2.percona.com,resources=perconapgclusters/status,verbs=get
// +kubebuilder:rbac:groups=pgv2.percona.com,resources=perconapgclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=backup.openeverest.io,resources=restores,verbs=get;list;watch
