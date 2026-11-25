// Package definition contains shared types used across the provider definition.
//
// +k8s:openapi-gen=true
package definition

// Parameters defines instance-wide structured parameters that apply to the
// entire cluster (Instance.spec.parameters).
type Parameters struct {
	// TLS configures encryption in transit, between clients and the instance
	// and between its members.
	// +optional
	TLS *TLSParameters `json:"tls,omitempty"`
}

// TLSMode controls whether the instance accepts non-TLS connections.
// +enum
type TLSMode string

const (
	// TLSModePrefer encrypts traffic to pgBouncer (pgBouncer always requires
	// TLS from clients) but lets clients connect to PostgreSQL directly
	// with or without TLS. The default.
	TLSModePrefer TLSMode = "preferTLS"
	// TLSModeRequire rejects any client connection, direct or through
	// pgBouncer, that doesn't use TLS.
	TLSModeRequire TLSMode = "requireTLS"
)

// TLSParameters configures TLS for the instance. The certificates come from,
// in order of precedence: the certificate secrets, the cert-manager issuer,
// a user-supplied root CA, or else are generated (through cert-manager when
// it is installed, or else from a CA the operator creates once). The
// certificate source can't change after the instance is created, as a new CA
// would stop existing clients and members from trusting the instance.
type TLSParameters struct {
	// Mode controls whether non-TLS connections are rejected.
	// +default="preferTLS"
	// +optional
	Mode TLSMode `json:"mode,omitempty"`

	// CertificateSecretName names a Secret in the instance namespace with
	// the certificate clients connect to: tls.crt, tls.key and ca.crt. Its
	// subject alternative names must cover the instance's service names.
	// Requires InternalCertificateSecretName.
	// +optional
	CertificateSecretName string `json:"certificateSecretName,omitempty"`

	// InternalCertificateSecretName names a Secret in the instance
	// namespace with the client certificate members use to authenticate
	// replication connections, in the same format and sharing the same CA
	// as the certificate secret.
	// +optional
	InternalCertificateSecretName string `json:"internalCertificateSecretName,omitempty"`

	// Issuer is the cert-manager issuer that signs the instance's
	// certificates, instead of a CA created for the instance. Requires
	// cert-manager. The issuer must write its CA certificate to ca.crt, as
	// the CA and Vault issuers do. Can't be combined with
	// CertificateSecretName or RootCASecretName.
	// +optional
	Issuer *IssuerReference `json:"issuer,omitempty"`

	// RootCASecretName names a Secret in the instance namespace with the CA
	// certificate and key (root.crt, root.key) the operator signs the
	// certificates it generates with, instead of creating its own CA for
	// the instance. Can't be combined with CertificateSecretName or Issuer.
	// +optional
	RootCASecretName string `json:"rootCASecretName,omitempty"`

	// AdditionalNames are DNS names the certificates would be valid for
	// besides the instance's in-cluster names, such as an external name
	// clients connect to. Not supported yet: the Percona Operator for
	// PostgreSQL can't add names to the certificates it issues
	// (https://github.com/percona/percona-postgresql-operator/issues/1811),
	// so it is rejected. Meanwhile, issue a certificate with the names and
	// set CertificateSecretName.
	// +optional
	AdditionalNames []string `json:"additionalNames,omitempty"`
}

// IssuerKind is the kind of a cert-manager issuer.
// +enum
type IssuerKind string

const (
	// IssuerKindIssuer is an Issuer in the instance namespace. Not
	// supported: the Percona Operator for PostgreSQL takes a namespaced
	// Issuer over as its own.
	IssuerKindIssuer IssuerKind = "Issuer"
	// IssuerKindClusterIssuer is a cluster-wide ClusterIssuer.
	IssuerKindClusterIssuer IssuerKind = "ClusterIssuer"
)

// IssuerReference refers to a cert-manager issuer.
type IssuerReference struct {
	// Name of the issuer.
	// +optional
	Name string `json:"name,omitempty"`

	// Kind of the issuer: ClusterIssuer, or the kind of an external issuer
	// of group, such as AWSPCAClusterIssuer. A namespaced Issuer is not
	// supported, as the Percona Operator for PostgreSQL takes it over as
	// its own and deletes it with the instance.
	// +default="Issuer"
	// +optional
	Kind IssuerKind `json:"kind,omitempty"`

	// Group is the API group of an external issuer, such as
	// awspca.cert-manager.io.
	// +default="cert-manager.io"
	// +optional
	Group string `json:"group,omitempty"`
}
