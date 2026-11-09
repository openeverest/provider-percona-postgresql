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
// in order of precedence: the certificate secrets, a user-supplied CA, or
// else are generated (through cert-manager when it is installed, or else
// from a CA the operator creates once). The certificate source can't change
// after the instance is created, as a new CA would stop existing clients and
// members from trusting the instance.
type TLSParameters struct {
	// Mode controls whether non-TLS connections are rejected.
	// +default="preferTLS"
	// +optional
	Mode TLSMode `json:"mode,omitempty"`

	// CertificateSecretName names a Secret in the instance namespace with
	// the certificate clients connect to: tls.crt, tls.key and ca.crt. Its
	// subject alternative names must cover the instance's service names.
	// Requires ReplicationCertificateSecretName.
	// +optional
	CertificateSecretName string `json:"certificateSecretName,omitempty"`

	// ReplicationCertificateSecretName names a Secret in the instance
	// namespace with the client certificate members use to authenticate
	// replication connections, in the same format and sharing the same CA
	// as the certificate secret.
	// +optional
	ReplicationCertificateSecretName string `json:"replicationCertificateSecretName,omitempty"`

	// RootCASecretName names a Secret in the instance namespace with the CA
	// certificate and key (root.crt, root.key) the operator signs the
	// certificates it generates with, instead of creating its own CA for
	// the instance. Can't be combined with CertificateSecretName.
	// +optional
	RootCASecretName string `json:"rootCASecretName,omitempty"`
}
