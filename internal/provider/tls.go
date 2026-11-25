// Copyright (C) 2026 The OpenEverest Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package provider

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"strings"

	cmmeta "github.com/cert-manager/cert-manager/pkg/apis/meta/v1"
	pgv2 "github.com/percona/percona-postgresql-operator/v3/pkg/apis/pgv2.percona.com/v2"
	crunchyv1beta1 "github.com/percona/percona-postgresql-operator/v3/pkg/apis/upstream.pgv2.percona.com/v1beta1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-percona-postgresql/definition"
)

var _ controller.ReferenceValidator = (*Provider)(nil)

const (
	certManagerGroup = "cert-manager.io"

	// additionalNamesIssue tracks the operator support additionalNames
	// waits for.
	additionalNamesIssue = "https://github.com/percona/percona-postgresql-operator/issues/1811"

	// caKey is the key holding the CA certificate, in the certificate
	// secrets and in the connection secret.
	caKey = "ca.crt"

	// defaultCertificateSecretSuffix names the secret the operator
	// generates for the instance's leaf certificate when no certificate
	// secret is supplied. Mirrors naming.ClusterCertSecret ("%s-cluster-cert")
	// from the Percona PostgreSQL Operator.
	defaultCertificateSecretSuffix = "-cluster-cert"
)

// certificateSecretKeys are the keys a server or replication certificate
// secret must hold.
var certificateSecretKeys = []string{"tls.crt", "tls.key", caKey}

// rootCASecretKeys are the keys a root CA secret must hold.
var rootCASecretKeys = []string{"root.crt", "root.key"}

// tlsParameters returns the instance's TLS parameters with defaults applied.
func tlsParameters(instance *corev1alpha1.Instance) (definition.TLSParameters, error) {
	params := definition.Parameters{}
	if raw := instance.Spec.Parameters; raw != nil && len(raw.Raw) > 0 {
		if err := json.Unmarshal(raw.Raw, &params); err != nil {
			return definition.TLSParameters{}, fmt.Errorf("decode parameters: %w", err)
		}
	}

	tls := definition.TLSParameters{}
	if params.TLS != nil {
		tls = *params.TLS
	}
	if tls.Mode == "" {
		tls.Mode = definition.TLSModePrefer
	}
	if tls.Issuer != nil && tls.Issuer.Name == "" {
		tls.Issuer = nil
	}
	if tls.Issuer != nil {
		issuer := *tls.Issuer
		issuer.Kind = cmp.Or(issuer.Kind, definition.IssuerKindIssuer)
		issuer.Group = cmp.Or(issuer.Group, certManagerGroup)
		tls.Issuer = &issuer
	}

	return tls, nil
}

// clientCertificateSecretName is the secret holding the certificate clients
// connect to; the operator generates it under its default name unless the
// instance brings its own.
func clientCertificateSecretName(c *controller.Context, tls definition.TLSParameters) string {
	if tls.CertificateSecretName != "" {
		return tls.CertificateSecretName
	}
	return c.Name() + defaultCertificateSecretSuffix
}

// secretProjection wraps a secret name into a SecretProjection with its
// default keys (the whole secret is projected).
func secretProjection(name string) *corev1.SecretProjection {
	if name == "" {
		return nil
	}
	return &corev1.SecretProjection{
		LocalObjectReference: corev1.LocalObjectReference{Name: name},
	}
}

// configureTLS sets the TLS mode and the certificate source on the
// PerconaPGCluster resource.
func configureTLS(cluster *pgv2.PerconaPGCluster, tls definition.TLSParameters) {
	cluster.Spec.TLSOnly = tls.Mode == definition.TLSModeRequire

	switch {
	case tls.CertificateSecretName != "":
		cluster.Spec.Secrets.CustomTLSSecret = secretProjection(tls.CertificateSecretName)
		cluster.Spec.Secrets.CustomReplicationClientTLSSecret = secretProjection(tls.InternalCertificateSecretName)
		if cluster.Spec.Proxy != nil && cluster.Spec.Proxy.PGBouncer != nil {
			// Clients connect to pgBouncer, not directly to PostgreSQL, so it
			// needs the same certificate.
			cluster.Spec.Proxy.PGBouncer.CustomTLSSecret = secretProjection(tls.CertificateSecretName)
		}
	case tls.Issuer != nil:
		cluster.Spec.TLS = &crunchyv1beta1.TLSSpec{
			IssuerConf: &cmmeta.IssuerReference{
				Name:  tls.Issuer.Name,
				Kind:  string(tls.Issuer.Kind),
				Group: tls.Issuer.Group,
			},
		}
	case tls.RootCASecretName != "":
		cluster.Spec.Secrets.CustomRootCATLSSecret = secretProjection(tls.RootCASecretName)
	}
}

func validateTLSParameters(tls definition.TLSParameters, path *field.Path) field.ErrorList {
	var errs field.ErrorList

	switch tls.Mode {
	case definition.TLSModePrefer, definition.TLSModeRequire:
	default:
		errs = append(errs, field.Invalid(path.Child("mode"), tls.Mode,
			fmt.Sprintf("must be one of %s or %s", definition.TLSModePrefer, definition.TLSModeRequire)))
	}

	errs = append(errs, validateIssuer(path.Child("issuer"), tls.Issuer)...)
	if len(tls.AdditionalNames) > 0 {
		errs = append(errs, field.Forbidden(path.Child("additionalNames"),
			"not supported yet: the Percona Operator for PostgreSQL can't add names to its certificates ("+
				additionalNamesIssue+"); issue a certificate with the names and set certificateSecretName instead"))
	}

	switch {
	case tls.CertificateSecretName != "" && tls.Issuer != nil:
		errs = append(errs, field.Forbidden(path.Child("issuer"), "use either certificate secrets or an issuer, not both"))
	case tls.CertificateSecretName != "" && tls.RootCASecretName != "":
		errs = append(errs, field.Invalid(path.Child("rootCASecretName"), tls.RootCASecretName,
			"use either certificate secrets or a root CA secret, not both"))
	case tls.Issuer != nil && tls.RootCASecretName != "":
		errs = append(errs, field.Forbidden(path.Child("rootCASecretName"), "use either an issuer or a root CA secret, not both"))
	case tls.CertificateSecretName == "" && tls.InternalCertificateSecretName != "":
		errs = append(errs, field.Required(path.Child("certificateSecretName"),
			"required when internalCertificateSecretName is set"))
	case tls.CertificateSecretName != "" && tls.InternalCertificateSecretName == "":
		errs = append(errs, field.Required(path.Child("internalCertificateSecretName"),
			"required when certificateSecretName is set"))
	}

	return errs
}

// validateIssuer checks an issuer reference with defaults applied. The
// operator takes a namespaced Issuer over as its own, whatever the group: it
// creates a CA issuer of that name or, if one exists, makes the cluster its
// owner, so it is deleted with the instance. It looks a ClusterIssuer up in
// cert-manager's group, whatever the group set. Any other kind is an external
// issuer it leaves to its controller.
func validateIssuer(path *field.Path, issuer *definition.IssuerReference) field.ErrorList {
	if issuer == nil {
		return nil
	}
	var errs field.ErrorList
	if msgs := validation.IsDNS1123Subdomain(issuer.Group); len(msgs) > 0 {
		errs = append(errs, field.Invalid(path.Child("group"), issuer.Group, strings.Join(msgs, "; ")))
	}
	switch {
	case issuer.Kind == definition.IssuerKindIssuer:
		errs = append(errs, field.Invalid(path.Child("kind"), issuer.Kind,
			"a namespaced Issuer is not supported: the Percona Operator for PostgreSQL takes it over as its own "+
				"and deletes it with the instance; use a ClusterIssuer or an external issuer"))
	case issuer.Group == certManagerGroup && issuer.Kind != definition.IssuerKindClusterIssuer:
		errs = append(errs, field.NotSupported(path.Child("kind"), issuer.Kind,
			[]definition.IssuerKind{definition.IssuerKindClusterIssuer}))
	case issuer.Group != certManagerGroup && issuer.Kind == definition.IssuerKindClusterIssuer:
		errs = append(errs, field.Invalid(path.Child("kind"), issuer.Kind,
			"set the kind of the external issuer, such as AWSPCAClusterIssuer: the operator takes "+
				"ClusterIssuer for cert-manager's own"))
	}
	return errs
}

func (p *Provider) ValidateReferences(c *controller.Context) (field.ErrorList, error) {
	tls, err := tlsParameters(c.Instance())
	if err != nil {
		return nil, err
	}
	path := field.NewPath("spec", "parameters", "tls")

	var errs field.ErrorList

	if tls.CertificateSecretName != "" {
		certPath := path.Child("certificateSecretName")
		ca, certErrs, err := validateCertificateSecret(c, certPath, tls.CertificateSecretName)
		if err != nil {
			return nil, err
		}
		errs = append(errs, certErrs...)

		replPath := path.Child("internalCertificateSecretName")
		replicationCA, replErrs, err := validateCertificateSecret(c, replPath, tls.InternalCertificateSecretName)
		if err != nil {
			return nil, err
		}
		errs = append(errs, replErrs...)

		if len(certErrs) == 0 && len(replErrs) == 0 && !bytes.Equal(ca, replicationCA) {
			errs = append(errs, field.Invalid(replPath, tls.InternalCertificateSecretName,
				"must share the same ca.crt as certificateSecretName"))
		}
	}

	if tls.RootCASecretName != "" {
		rootErrs, err := validateRootCASecret(c, path.Child("rootCASecretName"), tls.RootCASecretName)
		if err != nil {
			return nil, err
		}
		errs = append(errs, rootErrs...)
	}

	changeErrs, err := validateTLSChange(c, tls, path)
	if err != nil {
		return nil, err
	}
	errs = append(errs, changeErrs...)

	return errs, nil
}

// validateCertificateSecret checks that a certificate secret has the keys a
// server or replication certificate needs, and returns its CA certificate.
func validateCertificateSecret(c *controller.Context, path *field.Path, name string) ([]byte, field.ErrorList, error) {
	secret := &corev1.Secret{}
	if err := c.Get(secret, name); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, field.ErrorList{field.NotFound(path, name)}, nil
		}
		return nil, nil, fmt.Errorf("get certificate secret %q: %w", name, err)
	}
	var errs field.ErrorList
	for _, key := range certificateSecretKeys {
		if len(secret.Data[key]) == 0 {
			errs = append(errs, field.Invalid(path, name, fmt.Sprintf("secret is missing %q", key)))
		}
	}
	return secret.Data[caKey], errs, nil
}

// validateRootCASecret checks that a root CA secret has the keys the
// operator needs to sign generated certificates with it.
func validateRootCASecret(c *controller.Context, path *field.Path, name string) (field.ErrorList, error) {
	secret := &corev1.Secret{}
	if err := c.Get(secret, name); err != nil {
		if apierrors.IsNotFound(err) {
			return field.ErrorList{field.NotFound(path, name)}, nil
		}
		return nil, fmt.Errorf("get root CA secret %q: %w", name, err)
	}
	var errs field.ErrorList
	for _, key := range rootCASecretKeys {
		if len(secret.Data[key]) == 0 {
			errs = append(errs, field.Invalid(path, name, fmt.Sprintf("secret is missing %q", key)))
		}
	}
	return errs, nil
}

// validateTLSChange rejects a change of certificate source on a running
// instance: a new CA would stop existing clients and members from trusting
// the instance.
func validateTLSChange(c *controller.Context, tls definition.TLSParameters, path *field.Path) (field.ErrorList, error) {
	current := &pgv2.PerconaPGCluster{}
	if err := c.Get(current, c.Name()); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("fetching current PerconaPGCluster: %w", err)
	}

	projectionName := func(p *corev1.SecretProjection) string {
		if p == nil {
			return ""
		}
		return p.Name
	}
	currentIssuer := ""
	if current.Spec.TLS != nil && current.Spec.TLS.IssuerConf != nil {
		conf := current.Spec.TLS.IssuerConf
		currentIssuer = issuerKey(conf.Kind, conf.Name, conf.Group)
	}
	wantIssuer := ""
	if tls.Issuer != nil {
		wantIssuer = issuerKey(string(tls.Issuer.Kind), tls.Issuer.Name, tls.Issuer.Group)
	}

	if projectionName(current.Spec.Secrets.CustomTLSSecret) != tls.CertificateSecretName ||
		projectionName(current.Spec.Secrets.CustomReplicationClientTLSSecret) != tls.InternalCertificateSecretName ||
		projectionName(current.Spec.Secrets.CustomRootCATLSSecret) != tls.RootCASecretName ||
		currentIssuer != wantIssuer {
		return field.ErrorList{field.Forbidden(path, "certificates, issuer and root CA secret can't change on an existing instance")}, nil
	}

	return nil, nil
}

// issuerKey identifies an issuer, with defaults applied; empty for none.
func issuerKey(kind, name, group string) string {
	if name == "" {
		return ""
	}
	return cmp.Or(kind, string(definition.IssuerKindIssuer)) + "/" + name + "." + cmp.Or(group, certManagerGroup)
}

// tlsConnection returns the CA clients verify the instance with, and the
// sslmode clients should use: "require" when either the instance requires
// TLS or, regardless of mode, clients connect through pgBouncer (which
// always requires TLS from clients); "prefer" otherwise.
func tlsConnection(c *controller.Context, cluster *pgv2.PerconaPGCluster) (ca []byte, sslMode string, err error) {
	tls, err := tlsParameters(c.Instance())
	if err != nil {
		return nil, "", err
	}

	sslMode = "prefer"
	if tls.Mode == definition.TLSModeRequire || pgBouncerEnabled(cluster) {
		sslMode = "require"
	}

	secret := &corev1.Secret{}
	if err := c.Get(secret, clientCertificateSecretName(c, tls)); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, sslMode, nil
		}
		return nil, "", fmt.Errorf("get certificate secret: %w", err)
	}

	return secret.Data[caKey], sslMode, nil
}
