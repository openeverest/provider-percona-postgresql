package provider

import (
	"encoding/json"
	"fmt"
	"net"
	"regexp"
	"slices"
	"strings"

	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	crunchyv1beta1 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/upstream.pgv2.percona.com/v1beta1"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"

	"github.com/openeverest/provider-percona-postgresql/definition"
	"github.com/openeverest/provider-percona-postgresql/definition/components"
)

var (
	// rejectNonTLSRule goes before the pgHBA records with tls.mode requireTLS:
	// the operator's tlsOnly only turns its own records into hostssl.
	rejectNonTLSRule = hbaRule{Line: "hostnossl all all all reject", Connection: "hostnossl", Method: "reject"}
	// defaultHBARule is the operator's default record, which it leaves out
	// once spec.authentication has rules. It goes after the pgHBA records so
	// they narrow access instead of replacing the default.
	defaultHBARule = hbaRule{Line: "hostssl all all all scram-sha-256", Connection: "hostssl", Method: "scram-sha-256"}
)

// engineHBA returns the engine component's pgHBA parameter.
func engineHBA(engine corev1alpha1.ComponentSpec) (string, error) {
	if engine.Parameters == nil || len(engine.Parameters.Raw) == 0 {
		return "", nil
	}
	params := &components.PostgresqlParameters{}
	if err := json.Unmarshal(engine.Parameters.Raw, params); err != nil {
		return "", fmt.Errorf("decode engine component parameters: %w", err)
	}
	return params.PgHBA, nil
}

// validateEngineHBA checks the engine component's pgHBA parameter.
func validateEngineHBA(engine corev1alpha1.ComponentSpec) error {
	pgHBA, err := engineHBA(engine)
	if err != nil {
		return err
	}
	_, err = parsePgHBA(pgHBA)
	return err
}

// applyPgHBA sets spec.authentication from the pgHBA parameter. The operator
// places the rules after its mandatory records (replication, pgBouncer,
// monitoring), and mirrors ldap rules to pgBouncer.
func applyPgHBA(cluster *pgv2.PerconaPGCluster, pgHBA string, tls definition.TLSParameters) error {
	rules, err := parsePgHBA(pgHBA)
	if err != nil {
		return err
	}
	if len(rules) == 0 {
		return nil
	}
	if tls.Mode == definition.TLSModeRequire {
		rules = append([]hbaRule{rejectNonTLSRule}, rules...)
	}
	rules = append(rules, defaultHBARule)

	authn := &crunchyv1beta1.PostgresClusterAuthentication{}
	for _, r := range rules {
		authn.Rules = append(authn.Rules, crunchyv1beta1.PostgresAuthenticationRule{
			// The CRD requires connection and method even with a raw record.
			PostgresHBARule: crunchyv1beta1.PostgresHBARule{Connection: r.Connection, Method: r.Method},
			HBA:             r.Line,
		})
	}
	cluster.Spec.Authentication = authn
	return nil
}

// hbaConnectionTypes are the connection types a pgHBA rule may match. Local
// (Unix socket) connections are reserved for the operator, and GSSAPI
// encryption needs a Kerberos setup the images don't have.
var hbaConnectionTypes = []string{"host", "hostssl", "hostnossl"}

// hbaMethods are the authentication methods a pgHBA rule may use. trust and
// password (cleartext) are left out on purpose, as are methods that need
// server-side setup the instance doesn't have (ident, pam, gss, ...).
var hbaMethods = []string{"scram-sha-256", "md5", "cert", "ldap", "radius", "reject"}

var hbaHostname = regexp.MustCompile(`^\.?([a-zA-Z0-9]([-a-zA-Z0-9]*[a-zA-Z0-9])?)(\.[a-zA-Z0-9]([-a-zA-Z0-9]*[a-zA-Z0-9])?)*$`)

// hbaRule is one validated pg_hba.conf record from the pgHBA parameter.
type hbaRule struct {
	// Line is the record without surrounding blanks or a trailing comment.
	Line string
	// Connection is the record's connection type, such as hostssl.
	Connection string
	// Method is the record's authentication method, such as scram-sha-256.
	Method string
}

// parsePgHBA parses the pgHBA parameter: one pg_hba.conf record per line, with
// blank lines and # comments ignored. It accepts host, hostssl and hostnossl
// records with an address and one of hbaMethods.
func parsePgHBA(text string) ([]hbaRule, error) {
	var rules []hbaRule
	for i, raw := range strings.Split(text, "\n") {
		rule, err := parseHBALine(raw)
		if err != nil {
			return nil, fmt.Errorf("pgHBA line %d: %w", i+1, err)
		}
		if rule != nil {
			rules = append(rules, *rule)
		}
	}
	return rules, nil
}

func parseHBALine(raw string) (*hbaRule, error) {
	line, tokens, err := tokenizeHBALine(raw)
	if err != nil {
		return nil, err
	}
	if len(tokens) == 0 {
		return nil, nil
	}
	if strings.HasSuffix(line, `\`) {
		return nil, fmt.Errorf("line continuations are not supported; write the record on one line")
	}

	connection := tokens[0]
	switch {
	case connection == "local":
		return nil, fmt.Errorf("local records are not supported: Unix socket connections are reserved for the operator")
	case strings.HasPrefix(connection, "include"):
		return nil, fmt.Errorf("%s directives are not supported", connection)
	case !slices.Contains(hbaConnectionTypes, connection):
		return nil, fmt.Errorf("unsupported connection type %q; use one of %s", connection, strings.Join(hbaConnectionTypes, ", "))
	}

	if len(tokens) < 5 {
		return nil, fmt.Errorf("expected \"%s DATABASE USER ADDRESS METHOD [OPTIONS]\"", connection)
	}
	for _, f := range []struct{ name, value string }{{"database", tokens[1]}, {"user", tokens[2]}} {
		if err := validateHBAList(f.name, f.value); err != nil {
			return nil, err
		}
	}

	methodIndex, err := validateHBAAddress(tokens[3:])
	if err != nil {
		return nil, err
	}
	methodIndex += 3
	if methodIndex >= len(tokens) {
		return nil, fmt.Errorf("missing authentication method")
	}

	method := tokens[methodIndex]
	switch {
	case method == "trust":
		return nil, fmt.Errorf("method trust is not allowed: it lets clients connect without a password")
	case !slices.Contains(hbaMethods, method):
		return nil, fmt.Errorf("unsupported authentication method %q; use one of %s", method, strings.Join(hbaMethods, ", "))
	case method == "cert" && connection != "hostssl":
		return nil, fmt.Errorf("method cert requires a hostssl record")
	}

	for _, option := range tokens[methodIndex+1:] {
		if key, _, ok := strings.Cut(option, "="); !ok || key == "" || strings.ContainsRune(key, '"') {
			return nil, fmt.Errorf("option %q must be NAME=VALUE", option)
		}
	}

	return &hbaRule{Line: line, Connection: connection, Method: method}, nil
}

// tokenizeHBALine splits a pg_hba.conf line into whitespace-separated tokens,
// keeping double-quoted text (which may hold blanks and #) inside its token.
// It returns the line up to its comment, trimmed.
func tokenizeHBALine(raw string) (string, []string, error) {
	var (
		tokens  []string
		current strings.Builder
		quoted  bool
		end     = len(raw)
	)
	flush := func() {
		if current.Len() > 0 {
			tokens = append(tokens, current.String())
			current.Reset()
		}
	}
loop:
	for i, r := range raw {
		switch {
		case r == '"':
			quoted = !quoted
			current.WriteRune(r)
		case quoted:
			current.WriteRune(r)
		case r == '#':
			end = i
			break loop
		case r == ' ' || r == '\t' || r == '\r':
			flush()
		default:
			current.WriteRune(r)
		}
	}
	if quoted {
		return "", nil, fmt.Errorf("unterminated quoted string")
	}
	flush()
	return strings.TrimSpace(raw[:end]), tokens, nil
}

// validateHBAList checks a comma-separated database or user field.
func validateHBAList(name, value string) error {
	for _, item := range strings.Split(value, ",") {
		switch {
		case item == "" || item == `""`:
			return fmt.Errorf("empty %s name", name)
		case strings.HasPrefix(item, "@"):
			return fmt.Errorf("%s file references (%s) are not supported", name, item)
		}
	}
	return nil
}

// validateHBAAddress checks the address field at the start of tokens and
// returns the number of tokens it takes: two for an IP address followed by
// a netmask, one otherwise.
func validateHBAAddress(tokens []string) (int, error) {
	address := tokens[0]
	switch {
	case address == "all" || address == "samehost" || address == "samenet":
		return 1, nil
	case strings.Contains(address, "/"):
		if _, _, err := net.ParseCIDR(address); err != nil {
			return 0, fmt.Errorf("invalid address %q: %w", address, err)
		}
		return 1, nil
	case net.ParseIP(address) != nil:
		if len(tokens) < 2 || net.ParseIP(tokens[1]) == nil {
			return 0, fmt.Errorf("address %q needs a CIDR suffix (e.g. %s/32) or a netmask", address, address)
		}
		return 2, nil
	case hbaHostname.MatchString(address):
		return 1, nil
	default:
		return 0, fmt.Errorf("invalid address %q; use all, samehost, samenet, a CIDR range or a host name", address)
	}
}
