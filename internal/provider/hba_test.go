package provider

import (
	"slices"
	"strings"
	"testing"

	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	crunchyv1beta1 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/upstream.pgv2.percona.com/v1beta1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openeverest/provider-percona-postgresql/definition"
)

func TestParsePgHBA(t *testing.T) {
	tests := []struct {
		name string
		text string
		want []hbaRule
	}{
		{name: "empty", text: ""},
		{name: "blank lines and comments", text: "\n  # only a comment\n\t\n"},
		{
			name: "records in order",
			text: `
# orders service from the app network
hostssl orders orders_app 10.0.0.0/8 scram-sha-256   # trailing comment
host    orders all        all        reject
`,
			want: []hbaRule{
				{Line: "hostssl orders orders_app 10.0.0.0/8 scram-sha-256", Connection: "hostssl", Method: "scram-sha-256"},
				{Line: "host    orders all        all        reject", Connection: "host", Method: "reject"},
			},
		},
		{
			name: "ip address with netmask",
			text: "host all all 192.168.1.0 255.255.255.0 md5",
			want: []hbaRule{{Line: "host all all 192.168.1.0 255.255.255.0 md5", Connection: "host", Method: "md5"}},
		},
		{
			name: "ipv6 cidr, lists and quoted names",
			text: `hostnossl "my db",app sales,"Bob Smith" ::1/128 reject`,
			want: []hbaRule{{Line: `hostnossl "my db",app sales,"Bob Smith" ::1/128 reject`, Connection: "hostnossl", Method: "reject"}},
		},
		{
			name: "host name and domain suffix",
			text: "hostssl all all .example.com scram-sha-256\nhostssl all all db-client.svc scram-sha-256",
			want: []hbaRule{
				{Line: "hostssl all all .example.com scram-sha-256", Connection: "hostssl", Method: "scram-sha-256"},
				{Line: "hostssl all all db-client.svc scram-sha-256", Connection: "hostssl", Method: "scram-sha-256"},
			},
		},
		{
			name: "cert with options",
			text: "hostssl all all samenet cert clientcert=verify-full map=certs",
			want: []hbaRule{{Line: "hostssl all all samenet cert clientcert=verify-full map=certs", Connection: "hostssl", Method: "cert"}},
		},
		{
			name: "ldap with a quoted option holding # and blanks",
			text: `hostssl all all all ldap ldapserver=ldap.example.com ldapbinddn="cn=svc #1, dc=example" # comment`,
			want: []hbaRule{{
				Line:       `hostssl all all all ldap ldapserver=ldap.example.com ldapbinddn="cn=svc #1, dc=example"`,
				Connection: "hostssl", Method: "ldap",
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parsePgHBA(tt.text)
			if err != nil {
				t.Fatalf("parsePgHBA() error = %v", err)
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("parsePgHBA() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParsePgHBAErrors(t *testing.T) {
	tests := []struct {
		name string
		text string
		want string
	}{
		{name: "trust", text: "host all all all trust", want: "line 1: method trust is not allowed"},
		{name: "reports the line number", text: "# ok\nhostssl all all all scram-sha-256\nhost all all all password", want: `line 3: unsupported authentication method "password"`},
		{name: "local", text: "local all all peer", want: "local records are not supported"},
		{name: "include", text: "include_dir conf.d", want: "include_dir directives are not supported"},
		{name: "unknown type", text: "hostgssenc all all all scram-sha-256", want: `unsupported connection type "hostgssenc"`},
		{name: "too few fields", text: "host all all scram-sha-256", want: "expected"},
		{name: "missing method after netmask", text: "host all all 10.0.0.0 255.0.0.0", want: "missing authentication method"},
		{name: "ip without mask", text: "host all all 10.0.0.1 scram-sha-256", want: "needs a CIDR suffix"},
		{name: "bad cidr", text: "host all all 10.0.0.0/33 scram-sha-256", want: "invalid address"},
		{name: "bad address", text: "host all all ${podselector:app} scram-sha-256", want: "invalid address"},
		{name: "file reference", text: "host all @admins all scram-sha-256", want: "user file references"},
		{name: "empty list item", text: "host app, all all scram-sha-256", want: "empty database name"},
		{name: "cert without ssl", text: "host all all all cert", want: "method cert requires a hostssl record"},
		{name: "bad option", text: "hostssl all all all ldap ldapserver", want: `option "ldapserver" must be NAME=VALUE`},
		{name: "unterminated quote", text: `host "app all all reject`, want: "unterminated quoted string"},
		{name: "continuation", text: `host all all all reject \`, want: "line continuations are not supported"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parsePgHBA(tt.text)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("parsePgHBA() error = %v, want it to contain %q", err, tt.want)
			}
		})
	}
}

func TestApplyPgHBA(t *testing.T) {
	t.Parallel()

	rule := func(line, connection, method string) crunchyv1beta1.PostgresAuthenticationRule {
		return crunchyv1beta1.PostgresAuthenticationRule{
			PostgresHBARule: crunchyv1beta1.PostgresHBARule{Connection: connection, Method: method},
			HBA:             line,
		}
	}
	app := rule("hostssl app app 10.0.0.0/8 scram-sha-256", "hostssl", "scram-sha-256")
	ldap := rule(`hostssl all all all ldap ldapserver=ldap.example.com ldapbinddn="cn=svc, dc=example"`, "hostssl", "ldap")
	rejectAll := rule("host all all all reject", "host", "reject")
	defaultRule := rule("hostssl all all all scram-sha-256", "hostssl", "scram-sha-256")
	rejectNonTLS := rule("hostnossl all all all reject", "hostnossl", "reject")

	tests := []struct {
		name  string
		pgHBA string
		mode  definition.TLSMode
		want  *crunchyv1beta1.PostgresClusterAuthentication
	}{
		{name: "unset keeps the operator defaults", mode: definition.TLSModePrefer},
		{name: "only comments keep the operator defaults", pgHBA: "# nothing yet\n\n", mode: definition.TLSModeRequire},
		{
			name: "records before the default record",
			pgHBA: `# app
hostssl app app 10.0.0.0/8 scram-sha-256   # app pods
hostssl all all all ldap ldapserver=ldap.example.com ldapbinddn="cn=svc, dc=example"
host all all all reject
`,
			mode: definition.TLSModePrefer,
			want: &crunchyv1beta1.PostgresClusterAuthentication{Rules: []crunchyv1beta1.PostgresAuthenticationRule{app, ldap, rejectAll, defaultRule}},
		},
		{
			name:  "requireTLS rejects unencrypted clients first",
			pgHBA: "hostssl app app 10.0.0.0/8 scram-sha-256",
			mode:  definition.TLSModeRequire,
			want:  &crunchyv1beta1.PostgresClusterAuthentication{Rules: []crunchyv1beta1.PostgresAuthenticationRule{rejectNonTLS, app, defaultRule}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cluster := &pgv2.PerconaPGCluster{}
			require.NoError(t, applyPgHBA(cluster, tt.pgHBA, definition.TLSParameters{Mode: tt.mode}))
			assert.Equal(t, tt.want, cluster.Spec.Authentication)
		})
	}
}

func TestApplyPgHBA_Invalid(t *testing.T) {
	t.Parallel()

	cluster := &pgv2.PerconaPGCluster{}
	err := applyPgHBA(cluster, "host all all all trust", definition.TLSParameters{})
	require.ErrorContains(t, err, "method trust is not allowed")
	assert.Nil(t, cluster.Spec.Authentication)
}
