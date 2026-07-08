package provider

import pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"

// defaultSpec provides a minimal valid starting point for PerconaPGCluster.
func defaultSpec() pgv2.PerconaPGClusterSpec {
	return pgv2.PerconaPGClusterSpec{
		PostgresVersion: 16,
	}
}
