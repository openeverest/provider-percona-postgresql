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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	apicommon "github.com/openeverest/openeverest/v2/api/common/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	pgv2 "github.com/percona/percona-postgresql-operator/v3/pkg/apis/pgv2.percona.com/v2"
)

func raw(s string) *runtime.RawExtension {
	return &runtime.RawExtension{Raw: []byte(s)}
}

func TestParseBackupParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		params   *runtime.RawExtension
		wantType string
		wantErr  string
	}{
		{name: "absent", wantType: "full"},
		{name: "empty", params: raw(""), wantType: "full"},
		{name: "null", params: raw("null"), wantType: "full"},
		{name: "empty object", params: raw("{}"), wantType: "full"},
		{name: "full", params: raw(`{"type":"full"}`), wantType: "full"},
		{name: "diff", params: raw(`{"type":"diff"}`), wantType: "diff"},
		{name: "incr", params: raw(`{"type":"incr"}`), wantType: "incr"},
		{name: "unknown type", params: raw(`{"type":"weekly"}`), wantErr: `unsupported backup type "weekly"`},
		{name: "upper-case type", params: raw(`{"type":"DIFF"}`), wantErr: `unsupported backup type "DIFF"`},
		{
			name:    "extra pgBackRest options in type",
			params:  raw(`{"type":"full --repo1-retention-full=1"}`),
			wantErr: "unsupported backup type",
		},
		{name: "type of the wrong JSON type", params: raw(`{"type":5}`), wantErr: "cannot unmarshal number"},
		{name: "undeclared field", params: raw(`{"typ":"diff"}`), wantErr: `unknown field "typ"`},
		{name: "not an object", params: raw(`"diff"`), wantErr: "invalid parameters"},
		{name: "trailing data", params: raw(`{"type":"diff"}{}`), wantErr: "unexpected data"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg, err := parseBackupParameters(tt.params)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantType, string(backupTypeOf(cfg)))
		})
	}
}

func TestValidateRestoreParameters(t *testing.T) {
	t.Parallel()

	require.NoError(t, validateRestoreParameters(nil))
	require.NoError(t, validateRestoreParameters(raw("{}")))
	require.ErrorContains(t, validateRestoreParameters(raw(`{"target":"x"}`)), `unknown field "target"`)
}

func TestSyncBackupRejectsInvalidParameters(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, backupv1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	require.NoError(t, pgv2.AddToScheme(scheme))

	instance := &corev1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: "pg-prod", Namespace: "everest"}}
	backup := &backupv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "b1",
			Namespace: "everest",
			Labels:    map[string]string{instanceNameLabelKey: "pg-prod"},
		},
		Spec: backupv1alpha1.BackupSpec{
			Origin: backupv1alpha1.BackupOrigin{
				Type:        backupv1alpha1.BackupOriginTypeInstance,
				InstanceRef: &apicommon.ObjectRef{Name: "pg-prod"},
			},
			ClassRef:   apicommon.ObjectRef{Name: "pg"},
			StorageRef: apicommon.ObjectRef{Name: "minio"},
			Parameters: raw(`{"type":"weekly"}`),
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance, backup.DeepCopy()).Build()
	c := controller.NewContext(context.Background(), k8sClient, instance, "provider-percona-postgresql")

	status, err := (&Provider{}).SyncBackup(c, backup)

	require.NoError(t, err)
	assert.Equal(t, backupv1alpha1.BackupStateFailed, status.State)
	assert.Contains(t, status.Message, `spec.parameters: unsupported backup type "weekly"`)
}

func TestSyncRestoreRejectsInvalidParameters(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, backupv1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	require.NoError(t, pgv2.AddToScheme(scheme))

	instance := &corev1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: "pg-prod", Namespace: "everest"}}
	restore := &backupv1alpha1.Restore{
		ObjectMeta: metav1.ObjectMeta{Name: "r1", Namespace: "everest"},
		Spec: backupv1alpha1.RestoreSpec{
			InstanceRef: apicommon.ObjectRef{Name: "pg-prod"},
			DataSource: backupv1alpha1.DataSource{
				Type:   backupv1alpha1.DataSourceTypeBackup,
				Backup: &backupv1alpha1.DataSourceBackup{BackupRef: apicommon.ObjectRef{Name: "b1"}},
			},
			Parameters: raw(`{"target":"x"}`),
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance, restore.DeepCopy()).Build()
	c := controller.NewContext(context.Background(), k8sClient, instance, "provider-percona-postgresql")

	status, err := (&Provider{}).SyncRestore(c, restore)

	require.NoError(t, err)
	assert.Equal(t, backupv1alpha1.RestoreStateFailed, status.State)
	assert.Contains(t, status.Message, `spec.parameters: invalid parameters: json: unknown field "target"`)
}
