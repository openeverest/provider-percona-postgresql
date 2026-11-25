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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"

	"github.com/openeverest/provider-percona-postgresql/definition/backupclasses/pg"
)

// The pgBackRest backup types, the values of the --type flag and of
// PgBackupConfig.Type. A schedule selects its type via its Parameters.type
// (defaulting to full when unset); its Name is just a user-facing identifier
// -- unique across all storages on the Instance -- used verbatim as
// Backup.spec.scheduleName on mirrored Backup CRs.
const (
	backupTypeFull = "full"
	backupTypeDiff = "diff"
	backupTypeIncr = "incr"

	defaultBackupType = backupTypeFull
)

var backupTypes = []string{backupTypeFull, backupTypeDiff, backupTypeIncr}

// Neither the core webhooks nor the runtime check Backup.spec.parameters,
// Restore.spec.parameters or InstanceBackupSchedule.parameters against the
// pg BackupClass's schemas, so the provider does.

// parseBackupParameters decodes backup parameters (Backup.spec.parameters or
// InstanceBackupSchedule.parameters) as the pg class's parametersSchema
// declares them: no fields beyond PgBackupConfig's, and a known type if set.
func parseBackupParameters(raw *runtime.RawExtension) (pg.PgBackupConfig, error) {
	var cfg pg.PgBackupConfig
	if err := decodeStrict(raw, &cfg); err != nil {
		return pg.PgBackupConfig{}, err
	}
	if cfg.Type != "" && !slices.Contains(backupTypes, string(cfg.Type)) {
		return pg.PgBackupConfig{}, fmt.Errorf(
			"unsupported backup type %q: must be one of %s", cfg.Type, strings.Join(backupTypes, ", "),
		)
	}
	return cfg, nil
}

// backupTypeOf returns the pgBackRest type valid backup parameters select.
func backupTypeOf(cfg pg.PgBackupConfig) string {
	if cfg.Type == "" {
		return defaultBackupType
	}
	return string(cfg.Type)
}

// validateRestoreParameters checks restore parameters against the pg class's
// restoreParametersSchema, which declares no fields.
func validateRestoreParameters(raw *runtime.RawExtension) error {
	return decodeStrict(raw, &pg.PgRestoreConfig{})
}

// decodeStrict decodes a parameters payload into target, rejecting fields
// target does not declare. An absent or null payload leaves target as is.
func decodeStrict(raw *runtime.RawExtension, target any) error {
	if raw == nil || len(bytes.TrimSpace(raw.Raw)) == 0 {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw.Raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(target); err != nil {
		return fmt.Errorf("invalid parameters: %w", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("invalid parameters: unexpected data after the JSON object")
	}
	return nil
}

// validateBackupSchedules checks the schedules of each backup storage: each
// schedule's Name must be unique across the whole Instance (it becomes
// Backup.spec.scheduleName on mirrored Backup CRs). A schedule's effective
// backup type comes from its Parameters (default full).
func validateBackupSchedules(backup *corev1alpha1.InstanceBackupSpec) field.ErrorList {
	if backup == nil {
		return nil
	}
	var errs field.ErrorList
	storagesPath := field.NewPath("spec", "backup", "storages")
	seenNames := map[string]bool{}
	for i, storage := range backup.Storages {
		seenTypes := map[string]bool{}
		for j, schedule := range storage.Schedules {
			path := storagesPath.Index(i).Child("schedules").Index(j)

			if seenNames[schedule.Name] {
				errs = append(errs, field.Duplicate(path.Child("name"), schedule.Name))
			}
			seenNames[schedule.Name] = true

			cfg, err := parseBackupParameters(schedule.Parameters)
			if err != nil {
				errs = append(errs, field.Invalid(path.Child("parameters"), field.OmitValueType{}, err.Error()))
				continue
			}
			backupType := backupTypeOf(cfg)

			if schedule.Enabled {
				if seenTypes[backupType] {
					errs = append(errs, field.Invalid(path.Child("parameters", "type"), backupType,
						fmt.Sprintf("another enabled schedule on this storage already runs %q backups; "+
							"the PG operator supports only one schedule per backup type per storage", backupType),
					))
				}
				seenTypes[backupType] = true
			}

			if backupType == backupTypeDiff && schedule.Retention != nil &&
				schedule.Retention.Type != corev1alpha1.BackupScheduleRetentionTypeCount {
				errs = append(errs, field.NotSupported(path.Child("retention", "type"), schedule.Retention.Type,
					[]corev1alpha1.BackupScheduleRetentionType{corev1alpha1.BackupScheduleRetentionTypeCount},
				))
			}
		}
	}
	return errs
}
