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

	"github.com/openeverest/provider-percona-postgresql/definition/backupclasses/pg"
)

// Neither the core webhooks nor the runtime check Backup.spec.parameters or
// Restore.spec.parameters against the "pg" BackupClass's schemas, so the
// provider does, rejecting unknown fields and invalid values instead of
// silently falling back to defaults.

// parseBackupParameters decodes Backup.spec.parameters as the "pg" class's
// parametersSchema declares them: no fields beyond PgBackupConfig's, and a
// known backup type if set.
func parseBackupParameters(raw *runtime.RawExtension) (pg.PgBackupConfig, error) {
	var cfg pg.PgBackupConfig
	if err := decodeStrict(raw, &cfg); err != nil {
		return pg.PgBackupConfig{}, err
	}
	if cfg.Type != "" && !slices.Contains(pg.PgBackupTypes, cfg.Type) {
		return pg.PgBackupConfig{}, fmt.Errorf(
			"unsupported backup type %q: must be one of %s", cfg.Type, joinBackupTypes(pg.PgBackupTypes),
		)
	}
	return cfg, nil
}

// backupTypeOf returns the pgBackRest type valid backup parameters select,
// defaulting to defaultBackupType when unset.
func backupTypeOf(cfg pg.PgBackupConfig) pg.PgBackupType {
	if cfg.Type == "" {
		return defaultBackupType
	}
	return cfg.Type
}

// validateRestoreParameters checks Restore.spec.parameters against the "pg"
// class's restoreParametersSchema, which declares no fields.
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

func joinBackupTypes(types []pg.PgBackupType) string {
	s := make([]string, len(types))
	for i, t := range types {
		s[i] = string(t)
	}
	return strings.Join(s, ", ")
}
