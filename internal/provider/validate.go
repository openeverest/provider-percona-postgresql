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
	"errors"

	"k8s.io/apimachinery/pkg/util/validation/field"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-percona-postgresql/internal/common"
)

// Validate checks the rules about the Instance alone. The runtime runs it once
// the Instance matches the topologies, components, versions and parameter
// schemas the Provider declares.
func (p *Provider) Validate(in controller.ValidationInput) controller.ValidationResult {
	spec := &in.Provider.Spec
	instance := in.Instance
	componentsPath := field.NewPath("spec", "components")

	var errs field.ErrorList
	bundle := map[string]string{}
	if name := controller.EffectiveVersionBundleName(spec, instance); name != "" {
		resolved, err := controller.ResolveVersionBundle(spec, name)
		if err != nil {
			errs = append(errs, field.Invalid(field.NewPath("spec", "version"), name, err.Error()))
		} else {
			bundle = resolved.Components
		}
	}

	enginePath := componentsPath.Key(common.ComponentEngine)
	if engine, ok := instance.Spec.Components[common.ComponentEngine]; !ok {
		errs = append(errs, field.Required(enginePath, ""))
	} else {
		switch {
		case engine.Replicas == nil:
			errs = append(errs, field.Required(enginePath.Child("replicas"), ""))
		case *engine.Replicas < 1:
			errs = append(errs, field.Invalid(enginePath.Child("replicas"), *engine.Replicas, "must be at least 1"))
		}
		errs = append(errs, validateImage(spec, enginePath, common.ComponentEngine, engine, bundle, componentTypePostgreSQL)...)
		if err := validateEngineConfiguration(engine); err != nil {
			errs = append(errs, field.Invalid(
				enginePath.Child("parameters", "configuration"), field.OmitValueType{}, err.Error(),
			))
		}
		if enabled, err := logCollectorEnabled(engine); err == nil && enabled != nil && *enabled &&
			controller.GetDefaultImage(spec, componentTypeFluentBit) == "" {
			errs = append(errs, field.Invalid(
				enginePath.Child("parameters", "logCollector", "enabled"), true,
				"the provider's catalog has no default "+componentTypeFluentBit+" image",
			))
		}
		if err := validateSpreadConstraints(engine.SchedulingPolicy); err != nil {
			errs = append(errs, field.Invalid(
				enginePath.Child("schedulingPolicy", "topologySpreadConstraints"), field.OmitValueType{}, err.Error(),
			))
		}
	}

	proxyPath := componentsPath.Key(common.ComponentProxy)
	if proxy, ok := instance.Spec.Components[common.ComponentProxy]; !ok {
		errs = append(errs, field.Required(proxyPath, ""))
	} else {
		switch {
		case proxy.Replicas == nil:
			errs = append(errs, field.Required(proxyPath.Child("replicas"), ""))
		case *proxy.Replicas < 0:
			errs = append(errs, field.Invalid(proxyPath.Child("replicas"), *proxy.Replicas, "must not be negative"))
		}
		errs = append(errs, validateImage(spec, proxyPath, common.ComponentProxy, proxy, bundle, componentTypePGBouncer)...)
		if err := validateProxyConfiguration(proxy); err != nil {
			errs = append(errs, field.Invalid(
				proxyPath.Child("parameters", "configuration"), field.OmitValueType{}, err.Error(),
			))
		}
	}

	if controller.GetDefaultImage(spec, componentTypePGBackRest) == "" {
		errs = append(errs, field.InternalError(field.NewPath("spec"),
			errors.New("the provider's catalog has no default pgbackrest image"),
		))
	}

	tls, err := tlsParameters(instance)
	if err != nil {
		errs = append(errs, field.Invalid(
			field.NewPath("spec", "parameters", "tls"), field.OmitValueType{}, err.Error(),
		))
	} else {
		errs = append(errs, validateTLSParameters(tls, field.NewPath("spec", "parameters", "tls"))...)
	}

	return controller.ValidationResult{Errors: errs}
}

// validateImage checks that the component's image can be resolved: from its
// image, from its version or the version bundle's, or from the catalog's
// default for componentType.
func validateImage(
	spec *corev1alpha1.ProviderSpec,
	path *field.Path,
	name string,
	component corev1alpha1.ComponentSpec,
	bundle map[string]string,
	componentType string,
) field.ErrorList {
	if component.Image != "" {
		return nil
	}
	if component.Version != "" {
		if controller.GetImageForVersion(spec, name, component.Version) == "" {
			return field.ErrorList{field.Invalid(path.Child("version"), component.Version,
				"the provider's catalog has no image for this version; set image",
			)}
		}
		return nil
	}
	if bundle[name] == "" && controller.GetDefaultImage(spec, componentType) == "" {
		return field.ErrorList{field.Required(path.Child("version"),
			"the provider's catalog has no default "+componentType+" image; set version or image",
		)}
	}
	return nil
}
