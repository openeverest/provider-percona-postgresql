package provider

import (
	"context"
	"errors"
	"testing"

	apicommon "github.com/openeverest/openeverest/v2/api/common/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	monitoringv1alpha1 "github.com/openeverest/openeverest/v2/api/monitoring/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	"github.com/openeverest/openeverest/v2/provider-runtime/monitoring"
	"github.com/openeverest/provider-percona-postgresql/internal/common"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const (
	pmmTestNamespace = "db"
	pmmTestInstance  = "orders"
	pmmTestUID       = "5b1f0c7e-0000-4000-8000-00000000c0de"
	pmmTestToken     = "glsa_test-token"
)

func TestPMMAdminParamsLabelTheInstance(t *testing.T) {
	t.Parallel()

	in := &corev1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: "my-pg", Namespace: "db"}}
	c := controller.NewContext(t.Context(), nil, in, "provider-percona-postgresql")

	assert.Equal(t,
		"--custom-labels=namespace=db,core_openeverest_io_instance=my-pg",
		pmmAdminParams(c))
}

// newPMMTestContext returns a Context for in whose client holds objs and the
// Provider, which declares the pmm integration.
func newPMMTestContext(t *testing.T, in *corev1alpha1.Instance, objs ...client.Object) (*controller.Context, client.WithWatch) {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(s))
	require.NoError(t, corev1alpha1.AddToScheme(s))
	require.NoError(t, monitoringv1alpha1.AddToScheme(s))
	require.NoError(t, pgv2.AddToScheme(s))
	provider := &corev1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: common.ProviderName},
		Spec: corev1alpha1.ProviderSpec{Monitoring: &corev1alpha1.ProviderMonitoring{
			Integrations: map[string]corev1alpha1.MonitoringIntegration{"pmm": {AgentVersions: ">=3.0.0 <4.0.0", AuthModes: []string{"ServiceAccountToken"}}},
		}},
	}
	k8s := fake.NewClientBuilder().WithScheme(s).WithObjects(append(objs, provider)...).Build()
	return controller.NewContext(t.Context(), k8s, in, common.ProviderName), k8s
}

// monitoredInstance returns the Instance with one destination entry per
// name, each naming the MonitoringDestination of the same name.
func monitoredInstance(entries ...string) *corev1alpha1.Instance {
	in := &corev1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: pmmTestInstance, Namespace: pmmTestNamespace, UID: pmmTestUID}}
	if len(entries) == 0 {
		return in
	}
	in.Spec.Monitoring = &corev1alpha1.InstanceMonitoringSpec{}
	for _, e := range entries {
		in.Spec.Monitoring.Destinations = append(in.Spec.Monitoring.Destinations, corev1alpha1.InstanceMonitoringDestination{
			Name:           e,
			DestinationRef: apicommon.MonitoringDestinationRef{Kind: "MonitoringDestination", Name: e},
		})
	}
	return in
}

// acceptedBinding returns the binding core creates for entry, Accepted for
// its generation.
func acceptedBinding(entry, class string, mode corev1alpha1.MonitoringExecutionMode) *monitoringv1alpha1.MonitoringBinding {
	return &monitoringv1alpha1.MonitoringBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:       monitoring.BindingName(pmmTestInstance, entry),
			Namespace:  pmmTestNamespace,
			Generation: 2,
			Labels:     monitoring.BindingLabels(pmmTestInstance, entry, entry, class),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "core.openeverest.io/v1alpha1", Kind: "Instance", Name: pmmTestInstance, UID: pmmTestUID, Controller: new(true),
			}},
		},
		Spec: monitoringv1alpha1.MonitoringBindingSpec{
			InstanceRef:    apicommon.ObjectRef{Name: pmmTestInstance},
			DestinationRef: apicommon.MonitoringDestinationRef{Kind: "MonitoringDestination", Name: entry},
			ClassRef:       &apicommon.ObjectRef{Name: class},
			ExecutionMode:  mode,
		},
		Status: monitoringv1alpha1.MonitoringBindingStatus{Conditions: []metav1.Condition{{
			Type: monitoringv1alpha1.MonitoringBindingConditionAccepted, Status: metav1.ConditionTrue,
			Reason: "Accepted", ObservedGeneration: 2, LastTransitionTime: metav1.Now(),
		}}},
	}
}

func pmmClass() *monitoringv1alpha1.MonitoringClass {
	return &monitoringv1alpha1.MonitoringClass{
		ObjectMeta: metav1.ObjectMeta{Name: "pmm"},
		Spec: monitoringv1alpha1.MonitoringClassSpec{
			ProviderManaged: &monitoringv1alpha1.ProviderManagedFulfilment{
				Integration:  "pmm",
				AgentImage:   "percona/pmm-client:3.8.0",
				AgentVersion: "3.8.0",
			},
			AuthModes: []apicommon.AuthMode{{
				Name:        "ServiceAccountToken",
				Credentials: &apicommon.CredentialKeys{Required: []string{"token"}},
			}},
		},
	}
}

func pmmDestination() *monitoringv1alpha1.MonitoringDestination {
	return &monitoringv1alpha1.MonitoringDestination{
		ObjectMeta: metav1.ObjectMeta{Name: "pmm", Namespace: pmmTestNamespace},
		Spec: monitoringv1alpha1.MonitoringDestinationSpec{
			ClassRef:   apicommon.ObjectRef{Name: "pmm"},
			Parameters: &runtime.RawExtension{Raw: []byte(`{"url":"https://pmm.example.com"}`)},
			Authentication: apicommon.Authentication{
				Mode:                 "ServiceAccountToken",
				CredentialsSecretRef: &apicommon.SecretRef{Name: "pmm-token"},
			},
		},
	}
}

func pmmTokenSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "pmm-token", Namespace: pmmTestNamespace},
		Data:       map[string][]byte{"token": []byte(pmmTestToken)},
	}
}

// liveCluster returns the PerconaPGCluster as the operator runs it, with PMM
// sending to the old server.
func liveCluster() *pgv2.PerconaPGCluster {
	return &pgv2.PerconaPGCluster{
		ObjectMeta: metav1.ObjectMeta{Name: pmmTestInstance, Namespace: pmmTestNamespace},
		Spec: pgv2.PerconaPGClusterSpec{PMM: &pgv2.PMMSpec{
			Enabled:     true,
			ServerHost:  "pmm-old.example.com",
			Image:       "percona/pmm-client:3.7.1",
			Secret:      "orders-pmm-secret",
			QuerySource: pgv2.PgStatStatements,
		}},
	}
}

func TestApplyPMMRendersTheBinding(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		entries        []string
		existingMirror *corev1.Secret
	}{
		"creates the token Secret": {entries: []string{"pmm"}},
		"refreshes a stale token": {
			entries: []string{"pmm"},
			existingMirror: &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "orders-pmm-secret", Namespace: pmmTestNamespace},
				Data:       map[string][]byte{"PMM_SERVER_TOKEN": []byte("glsa_revoked")},
			},
		},
		"next to an entry without a binding": {entries: []string{"pmm", "prometheus"}},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			b := acceptedBinding("pmm", "pmm", corev1alpha1.MonitoringExecutionModeProviderManaged)
			b.Spec.Parameters = &runtime.RawExtension{Raw: []byte(`{"resources":{"requests":{"memory":"400Mi"},"limits":{"memory":"400Mi"}}}`)}
			objs := []client.Object{pmmClass(), pmmDestination(), pmmTokenSecret(), b, liveCluster()}
			if tt.existingMirror != nil {
				objs = append(objs, tt.existingMirror)
			}
			c, k8s := newPMMTestContext(t, monitoredInstance(tt.entries...), objs...)

			cluster := &pgv2.PerconaPGCluster{}
			require.NoError(t, applyPMM(c, cluster))

			got := cluster.Spec.PMM
			require.NotNil(t, got)
			assert.Equal(t, "400Mi", got.Resources.Requests.Memory().String())
			assert.Equal(t, "400Mi", got.Resources.Limits.Memory().String())
			got.Resources = corev1.ResourceRequirements{}
			assert.Equal(t, &pgv2.PMMSpec{
				Enabled:           true,
				ServerHost:        "pmm.example.com",
				Image:             "percona/pmm-client:3.8.0",
				Secret:            "orders-pmm-secret",
				CustomClusterName: "orders",
				PostgresParams:    "--custom-labels=namespace=db,core_openeverest_io_instance=orders",
				ImagePullPolicy:   corev1.PullIfNotPresent,
			}, got)

			mirror := &corev1.Secret{}
			require.NoError(t, k8s.Get(t.Context(), client.ObjectKey{Namespace: pmmTestNamespace, Name: "orders-pmm-secret"}, mirror))
			assert.Equal(t, pmmTestToken, string(mirror.Data["PMM_SERVER_TOKEN"]))
			require.Len(t, mirror.OwnerReferences, 1)
			assert.Equal(t, "Instance", mirror.OwnerReferences[0].Kind)
			assert.Equal(t, pmmTestInstance, mirror.OwnerReferences[0].Name)
			assert.Equal(t, new(true), mirror.OwnerReferences[0].Controller, "the Secret is deleted with the Instance")

			reason, message, staged := c.MonitoringBindingResult(b.Name)
			require.True(t, staged)
			assert.Equal(t, monitoringv1alpha1.ReasonAgentTLSNotVerified, reason)
			assert.Contains(t, message, "PMM_AGENT_SERVER_INSECURE_TLS=1")
		})
	}
}

func TestApplyPMMHoldsTheLiveRendering(t *testing.T) {
	t.Parallel()

	notAccepted := func() *monitoringv1alpha1.MonitoringBinding {
		b := acceptedBinding("pmm", "pmm", corev1alpha1.MonitoringExecutionModeProviderManaged)
		b.Status.Conditions[0].Status = metav1.ConditionFalse
		b.Status.Conditions[0].Reason = "ClassNotAccepted"
		return b
	}

	tests := map[string]struct {
		objs []client.Object
		live *pgv2.PerconaPGCluster
	}{
		"binding not Accepted": {
			objs: []client.Object{pmmClass(), pmmDestination(), pmmTokenSecret(), notAccepted()},
			live: liveCluster(),
		},
		"destination deleted": {
			objs: []client.Object{pmmClass(), pmmTokenSecret(), acceptedBinding("pmm", "pmm", corev1alpha1.MonitoringExecutionModeProviderManaged)},
			live: liveCluster(),
		},
		"binding not created yet": {
			objs: []client.Object{pmmClass(), pmmDestination(), pmmTokenSecret()},
			live: liveCluster(),
		},
		"nothing live to hold": {
			objs: []client.Object{pmmClass(), pmmDestination(), pmmTokenSecret(), notAccepted()},
			live: &pgv2.PerconaPGCluster{ObjectMeta: metav1.ObjectMeta{Name: pmmTestInstance, Namespace: pmmTestNamespace}},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			c, k8s := newPMMTestContext(t, monitoredInstance("pmm"), append(tt.objs, tt.live)...)

			cluster := &pgv2.PerconaPGCluster{}
			require.NoError(t, applyPMM(c, cluster))

			assert.Equal(t, tt.live.Spec.PMM, cluster.Spec.PMM)
			err := k8s.Get(t.Context(), client.ObjectKey{Namespace: pmmTestNamespace, Name: "orders-pmm-secret"}, &corev1.Secret{})
			assert.True(t, apierrors.IsNotFound(err), "a held binding copies no token, got %v", err)
		})
	}
}

func TestApplyPMMReportsRenderFailed(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		mutate        func(*monitoringv1alpha1.MonitoringClass, *monitoringv1alpha1.MonitoringDestination, *corev1.Secret)
		bindingParams string
		message       string
	}{
		"relative URL": {
			mutate: func(_ *monitoringv1alpha1.MonitoringClass, d *monitoringv1alpha1.MonitoringDestination, _ *corev1.Secret) {
				d.Spec.Parameters = &runtime.RawExtension{Raw: []byte(`{"url":"pmm.example.com"}`)}
			},
			message: `MonitoringDestination "pmm" parameters.url "pmm.example.com" is not an absolute URL`,
		},
		"undecodable destination parameters": {
			mutate: func(_ *monitoringv1alpha1.MonitoringClass, d *monitoringv1alpha1.MonitoringDestination, _ *corev1.Secret) {
				d.Spec.Parameters = &runtime.RawExtension{Raw: []byte(`{"url":443}`)}
			},
			message: `decode MonitoringDestination "pmm" parameters`,
		},
		"mode the class does not define": {
			mutate: func(_ *monitoringv1alpha1.MonitoringClass, d *monitoringv1alpha1.MonitoringDestination, _ *corev1.Secret) {
				d.Spec.Authentication = apicommon.Authentication{Mode: apicommon.AuthModeAnonymous}
			},
			message: `MonitoringClass "pmm" has no authentication mode "Anonymous"`,
		},
		"mode without a token": {
			mutate: func(cl *monitoringv1alpha1.MonitoringClass, d *monitoringv1alpha1.MonitoringDestination, _ *corev1.Secret) {
				cl.Spec.AuthModes = append(cl.Spec.AuthModes, apicommon.AuthMode{Name: apicommon.AuthModeAnonymous})
				d.Spec.Authentication = apicommon.Authentication{Mode: apicommon.AuthModeAnonymous}
			},
			message: `MonitoringDestination "pmm" authentication mode "Anonymous" provides no "token" credential`,
		},
		"credentials Secret missing": {
			mutate: func(_ *monitoringv1alpha1.MonitoringClass, _ *monitoringv1alpha1.MonitoringDestination, s *corev1.Secret) {
				s.Name = "another-token"
			},
			message: `get credentials Secret "pmm-token" of MonitoringDestination "pmm"`,
		},
		"credentials Secret without token": {
			mutate: func(_ *monitoringv1alpha1.MonitoringClass, _ *monitoringv1alpha1.MonitoringDestination, s *corev1.Secret) {
				s.Data = map[string][]byte{"apiKey": []byte(pmmTestToken)}
			},
			message: `missing key "token" required by authentication mode "ServiceAccountToken"`,
		},
		"class without agentImage": {
			mutate: func(cl *monitoringv1alpha1.MonitoringClass, _ *monitoringv1alpha1.MonitoringDestination, _ *corev1.Secret) {
				cl.Spec.ProviderManaged.AgentImage = ""
			},
			message: `MonitoringClass "pmm" sets no providerManaged.agentImage`,
		},
		"undecodable binding parameters": {
			bindingParams: `{"resources":{"limits":{"memory":"lots"}}}`,
			message:       `decode MonitoringBinding "` + monitoring.BindingName(pmmTestInstance, "pmm") + `" parameters`,
		},
		"request above its limit": {
			bindingParams: `{"resources":{"requests":{"cpu":"100m","memory":"500Mi"},"limits":{"cpu":"1","memory":"400Mi"}}}`,
			message:       `parameters.resources: memory request 500Mi exceeds its limit 400Mi`,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			class, dst, token := pmmClass(), pmmDestination(), pmmTokenSecret()
			if tt.mutate != nil {
				tt.mutate(class, dst, token)
			}
			b := acceptedBinding("pmm", "pmm", corev1alpha1.MonitoringExecutionModeProviderManaged)
			if tt.bindingParams != "" {
				b.Spec.Parameters = &runtime.RawExtension{Raw: []byte(tt.bindingParams)}
			}
			live := liveCluster()
			c, k8s := newPMMTestContext(t, monitoredInstance("pmm"), class, dst, token, b, live)

			cluster := &pgv2.PerconaPGCluster{}
			require.NoError(t, applyPMM(c, cluster))

			reason, message, staged := c.MonitoringBindingResult(b.Name)
			require.True(t, staged)
			assert.Equal(t, monitoringv1alpha1.ReasonRenderFailed, reason)
			assert.Contains(t, message, tt.message)
			assert.Equal(t, live.Spec.PMM, cluster.Spec.PMM, "a binding that fails to render keeps the live rendering")
			err := k8s.Get(t.Context(), client.ObjectKey{Namespace: pmmTestNamespace, Name: "orders-pmm-secret"}, &corev1.Secret{})
			assert.True(t, apierrors.IsNotFound(err), "no token is copied for a failed render, got %v", err)
		})
	}
}

func TestApplyPMMReturnsAPIErrors(t *testing.T) {
	t.Parallel()

	errUnavailable := errors.New("apiserver unavailable")
	tests := map[string]interceptor.Funcs{
		"token Secret apply fails": {
			Apply: func(context.Context, client.WithWatch, runtime.ApplyConfiguration, ...client.ApplyOption) error {
				return errUnavailable
			},
		},
	}
	for name, funcs := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			in := monitoredInstance("pmm")
			b := acceptedBinding("pmm", "pmm", corev1alpha1.MonitoringExecutionModeProviderManaged)
			_, k8s := newPMMTestContext(t, in, pmmClass(), pmmDestination(), pmmTokenSecret(), b, liveCluster())
			c := controller.NewContext(t.Context(), interceptor.NewClient(k8s, funcs), in, common.ProviderName)

			err := applyPMM(c, &pgv2.PerconaPGCluster{})

			require.ErrorIs(t, err, errUnavailable, "Sync fails and is retried")
			_, _, staged := c.MonitoringBindingResult(b.Name)
			assert.False(t, staged, "an API error is not reported as RenderFailed")
		})
	}
}

func TestApplyPMMReportsCredentialsReadFailure(t *testing.T) {
	t.Parallel()

	in := monitoredInstance("pmm")
	b := acceptedBinding("pmm", "pmm", corev1alpha1.MonitoringExecutionModeProviderManaged)
	live := liveCluster()
	_, k8s := newPMMTestContext(t, in, pmmClass(), pmmDestination(), pmmTokenSecret(), b, live)
	c := controller.NewContext(t.Context(), interceptor.NewClient(k8s, interceptor.Funcs{
		Get: func(ctx context.Context, k8s client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if key.Name == "pmm-token" {
				return errors.New("apiserver unavailable")
			}
			return k8s.Get(ctx, key, obj, opts...)
		},
	}), in, common.ProviderName)

	cluster := &pgv2.PerconaPGCluster{}
	require.NoError(t, applyPMM(c, cluster))

	reason, message, staged := c.MonitoringBindingResult(b.Name)
	require.True(t, staged)
	assert.Equal(t, monitoringv1alpha1.ReasonRenderFailed, reason)
	assert.Contains(t, message, "apiserver unavailable")
	assert.Equal(t, live.Spec.PMM, cluster.Spec.PMM, "the live rendering is kept")
}

func TestApplyPMMDisabledWithoutPMMEntry(t *testing.T) {
	t.Parallel()

	prometheus := &monitoringv1alpha1.MonitoringClass{
		ObjectMeta: metav1.ObjectMeta{Name: "prometheus"},
		Spec: monitoringv1alpha1.MonitoringClassSpec{
			ExtensionManaged: &monitoringv1alpha1.ExtensionManagedFulfilment{
				Requires: monitoringv1alpha1.SourceRequirements{Metrics: true},
			},
		},
	}

	tests := map[string]struct {
		in   *corev1alpha1.Instance
		objs []client.Object
		live *pgv2.PerconaPGCluster
		// mirrorKept is whether the token Secret survives: the operator may
		// still run the agent until the live cluster has PMM off.
		mirrorKept bool
	}{
		"no destinations": {
			in:         monitoredInstance(),
			live:       liveCluster(),
			mirrorKept: true,
		},
		"only an ExtensionManaged destination": {
			in:         monitoredInstance("prometheus"),
			objs:       []client.Object{prometheus, acceptedBinding("prometheus", "prometheus", corev1alpha1.MonitoringExecutionModeExtensionManaged)},
			live:       liveCluster(),
			mirrorKept: true,
		},
		"pmm entry removed while its binding exists": {
			in:         monitoredInstance(),
			objs:       []client.Object{pmmClass(), pmmDestination(), pmmTokenSecret(), acceptedBinding("pmm", "pmm", corev1alpha1.MonitoringExecutionModeProviderManaged)},
			live:       liveCluster(),
			mirrorKept: true,
		},
		"PMM already off in the live cluster": {
			in:   monitoredInstance(),
			live: &pgv2.PerconaPGCluster{ObjectMeta: metav1.ObjectMeta{Name: pmmTestInstance, Namespace: pmmTestNamespace}},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			mirror := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "orders-pmm-secret", Namespace: pmmTestNamespace},
				Data:       map[string][]byte{"PMM_SERVER_TOKEN": []byte(pmmTestToken)},
			}
			c, k8s := newPMMTestContext(t, tt.in, append(tt.objs, tt.live, mirror)...)

			cluster := &pgv2.PerconaPGCluster{}
			require.NoError(t, applyPMM(c, cluster))

			assert.Nil(t, cluster.Spec.PMM)
			err := k8s.Get(t.Context(), client.ObjectKey{Namespace: pmmTestNamespace, Name: "orders-pmm-secret"}, &corev1.Secret{})
			if tt.mirrorKept {
				assert.NoError(t, err)
			} else {
				assert.True(t, apierrors.IsNotFound(err), "the token Secret is deleted, got %v", err)
			}
		})
	}
}
