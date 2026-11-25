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
	"errors"
	"testing"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	apicommon "github.com/openeverest/openeverest/v2/api/common/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	"github.com/openeverest/provider-percona-postgresql/internal/common"
	pgv2 "github.com/percona/percona-postgresql-operator/v3/pkg/apis/pgv2.percona.com/v2"
	upstreamv1beta1 "github.com/percona/percona-postgresql-operator/v3/pkg/apis/upstream.pgv2.percona.com/v1beta1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	statusTestName      = "my-pg"
	statusTestNamespace = "db"
	statusTestImage     = "percona/percona-distribution-postgresql:17.5-1"
	statusTestOldImage  = "percona/percona-distribution-postgresql:17.4-1"
	statusTestHost      = "my-pg-pgbouncer.db.svc"
)

// statusInstanceSpec is the spec of an Instance with three PostgreSQL
// instances and one PGBouncer pod. Tests change what they are about.
func statusInstanceSpec(changes ...func(*corev1alpha1.InstanceSpec)) corev1alpha1.InstanceSpec {
	spec := corev1alpha1.InstanceSpec{
		ProviderRef: apicommon.ObjectRef{Name: common.ProviderName},
		Components: map[string]corev1alpha1.ComponentSpec{
			common.ComponentEngine: {Replicas: new(int32(3))},
			common.ComponentProxy:  {Replicas: new(int32(1))},
		},
	}
	for _, change := range changes {
		change(&spec)
	}
	return spec
}

// statusContext builds a context for an Instance named my-pg with the given
// spec, and seeds the Kubernetes cluster with objs, plus the user Secret the
// connection details are read from.
func statusContext(t *testing.T, spec corev1alpha1.InstanceSpec, funcs *interceptor.Funcs, objs ...client.Object) *controller.Context {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	require.NoError(t, backupv1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, discoveryv1.AddToScheme(scheme))
	require.NoError(t, pgv2.AddToScheme(scheme))

	in := &corev1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: statusTestName, Namespace: statusTestNamespace},
		Spec:       spec,
	}
	user := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: statusTestName + "-pguser-postgres", Namespace: statusTestNamespace},
		Data: map[string][]byte{
			"user":           []byte("postgres"),
			"password":       []byte("secret"),
			"host":           []byte("my-pg-primary.db.svc"),
			"uri":            []byte("postgresql://postgres:secret@my-pg-primary.db.svc:5432/postgres"),
			"pgbouncer-host": []byte(statusTestHost),
			"pgbouncer-uri":  []byte("postgresql://postgres:secret@" + statusTestHost + ":5432/postgres"),
		},
	}

	builder := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(append(objs, in, user)...).
		WithIndex(&backupv1alpha1.Restore{}, controller.IndexRestoreInstanceName, func(obj client.Object) []string {
			return []string{obj.(*backupv1alpha1.Restore).Spec.InstanceRef.Name}
		})
	if funcs != nil {
		builder = builder.WithInterceptorFuncs(*funcs)
	}
	return controller.NewContext(context.Background(), builder.Build(), in, common.ProviderName)
}

// readyCluster returns a cluster the operator reports ready for its latest
// generation: three PostgreSQL instances and one PGBouncer pod.
func readyCluster(changes ...func(*pgv2.PerconaPGCluster)) *pgv2.PerconaPGCluster {
	cluster := &pgv2.PerconaPGCluster{
		ObjectMeta: metav1.ObjectMeta{Name: statusTestName, Namespace: statusTestNamespace, Generation: 3},
		Spec: pgv2.PerconaPGClusterSpec{
			Image: statusTestImage,
			InstanceSets: pgv2.PGInstanceSets{{
				Name:     "instance1",
				Replicas: new(int32(3)),
				DataVolumeClaimSpec: corev1.PersistentVolumeClaimSpec{
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
					},
				},
			}},
			Proxy: &pgv2.PGProxySpec{PGBouncer: &pgv2.PGBouncerSpec{Replicas: new(int32(1))}},
		},
		Status: pgv2.PerconaPGClusterStatus{
			State:              pgv2.AppStateReady,
			ObservedGeneration: 3,
			Host:               statusTestHost,
			Postgres:           pgv2.PostgresStatus{Size: 3, Ready: 3},
			PGBouncer:          pgv2.PGBouncerStatus{Size: 1, Ready: 1},
		},
	}
	for _, change := range changes {
		change(cluster)
	}
	return cluster
}

func clusterState(state pgv2.AppState) func(*pgv2.PerconaPGCluster) {
	return func(cluster *pgv2.PerconaPGCluster) {
		cluster.Status.State = state
	}
}

func clusterCondition(condType string, status metav1.ConditionStatus, reason, message string) func(*pgv2.PerconaPGCluster) {
	return func(cluster *pgv2.PerconaPGCluster) {
		cluster.Status.Conditions = append(cluster.Status.Conditions, metav1.Condition{
			Type: condType, Status: status, Reason: reason, Message: message,
		})
	}
}

// leaderSlice is the EndpointSlice Kubernetes mirrors from the endpoint
// Patroni points at the primary.
func leaderSlice(ready bool) *discoveryv1.EndpointSlice {
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      statusTestName + "-ha-x7k2p",
			Namespace: statusTestNamespace,
			Labels: map[string]string{
				discoveryv1.LabelServiceName: statusTestName + "-ha",
				labelCluster:                 statusTestName,
			},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses:  []string{"10.42.0.12"},
			Conditions: discoveryv1.EndpointConditions{Ready: new(ready)},
		}},
	}
}

// instanceSet returns the StatefulSet of one PostgreSQL instance, its pod
// ready and running its latest template.
func instanceSet(name string, changes ...func(*appsv1.StatefulSet)) *appsv1.StatefulSet {
	set := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:       statusTestName + "-instance1-" + name,
			Namespace:  statusTestNamespace,
			Generation: 2,
			Labels:     map[string]string{labelCluster: statusTestName, labelInstanceSet: "instance1"},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:       new(int32(1)),
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType},
		},
		Status: appsv1.StatefulSetStatus{
			ObservedGeneration: 2,
			Replicas:           1,
			ReadyReplicas:      1,
			UpdatedReplicas:    1,
			// With OnDelete the controller never moves currentRevision.
			CurrentRevision: name + "-1",
			UpdateRevision:  name + "-2",
		},
	}
	for _, change := range changes {
		change(set)
	}
	return set
}

// pgBouncerDeployment returns the PGBouncer Deployment, rolled out.
func pgBouncerDeployment(changes ...func(*appsv1.Deployment)) *appsv1.Deployment {
	d := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:       statusTestName + "-pgbouncer",
			Namespace:  statusTestNamespace,
			Generation: 4,
			Labels:     map[string]string{labelCluster: statusTestName, labelRole: rolePGBouncer},
		},
		Spec: appsv1.DeploymentSpec{Replicas: new(int32(1))},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 4,
			Replicas:           1,
			UpdatedReplicas:    1,
			ReadyReplicas:      1,
			AvailableReplicas:  1,
		},
	}
	for _, change := range changes {
		change(d)
	}
	return d
}

// instancePod returns the pod of a PostgreSQL instance running image.
func instancePod(name, image string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      statusTestName + "-instance1-" + name + "-0",
			Namespace: statusTestNamespace,
			Labels:    map[string]string{labelCluster: statusTestName, labelData: dataPostgres},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{
			{Name: containerDatabase, Image: image},
			{Name: "pgbackrest", Image: "percona/percona-pgbackrest:2.55.0"},
		}},
	}
}

// dataVolume returns the data volume of a PostgreSQL instance, requesting
// size, with the conditions of its PersistentVolumeClaim.
func dataVolume(name, size string, conditions ...corev1.PersistentVolumeClaimConditionType) *corev1.PersistentVolumeClaim {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      statusTestName + "-instance1-" + name + "-pgdata",
			Namespace: statusTestNamespace,
			Labels: map[string]string{
				labelCluster:     statusTestName,
				labelInstanceSet: "instance1",
				labelRole:        rolePGData,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(size)},
			},
		},
	}
	for _, condType := range conditions {
		pvc.Status.Conditions = append(pvc.Status.Conditions, corev1.PersistentVolumeClaimCondition{
			Type: condType, Status: corev1.ConditionTrue,
		})
	}
	return pvc
}

func restoreInState(name string, state backupv1alpha1.RestoreState) *backupv1alpha1.Restore {
	return &backupv1alpha1.Restore{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: statusTestNamespace},
		Spec:       backupv1alpha1.RestoreSpec{InstanceRef: apicommon.ObjectRef{Name: statusTestName}},
		Status:     backupv1alpha1.RestoreStatus{State: state},
	}
}

// rolledOut returns the workloads and pods of a cluster that runs its latest
// pod templates and image, with a primary.
func rolledOut() []client.Object {
	return []client.Object{
		instanceSet("a1b2"), instanceSet("c3d4"), instanceSet("e5f6"),
		pgBouncerDeployment(),
		instancePod("a1b2", statusTestImage), instancePod("c3d4", statusTestImage), instancePod("e5f6", statusTestImage),
		leaderSlice(true),
	}
}

// without returns objs without the ones named in names.
func without(objs []client.Object, names ...string) []client.Object {
	out := make([]client.Object, 0, len(objs))
	for _, obj := range objs {
		keep := true
		for _, name := range names {
			if obj.GetName() == name {
				keep = false
			}
		}
		if keep {
			out = append(out, obj)
		}
	}
	return out
}

func TestStatus(t *testing.T) {
	t.Parallel()

	slice := statusTestName + "-ha-x7k2p"
	firstSet := statusTestName + "-instance1-a1b2"
	firstPod := firstSet + "-0"
	deployment := statusTestName + "-pgbouncer"
	listSlicesFails := &interceptor.Funcs{
		List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*discoveryv1.EndpointSliceList); ok {
				return errors.New("endpointslices is forbidden")
			}
			return cl.List(ctx, list, opts...)
		},
	}
	seeded := func(spec *corev1alpha1.InstanceSpec) {
		spec.DataSource = &backupv1alpha1.DataSource{
			Type:   backupv1alpha1.DataSourceTypeBackup,
			Backup: &backupv1alpha1.DataSourceBackup{BackupRef: apicommon.ObjectRef{Name: "nightly"}},
		}
	}

	tests := []struct {
		name string
		spec []func(*corev1alpha1.InstanceSpec)
		// cluster is the PerconaPGCluster object; nil when it does not exist.
		cluster *pgv2.PerconaPGCluster
		// objs are the other objects in the Kubernetes cluster; nil stands
		// for rolledOut().
		objs  []client.Object
		funcs *interceptor.Funcs

		converged bool
		// message is what the provider waits for, in part.
		message string
		// available, availableReason and availableMessage (in part) say
		// whether clients can connect.
		available        controller.Availability
		availableReason  string
		availableMessage string
		// problems are the reported problems, messages in part.
		problems []controller.Problem
		// operations are the reported operations, as "Type/Reason".
		operations []string
		// noDetails is true when no connection details are reported although
		// the cluster is available.
		noDetails bool
	}{
		{
			name:    "the cluster object does not exist yet",
			message: "Waiting for the PerconaPGCluster object to be created",
			// The provider cannot tell, so it leaves the answer to the runtime.
			available: controller.AvailabilityUnreported,
		},
		{
			name: "a new cluster the operator has not picked up",
			cluster: readyCluster(func(cluster *pgv2.PerconaPGCluster) {
				cluster.Generation = 1
				cluster.Status = pgv2.PerconaPGClusterStatus{}
			}),
			objs:             []client.Object{},
			message:          "Waiting for the operator to start creating the cluster",
			available:        controller.Unavailable,
			availableReason:  reasonNoPrimary,
			availableMessage: "No PostgreSQL primary serves connections",
		},
		{
			name: "a new cluster coming up",
			cluster: readyCluster(clusterState(pgv2.AppStateInit), func(cluster *pgv2.PerconaPGCluster) {
				cluster.Status.Postgres.Ready = 1
				cluster.Status.PGBouncer.Ready = 0
			}),
			objs:            without(rolledOut(), slice),
			message:         "Waiting for the PostgreSQL instances (1 of 3 ready); PGBouncer (0 of 1 ready)",
			available:       controller.Unavailable,
			availableReason: reasonNoPrimary,
		},
		{
			name: "a new cluster waiting for its repository host",
			cluster: readyCluster(clusterState(pgv2.AppStateInit), func(cluster *pgv2.PerconaPGCluster) {
				cluster.Status.PGBackRest = &upstreamv1beta1.PGBackRestStatus{
					RepoHost: &upstreamv1beta1.RepoHostStatus{Ready: false},
				}
			}),
			message:   "Waiting for the pgBackRest repository host",
			available: controller.Available,
		},
		{
			name:      "ready and caught up",
			cluster:   readyCluster(),
			converged: true,
			available: controller.Available,
		},
		{
			name: "ready but the operator has not seen the latest generation",
			cluster: readyCluster(func(cluster *pgv2.PerconaPGCluster) {
				cluster.Generation = 4
			}),
			message:   "Waiting for the operator to apply the latest configuration",
			available: controller.Available,
		},
		{
			name:    "ready but a StatefulSet has not picked up its new pod template",
			cluster: readyCluster(),
			objs: append(without(rolledOut(), firstSet), instanceSet("a1b2", func(set *appsv1.StatefulSet) {
				set.Generation = 3
			})),
			message:   "Waiting for StatefulSet my-pg-instance1-a1b2 to pick up its new pod template",
			available: controller.Available,
		},
		{
			name:    "ready but the rolling restart has not finished",
			cluster: readyCluster(),
			objs: append(without(rolledOut(), firstSet), instanceSet("a1b2", func(set *appsv1.StatefulSet) {
				set.Status.UpdatedReplicas = 0
			})),
			message:   "Waiting for StatefulSet my-pg-instance1-a1b2 to roll out (0 of 1 pods updated)",
			available: controller.Available,
		},
		{
			name:    "ready but PGBouncer has not rolled out",
			cluster: readyCluster(),
			objs: append(without(rolledOut(), deployment), pgBouncerDeployment(func(d *appsv1.Deployment) {
				d.Status.UpdatedReplicas = 0
				d.Status.Replicas = 2
			})),
			message:   "Waiting for Deployment my-pg-pgbouncer to roll out (0 of 1 pods updated)",
			available: controller.Available,
		},
		{
			name:    "a rolling restart with a primary still serving",
			cluster: readyCluster(clusterState(pgv2.AppStateInit), func(cluster *pgv2.PerconaPGCluster) { cluster.Status.Postgres.Ready = 2 }),
			objs: append(without(rolledOut(), firstSet), instanceSet("a1b2", func(set *appsv1.StatefulSet) {
				set.Status.UpdatedReplicas = 0
				set.Status.ReadyReplicas = 0
			})),
			message:   "Waiting for the PostgreSQL instances (2 of 3 ready)",
			available: controller.Available,
		},
		{
			name:             "ready but no primary",
			cluster:          readyCluster(),
			objs:             append(without(rolledOut(), slice), leaderSlice(false)),
			message:          "Waiting for Patroni to elect a primary",
			available:        controller.Unavailable,
			availableReason:  reasonNoPrimary,
			availableMessage: "No PostgreSQL primary serves connections",
		},
		{
			name:             "the leader Service's endpoints cannot be read",
			cluster:          readyCluster(),
			funcs:            listSlicesFails,
			converged:        true,
			available:        controller.AvailabilityUnknown,
			availableReason:  reasonPrimaryServiceUnreadable,
			availableMessage: "endpointslices is forbidden",
		},
		{
			name: "no PGBouncer pod ready",
			cluster: readyCluster(clusterState(pgv2.AppStateInit), func(cluster *pgv2.PerconaPGCluster) {
				cluster.Status.PGBouncer.Ready = 0
			}),
			message:          "Waiting for PGBouncer (0 of 1 ready)",
			available:        controller.Unavailable,
			availableReason:  reasonNoPGBouncerReady,
			availableMessage: "No PGBouncer pod is ready",
		},
		{
			name: "without PGBouncer",
			spec: []func(*corev1alpha1.InstanceSpec){func(spec *corev1alpha1.InstanceSpec) {
				spec.Components[common.ComponentProxy] = corev1alpha1.ComponentSpec{Replicas: new(int32(0))}
			}},
			cluster: readyCluster(func(cluster *pgv2.PerconaPGCluster) {
				cluster.Spec.Proxy.PGBouncer.Replicas = new(int32(0))
				cluster.Status.PGBouncer = pgv2.PGBouncerStatus{}
			}),
			objs:      without(rolledOut(), deployment),
			converged: true,
			available: controller.Available,
		},
		{
			name: "a PGBouncer load balancer without an address",
			cluster: readyCluster(func(cluster *pgv2.PerconaPGCluster) {
				cluster.Spec.Proxy.PGBouncer.ServiceExpose = &pgv2.ServiceExpose{Type: string(corev1.ServiceTypeLoadBalancer)}
				cluster.Status.Host = ""
			}),
			message:         "Waiting for the PGBouncer load balancer address",
			available:       controller.Unavailable,
			availableReason: reasonLoadBalancerPending,
		},
		{
			name: "stopped outside OpenEverest",
			cluster: readyCluster(clusterState(pgv2.AppStatePaused), func(cluster *pgv2.PerconaPGCluster) {
				cluster.Spec.Pause = new(true)
				cluster.Status.Postgres = pgv2.PostgresStatus{}
				cluster.Status.PGBouncer = pgv2.PGBouncerStatus{}
			}),
			objs:            []client.Object{},
			message:         "The cluster is stopped on its PerconaPGCluster object",
			available:       controller.Unavailable,
			availableReason: reasonPaused,
			problems: []controller.Problem{{
				Reason: reasonPausedOutsideOpenEverest, Terminal: true,
				Message: "spec.pause is set on its PerconaPGCluster object, outside OpenEverest",
			}},
		},
		{
			name: "stopping outside OpenEverest, pods still running",
			cluster: readyCluster(clusterState(pgv2.AppStateStopping), func(cluster *pgv2.PerconaPGCluster) {
				cluster.Spec.Pause = new(true)
			}),
			message:         "The cluster is stopped on its PerconaPGCluster object",
			available:       controller.Unavailable,
			availableReason: reasonPaused,
			problems: []controller.Problem{{
				Reason: reasonPausedOutsideOpenEverest, Terminal: true, Message: "Unset it to start the database again",
			}},
		},
		{
			name: "unmanaged outside OpenEverest",
			cluster: readyCluster(func(cluster *pgv2.PerconaPGCluster) {
				cluster.Spec.Unmanaged = new(true)
			}),
			message:   "spec.unmanaged is set",
			available: controller.Available,
			problems: []controller.Problem{{
				Reason: reasonUnmanagedOutsideOpenEverest, Terminal: true,
				Message: "spec.unmanaged is set on its PerconaPGCluster object, outside OpenEverest",
			}},
		},
		{
			name:            "a restore running",
			cluster:         readyCluster(clusterState(pgv2.AppStateInit)),
			objs:            append(rolledOut(), restoreInState("nightly-restore", backupv1alpha1.RestoreStateRunning)),
			message:         "Restore nightly-restore is running",
			available:       controller.Unavailable,
			availableReason: reasonRestoring,
			operations:      []string{"Restoring/Restoring"},
		},
		{
			name:    "a restore not started yet",
			cluster: readyCluster(),
			objs: append(rolledOut(),
				restoreInState("b-restore", ""), restoreInState("a-restore", backupv1alpha1.RestoreStatePending)),
			message:         "Restore a-restore is running",
			available:       controller.Unavailable,
			availableReason: reasonRestoring,
			operations:      []string{"Restoring/Restoring"},
		},
		{
			name:    "restores that have ended",
			cluster: readyCluster(),
			objs: append(rolledOut(),
				restoreInState("ok", backupv1alpha1.RestoreStateSucceeded), restoreInState("ko", backupv1alpha1.RestoreStateFailed)),
			converged: true,
			available: controller.Available,
		},
		{
			// The runtime reports the seeding from the data source itself.
			name:      "a new instance being seeded",
			spec:      []func(*corev1alpha1.InstanceSpec){seeded},
			cluster:   readyCluster(),
			objs:      append(rolledOut(), restoreInState(statusTestName+controller.DataSourceRestoreNameSuffix, backupv1alpha1.RestoreStateRunning)),
			converged: true,
			available: controller.Available,
		},
		{
			name: "the restore of the data failed",
			cluster: readyCluster(clusterState(pgv2.AppStateInit),
				func(cluster *pgv2.PerconaPGCluster) { cluster.Status.Postgres.Ready = 0 },
				clusterCondition(conditionPostgresDataInitialized, metav1.ConditionFalse, reasonPGBackRestRestoreFailed, "pgBackRest restore failed")),
			objs:      append(without(rolledOut(), slice), restoreInState("nightly-restore", backupv1alpha1.RestoreStateFailed)),
			message:   "Waiting for the PostgreSQL instances (0 of 3 ready)",
			available: controller.Unavailable, availableReason: reasonNoPrimary,
			problems: []controller.Problem{{
				Reason:  reasonRestoreFailed,
				Message: "pgBackRest restore failed. The logs of Job my-pg-pgbackrest-restore say why",
			}},
		},
		{
			name: "a restore running after one that failed",
			cluster: readyCluster(clusterState(pgv2.AppStateInit),
				clusterCondition(conditionPostgresDataInitialized, metav1.ConditionFalse, reasonPGBackRestRestoreFailed, "pgBackRest restore failed")),
			objs:            append(rolledOut(), restoreInState("retry", backupv1alpha1.RestoreStateRunning)),
			message:         "Restore retry is running",
			available:       controller.Unavailable,
			availableReason: reasonRestoring,
			operations:      []string{"Restoring/Restoring"},
		},
		{
			name:    "volumes being resized",
			cluster: readyCluster(clusterCondition(upstreamv1beta1.PersistentVolumeResizing, metav1.ConditionTrue, "Resizing", "One or more volumes are changing size")),
			objs: append(rolledOut(),
				dataVolume("a1b2", "20Gi", corev1.PersistentVolumeClaimFileSystemResizePending), dataVolume("c3d4", "20Gi")),
			message:    "Waiting for the volumes to be resized",
			available:  controller.Available,
			operations: []string{"StorageResizing/ResizingVolumes"},
		},
		{
			name:      "the operator still reports a resize that has finished",
			cluster:   readyCluster(clusterCondition(upstreamv1beta1.PersistentVolumeResizing, metav1.ConditionTrue, "Resizing", "One or more volumes are changing size")),
			objs:      append(rolledOut(), dataVolume("a1b2", "10Gi"), dataVolume("c3d4", "10Gi")),
			converged: true,
			available: controller.Available,
		},
		{
			name: "a resize the API server refused",
			cluster: readyCluster(
				func(cluster *pgv2.PerconaPGCluster) {
					cluster.Spec.InstanceSets[0].DataVolumeClaimSpec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("20Gi")
				},
				clusterCondition(upstreamv1beta1.PersistentVolumeResizing, metav1.ConditionFalse, "Forbidden", "One or more volumes cannot be resized")),
			objs:      append(rolledOut(), dataVolume("a1b2", "10Gi"), dataVolume("c3d4", "10Gi")),
			message:   "Waiting for the volumes to be resized, which the operator cannot do",
			available: controller.Available,
			problems: []controller.Problem{{
				Reason:  reasonStorageResizeRejected,
				Message: "One or more volumes cannot be resized (Forbidden)",
			}},
		},
		{
			name:      "the operator still reports a refused resize that was reverted",
			cluster:   readyCluster(clusterCondition(upstreamv1beta1.PersistentVolumeResizing, metav1.ConditionFalse, "Forbidden", "One or more volumes cannot be resized")),
			objs:      append(rolledOut(), dataVolume("a1b2", "10Gi"), dataVolume("c3d4", "10Gi")),
			converged: true,
			available: controller.Available,
		},
		{
			name: "a new engine image rolling out",
			cluster: readyCluster(clusterState(pgv2.AppStateInit), func(cluster *pgv2.PerconaPGCluster) {
				cluster.Status.Postgres.Ready = 2
			}),
			objs: append(without(rolledOut(), firstSet, firstPod),
				instanceSet("a1b2", func(set *appsv1.StatefulSet) {
					set.Status.UpdatedReplicas = 0
					set.Status.ReadyReplicas = 0
				}),
				instancePod("a1b2", statusTestOldImage)),
			message:    "Waiting for the PostgreSQL instances (2 of 3 ready)",
			available:  controller.Available,
			operations: []string{"Upgrading/RollingOutImage"},
		},
		{
			name: "a new engine image the operator has not seen yet",
			cluster: readyCluster(func(cluster *pgv2.PerconaPGCluster) {
				cluster.Generation = 4
			}),
			objs: append(without(rolledOut(), firstPod, statusTestName+"-instance1-c3d4-0", statusTestName+"-instance1-e5f6-0"),
				instancePod("a1b2", statusTestOldImage), instancePod("c3d4", statusTestOldImage), instancePod("e5f6", statusTestOldImage)),
			message:    "Waiting for the operator to apply the latest configuration",
			available:  controller.Available,
			operations: []string{"Upgrading/RollingOutImage"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			objs := tt.objs
			if objs == nil {
				objs = rolledOut()
			}
			if tt.cluster != nil {
				objs = append(objs, tt.cluster)
			}

			got, err := observeStatus(statusContext(t, statusInstanceSpec(tt.spec...), tt.funcs, objs...))
			require.NoError(t, err)

			assert.Equal(t, tt.converged, got.Converged, "converged; message: %s", got.Message)
			if tt.message == "" {
				assert.Empty(t, got.Message)
			} else {
				assert.Contains(t, got.Message, tt.message)
			}

			assert.Equal(t, tt.available, got.Available, "available: %s", got.AvailableMessage)
			assert.Equal(t, tt.availableReason, got.AvailableReason)
			if tt.availableMessage == "" && tt.available == controller.Available {
				assert.Empty(t, got.AvailableMessage)
			} else {
				assert.Contains(t, got.AvailableMessage, tt.availableMessage)
			}

			require.Len(t, got.Problems, len(tt.problems), "problems: %v", got.Problems)
			for i, want := range tt.problems {
				assert.Equal(t, want.Reason, got.Problems[i].Reason)
				assert.Equal(t, want.Terminal, got.Problems[i].Terminal, "terminal")
				assert.Contains(t, got.Problems[i].Message, want.Message)
			}

			var operations []string
			for _, op := range got.Operations {
				operations = append(operations, op.Type+"/"+op.Reason)
				assert.NotEmpty(t, op.Message)
			}
			assert.Equal(t, tt.operations, operations)

			// Connection details are reported while clients can connect,
			// converged or not, and never for a database that is down.
			if (tt.converged || tt.available == controller.Available) && !tt.noDetails {
				assert.Equal(t, "postgresql", got.ConnectionDetails.Type)
				assert.Equal(t, "postgres", got.ConnectionDetails.Username)
				assert.NotEmpty(t, got.ConnectionDetails.Host)
			} else {
				assert.True(t, got.ConnectionDetails.IsEmpty(), "connection details: %+v", got.ConnectionDetails)
			}
		})
	}
}

// TestStatusConnectionDetails covers where clients are told to connect: through
// PGBouncer when it runs, with the sslmode PGBouncer and the TLS mode need,
// and at the load balancer's address when PGBouncer is exposed through one.
func TestStatusConnectionDetails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		spec     []func(*corev1alpha1.InstanceSpec)
		cluster  *pgv2.PerconaPGCluster
		wantHost string
		wantURI  string
	}{
		{
			name:     "through PGBouncer",
			cluster:  readyCluster(),
			wantHost: statusTestHost,
			wantURI:  "postgresql://postgres:secret@" + statusTestHost + ":5432/postgres?sslmode=require",
		},
		{
			name: "directly without PGBouncer",
			spec: []func(*corev1alpha1.InstanceSpec){func(spec *corev1alpha1.InstanceSpec) {
				spec.Components[common.ComponentProxy] = corev1alpha1.ComponentSpec{Replicas: new(int32(0))}
			}},
			cluster: readyCluster(func(cluster *pgv2.PerconaPGCluster) {
				cluster.Spec.Proxy.PGBouncer.Replicas = new(int32(0))
				cluster.Status.PGBouncer = pgv2.PGBouncerStatus{}
			}),
			wantHost: "my-pg-primary.db.svc",
			wantURI:  "postgresql://postgres:secret@my-pg-primary.db.svc:5432/postgres?sslmode=prefer",
		},
		{
			name: "through a PGBouncer load balancer",
			cluster: readyCluster(func(cluster *pgv2.PerconaPGCluster) {
				cluster.Spec.Proxy.PGBouncer.ServiceExpose = &pgv2.ServiceExpose{Type: string(corev1.ServiceTypeLoadBalancer)}
				cluster.Status.Host = "203.0.113.7"
			}),
			wantHost: "203.0.113.7",
			wantURI:  "postgresql://postgres:secret@203.0.113.7:5432/postgres?sslmode=require",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			objs := append(rolledOut(), tt.cluster)
			got, err := observeStatus(statusContext(t, statusInstanceSpec(tt.spec...), nil, objs...))
			require.NoError(t, err)
			require.True(t, got.Converged, "message: %s", got.Message)

			assert.Equal(t, tt.wantHost, got.ConnectionDetails.Host)
			assert.Equal(t, "5432", got.ConnectionDetails.Port)
			assert.Equal(t, "postgres", got.ConnectionDetails.Username)
			assert.Equal(t, "secret", got.ConnectionDetails.Password)
			assert.Equal(t, tt.wantURI, got.ConnectionDetails.URI)
		})
	}
}

func TestRolloutWait(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		sets        []*appsv1.StatefulSet
		deployments []*appsv1.Deployment
		want        string
	}{
		{name: "no workloads"},
		{
			name:        "every pod runs the latest template",
			sets:        []*appsv1.StatefulSet{instanceSet("a1b2"), instanceSet("c3d4")},
			deployments: []*appsv1.Deployment{pgBouncerDeployment()},
		},
		{
			name: "pods on the previous template",
			sets: []*appsv1.StatefulSet{instanceSet("a1b2", func(set *appsv1.StatefulSet) {
				set.Status.UpdatedReplicas = 0
			})},
			want: "StatefulSet my-pg-instance1-a1b2 to roll out (0 of 1 pods updated)",
		},
		{
			name: "a pod not ready",
			sets: []*appsv1.StatefulSet{instanceSet("a1b2", func(set *appsv1.StatefulSet) {
				set.Status.ReadyReplicas = 0
			})},
			want: "StatefulSet my-pg-instance1-a1b2 to become ready (0 of 1 pods ready)",
		},
		{
			name: "the first one by name is reported",
			sets: []*appsv1.StatefulSet{
				instanceSet("c3d4", func(set *appsv1.StatefulSet) { set.Status.ReadyReplicas = 0 }),
				instanceSet("a1b2", func(set *appsv1.StatefulSet) { set.Status.UpdatedReplicas = 0 }),
			},
			want: "StatefulSet my-pg-instance1-a1b2 to roll out",
		},
		{
			name: "one being removed",
			sets: []*appsv1.StatefulSet{instanceSet("a1b2", func(set *appsv1.StatefulSet) {
				set.DeletionTimestamp = new(metav1.Now())
				set.Status.ReadyReplicas = 0
			})},
		},
		{
			name: "PGBouncer has not seen its new pod template",
			deployments: []*appsv1.Deployment{pgBouncerDeployment(func(d *appsv1.Deployment) {
				d.Generation = 5
			})},
			want: "Deployment my-pg-pgbouncer to pick up its new pod template",
		},
		{
			name: "PGBouncer pods of the previous template left",
			deployments: []*appsv1.Deployment{pgBouncerDeployment(func(d *appsv1.Deployment) {
				d.Status.Replicas = 2
			})},
			want: "Deployment my-pg-pgbouncer to scale to 1 pods (it has 2)",
		},
		{
			name: "PGBouncer pod not ready",
			deployments: []*appsv1.Deployment{pgBouncerDeployment(func(d *appsv1.Deployment) {
				d.Status.ReadyReplicas = 0
			})},
			want: "Deployment my-pg-pgbouncer to become ready (0 of 1 pods ready)",
		},
		{
			name: "StatefulSets come first",
			sets: []*appsv1.StatefulSet{instanceSet("a1b2", func(set *appsv1.StatefulSet) { set.Status.ReadyReplicas = 0 })},
			deployments: []*appsv1.Deployment{pgBouncerDeployment(func(d *appsv1.Deployment) {
				d.Status.ReadyReplicas = 0
			})},
			want: "StatefulSet my-pg-instance1-a1b2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var sets []appsv1.StatefulSet
			for _, set := range tt.sets {
				sets = append(sets, *set)
			}
			var deployments []appsv1.Deployment
			for _, d := range tt.deployments {
				deployments = append(deployments, *d)
			}
			got := rolloutWait(sets, deployments)
			if tt.want == "" {
				assert.Empty(t, got)
				return
			}
			assert.Contains(t, got, tt.want)
		})
	}
}

func TestEnqueueClusterInstance(t *testing.T) {
	t.Parallel()

	request := func(name string) []reconcile.Request {
		return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: statusTestNamespace, Name: name}}}
	}
	otherSlice := leaderSlice(true)
	otherSlice.Labels[discoveryv1.LabelServiceName] = statusTestName + "-primary"
	unlabelled := instanceSet("a1b2")
	unlabelled.Labels = nil

	tests := []struct {
		name string
		obj  client.Object
		want []reconcile.Request
	}{
		{name: "a StatefulSet of a cluster", obj: instanceSet("a1b2"), want: request(statusTestName)},
		{name: "the PGBouncer Deployment of a cluster", obj: pgBouncerDeployment(), want: request(statusTestName)},
		{name: "a StatefulSet of something else", obj: unlabelled},
		{name: "an EndpointSlice of the Patroni leader Service", obj: leaderSlice(true), want: request(statusTestName)},
		{name: "an EndpointSlice of another Service of the cluster", obj: otherSlice},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, enqueueClusterInstance()(context.Background(), tt.obj))
		})
	}
}

// TestStatusDesiredReplicas covers how many pods the provider reports each
// component will have: read from the Instance as Sync applies it, whatever the
// PerconaPGCluster object runs for now.
func TestStatusDesiredReplicas(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		spec    []func(*corev1alpha1.InstanceSpec)
		cluster *pgv2.PerconaPGCluster
		want    []controller.ComponentReplicas
	}{
		{
			name:    "three PostgreSQL instances and one PGBouncer pod",
			cluster: readyCluster(),
			want: []controller.ComponentReplicas{
				{Component: common.ComponentEngine, Desired: 3},
				{Component: common.ComponentProxy, Desired: 1},
			},
		},
		{
			name: "scaling from 3 to 5 instances",
			spec: []func(*corev1alpha1.InstanceSpec){func(spec *corev1alpha1.InstanceSpec) {
				spec.Components[common.ComponentEngine] = corev1alpha1.ComponentSpec{Replicas: new(int32(5))}
			}},
			cluster: readyCluster(),
			want: []controller.ComponentReplicas{
				{Component: common.ComponentEngine, Desired: 5},
				{Component: common.ComponentProxy, Desired: 1},
			},
		},
		{
			name: "without PGBouncer",
			spec: []func(*corev1alpha1.InstanceSpec){func(spec *corev1alpha1.InstanceSpec) {
				spec.Components[common.ComponentProxy] = corev1alpha1.ComponentSpec{Replicas: new(int32(0))}
			}},
			cluster: readyCluster(),
			want: []controller.ComponentReplicas{
				{Component: common.ComponentEngine, Desired: 3},
				{Component: common.ComponentProxy, Desired: 0},
			},
		},
		{
			name: "stopped on the PerconaPGCluster object",
			cluster: readyCluster(clusterState(pgv2.AppStatePaused), func(cluster *pgv2.PerconaPGCluster) {
				cluster.Spec.Pause = new(true)
			}),
			want: []controller.ComponentReplicas{
				{Component: common.ComponentEngine, Desired: 3},
				{Component: common.ComponentProxy, Desired: 1},
			},
		},
		{
			name: "PerconaPGCluster object not created yet",
			want: []controller.ComponentReplicas{
				{Component: common.ComponentEngine, Desired: 3},
				{Component: common.ComponentProxy, Desired: 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			objs := rolledOut()
			if tt.cluster != nil {
				objs = append(objs, tt.cluster)
			}
			got, err := observeStatus(statusContext(t, statusInstanceSpec(tt.spec...), nil, objs...))
			require.NoError(t, err)
			assert.Equal(t, tt.want, got.DesiredReplicas)
		})
	}
}
