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
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	"github.com/openeverest/provider-percona-postgresql/internal/common"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	upstreamv1beta1 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/upstream.pgv2.percona.com/v1beta1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// Reasons of the problems the provider reports. A problem's reason becomes
// the status word users read, so each names one cause.
const (
	// reasonPausedOutsideOpenEverest: spec.pause is set on the PerconaPGCluster
	// object. An Instance cannot ask for that, so it was set there by hand.
	reasonPausedOutsideOpenEverest = "PausedOutsideOpenEverest"
	// reasonUnmanagedOutsideOpenEverest: spec.unmanaged is set on the
	// PerconaPGCluster object, so the operator applies no change to the
	// cluster. An Instance cannot ask for that either.
	reasonUnmanagedOutsideOpenEverest = "UnmanagedOutsideOpenEverest"
	// reasonStorageResizeRejected: the API server refused the operator's
	// resize of the volumes.
	reasonStorageResizeRejected = "StorageResizeRejected"
	// reasonRestoreFailed: the pgBackRest job that restores the data failed.
	reasonRestoreFailed = "RestoreFailed"
)

// Reasons explaining why clients cannot connect, or why that is unknown.
const (
	reasonPaused                   = "Paused"
	reasonRestoring                = corev1alpha1.ConditionRestoring
	reasonNoPrimary                = "NoPrimary"
	reasonPrimaryServiceUnreadable = "PrimaryServiceUnreadable"
	reasonNoPGBouncerReady         = "NoPGBouncerReady"
	reasonLoadBalancerPending      = "LoadBalancerPending"
)

// Reasons of the operations the provider reports.
const (
	reasonRollingOutImage = "RollingOutImage"
	reasonResizingVolumes = "ResizingVolumes"
)

// Labels, condition and container names of the upstream operator the Percona
// operator embeds, which it does not export. labelCluster and
// labelInstanceSet are in scheduling.go.
const (
	labelData = "postgres-operator.crunchydata.com/data"
	labelRole = "postgres-operator.crunchydata.com/role"

	dataPostgres  = "postgres"
	rolePGBouncer = "pgbouncer"
	rolePGData    = "pgdata"

	containerDatabase = "database"

	conditionPostgresDataInitialized = "PostgresDataInitialized"
	reasonPGBackRestRestoreFailed    = "PGBackRestRestoreFailed"
)

// observation is what Status reads about the cluster.
type observation struct {
	cluster *pgv2.PerconaPGCluster
	// restore names the Restore that replaces the data of the running
	// cluster, or is "" when there is none.
	restore string
	// sets are the cluster's StatefulSets, deployments its PGBouncer
	// Deployment.
	sets        []appsv1.StatefulSet
	deployments []appsv1.Deployment
	// primary is true when the Patroni leader Service routes to a ready
	// endpoint. primaryErr is why that could not be read.
	primary    bool
	primaryErr error
	storage    storageState
}

// storageState is what the cluster's volumes say about a resize the operator
// reports.
type storageState struct {
	// resizing is true while a volume is being resized.
	resizing bool
	// rejected says why the operator cannot resize the volumes, while that
	// still stands.
	rejected string
}

// observeStatus reports what the provider observes about the cluster: whether
// it runs what the Instance asks for, whether clients can connect, what is
// wrong and which long operation is running, and how many pods each
// component will have. The runtime derives the Instance's phase and
// conditions from these facts.
func observeStatus(c *controller.Context) (controller.Status, error) {
	cluster, err := uncachedCluster(c)
	if apierrors.IsNotFound(err) {
		return withDesiredReplicas(c, controller.Progressing("Waiting for the PerconaPGCluster object to be created")), nil
	}
	if err != nil {
		return controller.Status{}, err
	}

	o := observation{cluster: cluster}
	if o.restore, err = inPlaceRestore(c); err != nil {
		return controller.Status{}, err
	}

	sets := &appsv1.StatefulSetList{}
	if err := c.List(sets, client.MatchingLabels{labelCluster: cluster.Name}); err != nil {
		return controller.Status{}, fmt.Errorf("list StatefulSets of cluster %q: %w", cluster.Name, err)
	}
	o.sets = sets.Items

	deployments := &appsv1.DeploymentList{}
	if err := c.List(deployments, client.MatchingLabels{labelCluster: cluster.Name, labelRole: rolePGBouncer}); err != nil {
		return controller.Status{}, fmt.Errorf("list PGBouncer Deployments of cluster %q: %w", cluster.Name, err)
	}
	o.deployments = deployments.Items

	o.primary, o.primaryErr = primaryReady(c, cluster.Name)

	if o.storage, err = observeStorage(c, cluster); err != nil {
		return controller.Status{}, err
	}

	status := observe(o)
	if !status.Converged {
		pods, err := listUncached[corev1.Pod](c, "PodList",
			client.MatchingLabels{labelCluster: cluster.Name, labelData: dataPostgres})
		if err != nil {
			return controller.Status{}, fmt.Errorf("list PostgreSQL pods of cluster %q: %w", cluster.Name, err)
		}
		status = withUpgrade(status, cluster, pods)
	}
	status = withDesiredReplicas(c, status)

	// The connection details are reported as soon as clients can connect,
	// not only once the cluster has converged: whoever is told the database
	// is available must find where to connect, and the details must follow a
	// change that is still rolling out.
	if !status.Converged && status.Available != controller.Available {
		return status, nil
	}
	details, err := connectionDetails(c, cluster)
	if err != nil {
		return controller.Status{}, err
	}
	status.ConnectionDetails = details
	return status, nil
}

// uncachedCluster reads the PerconaPGCluster object from the API server
// instead of the cache. Sync has just applied it, and the cache may still
// hold the object from before that: its status would then look caught up with
// a generation that is no longer the latest, and the Instance would read as
// ready for a change the operator has not seen. The client reads unstructured
// objects without the cache.
func uncachedCluster(c *controller.Context) (*pgv2.PerconaPGCluster, error) {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(pgv2.GroupVersion.WithKind("PerconaPGCluster"))
	if err := c.Get(obj, c.Name()); err != nil {
		return nil, fmt.Errorf("get PerconaPGCluster %q: %w", c.Name(), err)
	}

	raw, err := obj.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("encode PerconaPGCluster %q: %w", c.Name(), err)
	}
	cluster := &pgv2.PerconaPGCluster{}
	if err := json.Unmarshal(raw, cluster); err != nil {
		return nil, fmt.Errorf("decode PerconaPGCluster %q: %w", c.Name(), err)
	}
	return cluster, nil
}

// listUncached lists objects of a core kind from the API server, which the
// client does for unstructured objects. Status reads pods and volumes this way
// only when it needs them, rather than have every one in the Kubernetes
// cluster cached.
func listUncached[T any](c *controller.Context, kind string, opts ...client.ListOption) ([]T, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind(kind))
	if err := c.List(list, opts...); err != nil {
		return nil, err
	}
	out := make([]T, len(list.Items))
	for i := range list.Items {
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(list.Items[i].Object, &out[i]); err != nil {
			return nil, fmt.Errorf("decode %s: %w", kind, err)
		}
	}
	return out, nil
}

// inPlaceRestore returns the name of a Restore that replaces the data of the
// running cluster, or "" when there is none. A restore drives the cluster
// through intermediate and momentarily ready states, so the Restore reaching
// a terminal state is what ends it, not what the operator reports meanwhile.
// The Restore that seeds a new Instance from its data source is left out: the
// runtime reports the seeding itself.
func inPlaceRestore(c *controller.Context) (string, error) {
	restores, err := c.RestoresForInstance()
	if err != nil {
		return "", fmt.Errorf("list restores for instance %q: %w", c.Name(), err)
	}

	var names []string
	for i := range restores {
		r := &restores[i]
		if !r.DeletionTimestamp.IsZero() || isSeedingRestore(c, r) || !restoreRunning(r.Status.State) {
			continue
		}
		names = append(names, r.Name)
	}
	if len(names) == 0 {
		return "", nil
	}
	return slices.Min(names), nil
}

// leaderServiceSuffix ends the name of the Service Patroni points at the
// primary. The operator's primary Service, which clients connect to, resolves
// to it.
const leaderServiceSuffix = "-ha"

// leaderService names the Patroni leader Service of a cluster.
func leaderService(cluster string) string {
	return cluster + leaderServiceSuffix
}

// primaryReady reports whether a PostgreSQL primary serves connections: the
// Patroni leader Service routes to a ready endpoint.
//
// The operator's primary Service has no selector and a single endpoint it
// writes itself, the address of the leader Service, so it always looks ready.
// The leader Service has no selector either: Patroni points its endpoint at
// the pod it elected primary, and removes it when there is none. Kubernetes
// routes the Service's traffic from the EndpointSlices it mirrors from that.
func primaryReady(c *controller.Context, cluster string) (bool, error) {
	list := &discoveryv1.EndpointSliceList{}
	if err := c.List(list, client.MatchingLabels{discoveryv1.LabelServiceName: leaderService(cluster)}); err != nil {
		return false, fmt.Errorf("list EndpointSlices of Service %s: %w", leaderService(cluster), err)
	}
	for _, slice := range list.Items {
		for _, endpoint := range slice.Endpoints {
			// An endpoint without a ready condition counts as ready.
			if len(endpoint.Addresses) > 0 && (endpoint.Conditions.Ready == nil || *endpoint.Conditions.Ready) {
				return true, nil
			}
		}
	}
	return false, nil
}

// observeStorage says whether the cluster's volumes are being resized, or
// cannot be.
//
// The operator copies the PersistentVolumeResizing condition of its upstream
// controller into its status but never removes it, so the condition outlives
// the resize it reports. The volumes say whether one is still under way, and
// whether a refusal still stands.
func observeStorage(c *controller.Context, cluster *pgv2.PerconaPGCluster) (storageState, error) {
	cond := meta.FindStatusCondition(cluster.Status.Conditions, upstreamv1beta1.PersistentVolumeResizing)
	if cond == nil {
		return storageState{}, nil
	}

	pvcs, err := listUncached[corev1.PersistentVolumeClaim](c, "PersistentVolumeClaimList",
		client.MatchingLabels{labelCluster: cluster.Name})
	if err != nil {
		return storageState{}, fmt.Errorf("list volumes of cluster %q: %w", cluster.Name, err)
	}

	for i := range pvcs {
		for _, pc := range pvcs[i].Status.Conditions {
			if (pc.Type == corev1.PersistentVolumeClaimResizing || pc.Type == corev1.PersistentVolumeClaimFileSystemResizePending) &&
				pc.Status == corev1.ConditionTrue {
				return storageState{resizing: true}, nil
			}
		}
	}

	// The operator reports a refusal when the API server rejects its change to
	// a volume, which then keeps the size it had.
	if cond.Status != metav1.ConditionFalse || !dataVolumesDiffer(cluster, pvcs) {
		return storageState{}, nil
	}
	message := cmp.Or(cond.Message, "The volumes cannot be resized")
	if cond.Reason != "" {
		message += " (" + cond.Reason + ")"
	}
	return storageState{rejected: message + ". Check that the storage class allows volume expansion and that the size does not shrink."}, nil
}

// dataVolumesDiffer reports whether a PostgreSQL data volume requests another
// size than its instance set asks for.
func dataVolumesDiffer(cluster *pgv2.PerconaPGCluster, pvcs []corev1.PersistentVolumeClaim) bool {
	for i := range pvcs {
		pvc := &pvcs[i]
		if pvc.Labels[labelRole] != rolePGData {
			continue
		}
		for _, set := range cluster.Spec.InstanceSets {
			if set.Name != pvc.Labels[labelInstanceSet] {
				continue
			}
			want, ok := set.DataVolumeClaimSpec.Resources.Requests[corev1.ResourceStorage]
			if !ok {
				continue
			}
			if have := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; have.Cmp(want) != 0 {
				return true
			}
		}
	}
	return false
}

// observe turns what Status read into the facts the provider reports.
func observe(o observation) controller.Status {
	var status controller.Status
	status.Available, status.AvailableReason, status.AvailableMessage = availability(o)

	cluster := o.cluster
	// The provider never sets spec.pause nor spec.unmanaged and does not own
	// them, so its Sync leaves them alone: only whoever set them can unset
	// them.
	if isSet(cluster.Spec.Pause) {
		status = status.WithTerminalProblem(reasonPausedOutsideOpenEverest,
			"The cluster is stopped: spec.pause is set on its PerconaPGCluster object, outside OpenEverest. Unset it to start the database again.")
	}
	if isSet(cluster.Spec.Unmanaged) {
		status = status.WithTerminalProblem(reasonUnmanagedOutsideOpenEverest,
			"The operator applies no change to the cluster: spec.unmanaged is set on its PerconaPGCluster object, outside OpenEverest. Unset it to have the Instance applied again.")
	}
	if o.storage.rejected != "" {
		status = status.WithProblem(reasonStorageResizeRejected, o.storage.rejected)
	}
	// A restore that is running settles the outcome of an earlier one.
	if message, failed := restoreFailed(cluster); failed && o.restore == "" {
		status = status.WithProblem(reasonRestoreFailed, message)
	}

	if o.restore != "" {
		status = status.WithOperation(corev1alpha1.ConditionRestoring, corev1alpha1.ConditionRestoring,
			fmt.Sprintf("Restore %s is running", o.restore))
	}
	if o.storage.resizing {
		status = status.WithOperation(corev1alpha1.ConditionStorageResizing, reasonResizingVolumes,
			"The volumes are being resized")
	}

	status.Message = waitingFor(o)
	status.Converged = status.Message == ""
	return status
}

// restoreFailed returns what the operator says when its pgBackRest job failed
// to restore the data, and whether it did. The operator keeps saying so until
// a later restore succeeds.
func restoreFailed(cluster *pgv2.PerconaPGCluster) (string, bool) {
	cond := meta.FindStatusCondition(cluster.Status.Conditions, conditionPostgresDataInitialized)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != reasonPGBackRestRestoreFailed {
		return "", false
	}
	message := cmp.Or(cond.Message, "pgBackRest restore failed")
	return fmt.Sprintf("The operator reports: %s. The logs of Job %s-pgbackrest-restore say why.", message, cluster.Name), true
}

// waitingFor says what keeps the cluster from running what the Instance asks
// for, in the operator's own words where it has any. It returns "" when
// nothing does: the operator reports ready for the latest generation, every
// StatefulSet and the PGBouncer Deployment have rolled out, and a primary
// serves connections.
func waitingFor(o observation) string {
	cluster := o.cluster
	switch {
	case isSet(cluster.Spec.Pause):
		return "The cluster is stopped on its PerconaPGCluster object"
	case isSet(cluster.Spec.Unmanaged):
		return "The operator applies no change to the cluster while spec.unmanaged is set on its PerconaPGCluster object"
	case o.restore != "":
		return fmt.Sprintf("Restore %s is running", o.restore)
	case cluster.Status.ObservedGeneration == 0:
		return "Waiting for the operator to start creating the cluster"
	case cluster.Status.ObservedGeneration < cluster.Generation:
		return "Waiting for the operator to apply the latest configuration"
	case o.storage.resizing:
		return "Waiting for the volumes to be resized"
	case o.storage.rejected != "":
		return "Waiting for the volumes to be resized, which the operator cannot do"
	case cluster.Status.State != pgv2.AppStateReady:
		return notReadyWait(o)
	}

	// The operator decides that nothing is rolling from the workloads'
	// status. Right after a pod template changes that status still describes
	// the previous one, so it can report ready for the latest generation
	// before the rolling restart has started.
	if wait := rolloutWait(o.sets, o.deployments); wait != "" {
		return "Waiting for " + wait
	}
	// The operator counts pods that run PostgreSQL as ready, replicas
	// included, so it can report ready while no primary takes writes.
	if !o.primary && o.primaryErr == nil {
		return "Waiting for Patroni to elect a primary"
	}
	if loadBalancerPending(cluster) {
		return "Waiting for the PGBouncer load balancer address"
	}
	return ""
}

// notReadyWait says what a cluster the operator does not report ready is
// waiting for, from what the operator publishes about PostgreSQL, PGBouncer
// and the pgBackRest repository host.
func notReadyWait(o observation) string {
	st := o.cluster.Status

	var waits []string
	switch {
	case st.Postgres.Size == 0:
		waits = append(waits, "the PostgreSQL instances to be created")
	case st.Postgres.Ready < st.Postgres.Size:
		waits = append(waits, fmt.Sprintf("the PostgreSQL instances (%d of %d ready)", st.Postgres.Ready, st.Postgres.Size))
	}
	if pgBouncerEnabled(o.cluster) {
		switch {
		case st.PGBouncer.Size == 0:
			waits = append(waits, "PGBouncer to be created")
		case st.PGBouncer.Ready < st.PGBouncer.Size:
			waits = append(waits, fmt.Sprintf("PGBouncer (%d of %d ready)", st.PGBouncer.Ready, st.PGBouncer.Size))
		}
	}
	if st.PGBackRest != nil && st.PGBackRest.RepoHost != nil && !st.PGBackRest.RepoHost.Ready {
		waits = append(waits, "the pgBackRest repository host")
	}
	if len(waits) > 0 {
		return "Waiting for " + strings.Join(waits, "; ")
	}

	if wait := rolloutWait(o.sets, o.deployments); wait != "" {
		return "Waiting for " + wait
	}
	return fmt.Sprintf("Waiting for the operator to report the cluster ready (it reports %s)", cmp.Or(string(st.State), "no state"))
}

// rolloutWait returns what the first workload that has not finished rolling
// out is waiting for, or "" when all have.
//
// The operator replaces the PostgreSQL pods itself, replicas first, so their
// StatefulSets use the OnDelete strategy. Their currentRevision then never
// moves, and a rollout is complete when the controller has seen the latest pod
// template and every pod runs it.
func rolloutWait(sets []appsv1.StatefulSet, deployments []appsv1.Deployment) string {
	sets = slices.Clone(sets)
	slices.SortFunc(sets, func(a, b appsv1.StatefulSet) int { return strings.Compare(a.Name, b.Name) })
	for i := range sets {
		set := &sets[i]
		if !set.DeletionTimestamp.IsZero() {
			continue
		}
		want := int32(1)
		if set.Spec.Replicas != nil {
			want = *set.Spec.Replicas
		}
		st := set.Status
		switch {
		case st.ObservedGeneration < set.Generation:
			return fmt.Sprintf("StatefulSet %s to pick up its new pod template", set.Name)
		case st.Replicas != want:
			return fmt.Sprintf("StatefulSet %s to scale to %d pods (it has %d)", set.Name, want, st.Replicas)
		case st.UpdatedReplicas < st.Replicas:
			return fmt.Sprintf("StatefulSet %s to roll out (%d of %d pods updated)", set.Name, st.UpdatedReplicas, st.Replicas)
		case st.ReadyReplicas < st.Replicas:
			return fmt.Sprintf("StatefulSet %s to become ready (%d of %d pods ready)", set.Name, st.ReadyReplicas, st.Replicas)
		}
	}

	deployments = slices.Clone(deployments)
	slices.SortFunc(deployments, func(a, b appsv1.Deployment) int { return strings.Compare(a.Name, b.Name) })
	for i := range deployments {
		d := &deployments[i]
		if !d.DeletionTimestamp.IsZero() {
			continue
		}
		want := int32(1)
		if d.Spec.Replicas != nil {
			want = *d.Spec.Replicas
		}
		st := d.Status
		switch {
		case st.ObservedGeneration < d.Generation:
			return fmt.Sprintf("Deployment %s to pick up its new pod template", d.Name)
		case st.UpdatedReplicas < want:
			return fmt.Sprintf("Deployment %s to roll out (%d of %d pods updated)", d.Name, st.UpdatedReplicas, want)
		case st.Replicas != want:
			return fmt.Sprintf("Deployment %s to scale to %d pods (it has %d)", d.Name, want, st.Replicas)
		case st.ReadyReplicas < want:
			return fmt.Sprintf("Deployment %s to become ready (%d of %d pods ready)", d.Name, st.ReadyReplicas, want)
		}
	}
	return ""
}

// availability says whether clients can connect: a PostgreSQL primary serves
// connections and, when clients connect through PGBouncer, a PGBouncer pod is
// ready to pass them on. Whether the primary serves connections is read from
// where Kubernetes routes them, never guessed from pod counts. The PGBouncer
// pods are counted as the operator last recorded them.
func availability(o observation) (controller.Availability, string, string) {
	cluster := o.cluster
	switch {
	case isSet(cluster.Spec.Pause), cluster.Status.State == pgv2.AppStatePaused, cluster.Status.State == pgv2.AppStateStopping:
		return controller.Unavailable, reasonPaused, "The cluster is stopped"
	case o.restore != "":
		return controller.Unavailable, reasonRestoring, "A restore is replacing the data"
	case o.primaryErr != nil:
		return controller.AvailabilityUnknown, reasonPrimaryServiceUnreadable,
			"Cannot tell whether a primary serves connections: " + o.primaryErr.Error()
	case !o.primary:
		return controller.Unavailable, reasonNoPrimary, "No PostgreSQL primary serves connections"
	case pgBouncerEnabled(cluster) && cluster.Status.PGBouncer.Ready < 1:
		return controller.Unavailable, reasonNoPGBouncerReady, "No PGBouncer pod is ready to pass connections on"
	case loadBalancerPending(cluster):
		return controller.Unavailable, reasonLoadBalancerPending, "The PGBouncer load balancer has no address yet"
	}
	return controller.Available, "", ""
}

// loadBalancerPending reports whether clients connect through a PGBouncer
// load balancer the operator has no address for yet.
func loadBalancerPending(cluster *pgv2.PerconaPGCluster) bool {
	return pgBouncerEnabled(cluster) && pgBouncerLoadBalancerExposed(cluster) && cluster.Status.Host == ""
}

// withUpgrade adds the Upgrading operation while PostgreSQL pods run another
// image than the PerconaPGCluster asks for: a new engine version is rolling
// out, or about to.
func withUpgrade(status controller.Status, cluster *pgv2.PerconaPGCluster, pods []corev1.Pod) controller.Status {
	want := cluster.Spec.Image
	if want == "" {
		return status
	}

	var total int
	var previous []string
	for i := range pods {
		for _, container := range pods[i].Spec.Containers {
			if container.Name != containerDatabase {
				continue
			}
			total++
			if container.Image != want {
				previous = append(previous, container.Image)
			}
		}
	}
	if len(previous) == 0 {
		return status
	}
	slices.Sort(previous)
	return status.WithOperation(corev1alpha1.ConditionUpgrading, reasonRollingOutImage,
		fmt.Sprintf("Rolling out image %s (%d of %d PostgreSQL pods still run %s)", want, len(previous), total, previous[0]))
}

// componentReplicas returns how many pods the Instance asks for a component,
// and false when it does not say. Validate requires both components to say,
// so there is no default to apply.
func componentReplicas(in *corev1alpha1.Instance, name string) (int32, bool) {
	component, ok := in.Spec.Components[name]
	if !ok || component.Replicas == nil {
		return 0, false
	}
	return *component.Replicas, true
}

// withDesiredReplicas reports how many pods each component will have once the
// cluster runs what the Instance asks for. The counts come from the Instance,
// as Sync applies them, not from the PerconaPGCluster object, which may still
// describe an earlier spec. A cluster stopped on that object keeps them: the
// Instance does not ask for the stop. Every component runs pods of its own:
// the PostgreSQL instances and PGBouncer. The provider's pgBackRest
// repositories are all in object storage, so the operator runs no repository
// host.
func withDesiredReplicas(c *controller.Context, status controller.Status) controller.Status {
	for _, name := range []string{common.ComponentEngine, common.ComponentProxy} {
		if replicas, ok := componentReplicas(c.Instance(), name); ok {
			status = status.WithDesiredReplicas(name, replicas)
		}
	}
	return status
}

// connectionDetails reads the user Secret the operator writes and combines it
// with what the cluster publishes.
func connectionDetails(c *controller.Context, cluster *pgv2.PerconaPGCluster) (controller.ConnectionDetails, error) {
	port := int32(5432)
	if cluster.Spec.Port != nil {
		port = *cluster.Spec.Port
	}

	// Read the user secret created by the PG operator to obtain credentials.
	// The secret follows the naming convention: <cluster-name>-pguser-<username>.
	// The PG operator populates this secret with all connection details including
	// properly URL-encoded URIs for both direct and PGBouncer connections.
	// The PGBouncer keys are present even when PGBouncer has no replicas, so
	// they are only used when PGBouncer is enabled.
	pgBouncerOn := pgBouncerEnabled(cluster)
	ca, sslMode, err := tlsConnection(c, cluster)
	if err != nil {
		return controller.ConnectionDetails{}, err
	}
	var username, password, uri string
	userSecret := &corev1.Secret{}
	secretName := c.Name() + "-pguser-" + defaultUsername
	if err := c.Get(userSecret, secretName); err != nil {
		if !apierrors.IsNotFound(err) {
			return controller.ConnectionDetails{}, fmt.Errorf("get user secret %q: %w", secretName, err)
		}
		log.FromContext(c.Context()).Info("User secret not found, connection details will not include credentials", "secret", secretName)
	} else {
		username = string(userSecret.Data["user"])
		password = string(userSecret.Data["password"])
		// Prefer pgbouncer-uri when PGBouncer is enabled, fall back to direct uri.
		if v := userSecret.Data["pgbouncer-uri"]; pgBouncerOn && len(v) > 0 {
			uri = string(v)
		} else if v := userSecret.Data["uri"]; len(v) > 0 {
			uri = string(v)
		}
		// Ensure the URI's sslmode matches the instance's TLS mode and how
		// clients reach it. PGBouncer always requires TLS from clients
		// regardless of mode, and rejects plain-text connections.
		uri = ensureSSLMode(uri, sslMode)
	}

	// Use the pgbouncer-host from the secret when PGBouncer is running (it
	// includes the correct service FQDN for PGBouncer), otherwise the direct
	// host from the secret. Fall back to cluster status, which is the PGBouncer
	// host, only when the secret has neither.
	host := cluster.Status.Host
	if v := userSecret.Data["pgbouncer-host"]; pgBouncerOn && len(v) > 0 {
		host = string(v)
	} else if v := userSecret.Data["host"]; len(v) > 0 {
		host = string(v)
	}

	// The secret only ever carries the in-cluster Service name. When PGBouncer
	// is a LoadBalancer the operator publishes the external address in
	// status.host, which stays empty until the load balancer is provisioned.
	if pgBouncerOn && pgBouncerLoadBalancerExposed(cluster) {
		if cluster.Status.Host == "" {
			return controller.ConnectionDetails{}, nil
		}
		host = cluster.Status.Host
		uri = replaceURIHost(uri, host)
	}

	details := controller.ConnectionDetails{
		Type:     "postgresql",
		Provider: common.ProviderName,
		Host:     host,
		Port:     strconv.Itoa(int(port)),
		Username: username,
		Password: password,
		URI:      uri,
	}
	if len(ca) > 0 {
		// Clients verify the instance's certificate against this CA
		// (sslrootcert); the operator's own CA is not publicly trusted.
		details.AdditionalProperties = map[string]string{caKey: string(ca)}
	}
	return details, nil
}

// restoreRunning reports whether a Restore in this state has not ended.
func restoreRunning(state backupv1alpha1.RestoreState) bool {
	return state == "" || state == backupv1alpha1.RestoreStatePending || state == backupv1alpha1.RestoreStateRunning
}

// isSet reports whether an optional flag is set to true.
func isSet(flag *bool) bool {
	return flag != nil && *flag
}

// enqueueClusterInstance maps an object the operator runs for a cluster to
// the Instance of that cluster, which has the cluster's name. Whether the
// Instance has converged depends on the StatefulSets and the PGBouncer
// Deployment having rolled out, and whether it is available on the Patroni
// leader Service's endpoints; the operator's own status may not change again
// when they do. EndpointSlices are mapped by the Service they belong to, and
// count only for the leader Service.
func enqueueClusterInstance() func(ctx context.Context, obj client.Object) []reconcile.Request {
	return func(_ context.Context, obj client.Object) []reconcile.Request {
		labels := obj.GetLabels()
		cluster := labels[labelCluster]
		if _, ok := obj.(*discoveryv1.EndpointSlice); ok {
			service, isLeader := strings.CutSuffix(labels[discoveryv1.LabelServiceName], leaderServiceSuffix)
			if !isLeader {
				return nil
			}
			cluster = service
		}
		if cluster == "" {
			return nil
		}
		return []reconcile.Request{{
			NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: cluster},
		}}
	}
}
