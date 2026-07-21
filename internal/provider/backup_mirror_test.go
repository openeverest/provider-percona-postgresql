package provider

import (
	"context"
	"testing"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	pgv2 "github.com/percona/percona-postgresql-operator/v2/pkg/apis/pgv2.percona.com/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestMirrorScheduledBackupByAnnotations(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))

	instance := &corev1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "inst-pg", Namespace: "my-special-place"},
		Spec: corev1alpha1.InstanceSpec{
			Provider: "provider-percona-postgresql",
			Backup: &corev1alpha1.InstanceBackupSpec{
				ClassRef: corev1alpha1.BackupClassReference{Name: "pg"},
			},
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance).Build()

	repoName := "repo1"
	opBackup := &pgv2.PerconaPGBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cron-inst-pg-repo1-full-20260630141710",
			Namespace: "my-special-place",
			UID:       "11111111-1111-1111-1111-111111111111",
			Annotations: map[string]string{
				pgv2.PGBackrestAnnotationJobType:    "backup",
				pgv2.PGBackrestAnnotationBackupName: "nightly",
			},
		},
		Spec: pgv2.PerconaPGBackupSpec{
			PGCluster: "inst-pg",
			RepoName:  &repoName,
		},
	}

	mirror, err := (&Provider{}).Mirror(context.Background(), k8sClient, opBackup)
	require.NoError(t, err)
	require.NotNil(t, mirror)
	require.Equal(t, "nightly", mirror.Spec.ScheduleName)
	require.Equal(t, "repo1", mirror.Spec.StorageName)
	require.Len(t, mirror.OwnerReferences, 1)
	owner := mirror.OwnerReferences[0]
	require.Equal(t, pgv2.GroupVersion.String(), owner.APIVersion)
	require.Equal(t, "PerconaPGBackup", owner.Kind)
	require.Equal(t, opBackup.Name, owner.Name)
	require.Equal(t, opBackup.UID, owner.UID)
}

func TestMirrorSkipsOnDemandBackup(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))

	instance := &corev1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "inst-pg", Namespace: "my-special-place"},
		Spec: corev1alpha1.InstanceSpec{
			Provider: "provider-percona-postgresql",
			Backup: &corev1alpha1.InstanceBackupSpec{
				ClassRef: corev1alpha1.BackupClassReference{Name: "pg"},
			},
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance).Build()

	repoName := "repo1"
	opBackup := &pgv2.PerconaPGBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "manual-backup", Namespace: "my-special-place"},
		Spec: pgv2.PerconaPGBackupSpec{
			PGCluster: "inst-pg",
			RepoName:  &repoName,
		},
	}

	mirror, err := (&Provider{}).Mirror(context.Background(), k8sClient, opBackup)
	require.NoError(t, err)
	require.Nil(t, mirror)
}

func TestMirrorUsesJobTypeWhenBackupNameMissing(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))

	instance := &corev1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "inst-pg", Namespace: "my-special-place"},
		Spec: corev1alpha1.InstanceSpec{
			Provider: "provider-percona-postgresql",
			Backup:   &corev1alpha1.InstanceBackupSpec{ClassRef: corev1alpha1.BackupClassReference{Name: "pg"}},
		},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance).Build()

	repoName := "repo1"
	opBackup := &pgv2.PerconaPGBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cron-backup",
			Namespace: "my-special-place",
			Annotations: map[string]string{
				pgv2.PGBackrestAnnotationJobType: "backup",
			},
		},
		Spec: pgv2.PerconaPGBackupSpec{
			PGCluster: "inst-pg",
			RepoName:  &repoName,
		},
	}

	mirror, err := (&Provider{}).Mirror(context.Background(), k8sClient, opBackup)
	require.NoError(t, err)
	require.NotNil(t, mirror)
	require.Equal(t, "backup", mirror.Spec.ScheduleName)
}

func TestReconcileRepoSlotMap_NewStorages(t *testing.T) {
	t.Parallel()

	storages := []corev1alpha1.InstanceBackupStorage{
		{Name: "storage-a"},
		{Name: "storage-b"},
		{Name: "storage-c"},
	}

	result := reconcileRepoSlotMap(nil, storages)

	assert.Equal(t, 0, result["storage-a"])
	assert.Equal(t, 1, result["storage-b"])
	assert.Equal(t, 2, result["storage-c"])
}

func TestReconcileRepoSlotMap_RemoveMiddleStorageKeepsSlots(t *testing.T) {
	t.Parallel()

	// Simulate: originally had [a=0, b=1, c=2], now remove b.
	existing := repoSlotMap{"storage-a": 0, "storage-b": 1, "storage-c": 2}
	storages := []corev1alpha1.InstanceBackupStorage{
		{Name: "storage-a"},
		{Name: "storage-c"},
	}

	result := reconcileRepoSlotMap(existing, storages)

	// a and c must keep their original slots; b's slot (1) is now free.
	assert.Equal(t, 0, result["storage-a"])
	assert.Equal(t, 2, result["storage-c"])
	_, hasBSlot := result["storage-b"]
	assert.False(t, hasBSlot)
}

func TestReconcileRepoSlotMap_AddStorageUsesFreedSlot(t *testing.T) {
	t.Parallel()

	// After removing b (slot 1), add d — it should get slot 1 (lowest free).
	existing := repoSlotMap{"storage-a": 0, "storage-c": 2}
	storages := []corev1alpha1.InstanceBackupStorage{
		{Name: "storage-a"},
		{Name: "storage-c"},
		{Name: "storage-d"},
	}

	result := reconcileRepoSlotMap(existing, storages)

	assert.Equal(t, 0, result["storage-a"])
	assert.Equal(t, 2, result["storage-c"])
	assert.Equal(t, 1, result["storage-d"]) // takes freed slot
}

func TestReconcileRepoSlotMap_MaxSlotsRespected(t *testing.T) {
	t.Parallel()

	storages := []corev1alpha1.InstanceBackupStorage{
		{Name: "s1"},
		{Name: "s2"},
		{Name: "s3"},
		{Name: "s4"},
		{Name: "s5"}, // exceeds max — should not get a slot
	}

	result := reconcileRepoSlotMap(nil, storages)

	assert.Equal(t, 0, result["s1"])
	assert.Equal(t, 1, result["s2"])
	assert.Equal(t, 2, result["s3"])
	assert.Equal(t, 3, result["s4"])
	_, has5 := result["s5"]
	assert.False(t, has5)
}

func TestLoadSaveRepoSlotMap(t *testing.T) {
	t.Parallel()

	pgCluster := &pgv2.PerconaPGCluster{}
	m := repoSlotMap{"alpha": 0, "beta": 2}

	saveRepoSlotMap(pgCluster, m)
	loaded := loadRepoSlotMap(pgCluster)

	assert.Equal(t, m, loaded)
}

// TestReconcileRepoSlotMap_FullLifecycle simulates the real-world scenario of:
// 1. Creating an instance with 3 storages → they get repo1, repo2, repo3.
// 2. Removing the middle storage (storage-b) → storage-a stays repo1, storage-c stays repo3.
// 3. Adding a replacement storage (storage-d) → it gets repo2 (the freed slot).
//
// This proves that existing storages never shift slots, which prevents pgBackRest
// from losing track of existing backups at their original repo paths.
func TestReconcileRepoSlotMap_FullLifecycle(t *testing.T) {
	t.Parallel()

	pgCluster := &pgv2.PerconaPGCluster{}

	// Step 1: Initial creation with 3 storages.
	storagesV1 := []corev1alpha1.InstanceBackupStorage{
		{Name: "storage-a"},
		{Name: "storage-b"},
		{Name: "storage-c"},
	}
	slotMap := reconcileRepoSlotMap(loadRepoSlotMap(pgCluster), storagesV1)
	saveRepoSlotMap(pgCluster, slotMap)

	assert.Equal(t, 0, slotMap["storage-a"], "storage-a should be repo1 (slot 0)")
	assert.Equal(t, 1, slotMap["storage-b"], "storage-b should be repo2 (slot 1)")
	assert.Equal(t, 2, slotMap["storage-c"], "storage-c should be repo3 (slot 2)")

	// Verify repo names.
	assert.Equal(t, "repo1", pgBackRestRepoName(slotMap["storage-a"]))
	assert.Equal(t, "repo2", pgBackRestRepoName(slotMap["storage-b"]))
	assert.Equal(t, "repo3", pgBackRestRepoName(slotMap["storage-c"]))

	// Step 2: Remove storage-b. Remaining: [storage-a, storage-c].
	storagesV2 := []corev1alpha1.InstanceBackupStorage{
		{Name: "storage-a"},
		{Name: "storage-c"},
	}
	slotMap = reconcileRepoSlotMap(loadRepoSlotMap(pgCluster), storagesV2)
	saveRepoSlotMap(pgCluster, slotMap)

	// storage-a and storage-c MUST keep their original slots.
	assert.Equal(t, 0, slotMap["storage-a"], "storage-a must remain at repo1 after removal")
	assert.Equal(t, 2, slotMap["storage-c"], "storage-c must remain at repo3 after removal — NOT shift to repo2")
	// storage-b is gone.
	_, hasB := slotMap["storage-b"]
	assert.False(t, hasB, "storage-b should be evicted from the slot map")

	// Step 3: Add storage-d as a replacement. It should get the freed slot 1 (repo2).
	storagesV3 := []corev1alpha1.InstanceBackupStorage{
		{Name: "storage-a"},
		{Name: "storage-c"},
		{Name: "storage-d"},
	}
	slotMap = reconcileRepoSlotMap(loadRepoSlotMap(pgCluster), storagesV3)
	saveRepoSlotMap(pgCluster, slotMap)

	assert.Equal(t, 0, slotMap["storage-a"], "storage-a still repo1")
	assert.Equal(t, 2, slotMap["storage-c"], "storage-c still repo3")
	assert.Equal(t, 1, slotMap["storage-d"], "storage-d should take the freed repo2 slot")

	assert.Equal(t, "repo1", pgBackRestRepoName(slotMap["storage-a"]))
	assert.Equal(t, "repo2", pgBackRestRepoName(slotMap["storage-d"]))
	assert.Equal(t, "repo3", pgBackRestRepoName(slotMap["storage-c"]))
}

// TestReconcileRepoSlotMap_CannotExceedFourStorages demonstrates that pgBackRest
// only supports repo1..repo4, so a 5th storage will not get a slot assigned.
// The only way to add a new storage when all 4 slots are occupied is to first
// remove an existing one to free up a slot.
func TestReconcileRepoSlotMap_CannotExceedFourStorages(t *testing.T) {
	t.Parallel()

	pgCluster := &pgv2.PerconaPGCluster{}

	// Step 1: Fill all 4 slots.
	storagesV1 := []corev1alpha1.InstanceBackupStorage{
		{Name: "us-east"},
		{Name: "us-west"},
		{Name: "eu-central"},
		{Name: "ap-south"},
	}
	slotMap := reconcileRepoSlotMap(loadRepoSlotMap(pgCluster), storagesV1)
	saveRepoSlotMap(pgCluster, slotMap)

	assert.Len(t, slotMap, 4, "all 4 slots should be occupied")
	assert.Equal(t, "repo1", pgBackRestRepoName(slotMap["us-east"]))
	assert.Equal(t, "repo2", pgBackRestRepoName(slotMap["us-west"]))
	assert.Equal(t, "repo3", pgBackRestRepoName(slotMap["eu-central"]))
	assert.Equal(t, "repo4", pgBackRestRepoName(slotMap["ap-south"]))

	// Step 2: Try to add a 5th storage without removing any — it won't get a slot.
	storagesV2 := []corev1alpha1.InstanceBackupStorage{
		{Name: "us-east"},
		{Name: "us-west"},
		{Name: "eu-central"},
		{Name: "ap-south"},
		{Name: "af-north"}, // 5th storage — no free slot available
	}
	slotMap = reconcileRepoSlotMap(loadRepoSlotMap(pgCluster), storagesV2)
	saveRepoSlotMap(pgCluster, slotMap)

	// The first 4 keep their slots.
	assert.Equal(t, 0, slotMap["us-east"])
	assert.Equal(t, 1, slotMap["us-west"])
	assert.Equal(t, 2, slotMap["eu-central"])
	assert.Equal(t, 3, slotMap["ap-south"])
	// The 5th storage has no slot — it's simply not in the map.
	_, has5th := slotMap["af-north"]
	assert.False(t, has5th, "5th storage must not get a slot — pgBackRest only supports repo1..repo4")

	// Step 3: Remove one storage to free a slot, then the 5th can be added.
	storagesV3 := []corev1alpha1.InstanceBackupStorage{
		{Name: "us-east"},
		// us-west removed — frees repo2
		{Name: "eu-central"},
		{Name: "ap-south"},
		{Name: "af-north"}, // now there's a free slot
	}
	slotMap = reconcileRepoSlotMap(loadRepoSlotMap(pgCluster), storagesV3)
	saveRepoSlotMap(pgCluster, slotMap)

	assert.Len(t, slotMap, 4, "all 4 slots occupied again")
	assert.Equal(t, 0, slotMap["us-east"], "us-east stays repo1")
	assert.Equal(t, 2, slotMap["eu-central"], "eu-central stays repo3")
	assert.Equal(t, 3, slotMap["ap-south"], "ap-south stays repo4")
	assert.Equal(t, 1, slotMap["af-north"], "af-north gets the freed repo2 slot")
}

// TestPruneUnreferencedStorages_RemovesOrphanedStorage demonstrates that a
// storage with no schedules and no Backup CRs referencing it gets automatically
// removed from the Instance spec, freeing the repo slot for reuse.
func TestPruneUnreferencedStorages_RemovesOrphanedStorage(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	require.NoError(t, backupv1alpha1.AddToScheme(scheme))

	instance := &corev1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "my-pg", Namespace: "default"},
		Spec: corev1alpha1.InstanceSpec{
			Provider: "provider-percona-postgresql",
			Backup: &corev1alpha1.InstanceBackupSpec{
				Enabled:  true,
				ClassRef: corev1alpha1.BackupClassReference{Name: "pg"},
				Storages: []corev1alpha1.InstanceBackupStorage{
					{
						Name:       "active-storage",
						StorageRef: corev1.LocalObjectReference{Name: "s3-bucket-1"},
						Schedules: []corev1alpha1.InstanceBackupSchedule{
							{Name: "nightly", Enabled: true, Cron: "0 2 * * *"},
						},
					},
					{
						Name:       "referenced-storage",
						StorageRef: corev1.LocalObjectReference{Name: "s3-bucket-2"},
						// No schedules, but has a Backup CR referencing it.
					},
					{
						Name:       "orphaned-storage",
						StorageRef: corev1.LocalObjectReference{Name: "s3-bucket-3"},
						// No schedules, no backups — should be pruned.
					},
				},
			},
		},
	}

	// A backup referencing "referenced-storage".
	backup := &backupv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "backup-1", Namespace: "default"},
		Spec: backupv1alpha1.BackupSpec{
			InstanceName:    "my-pg",
			BackupClassName: "pg",
			StorageName:     "referenced-storage",
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(instance, backup).
		WithIndex(&backupv1alpha1.Backup{}, controller.IndexBackupInstanceName, func(obj client.Object) []string {
			return []string{obj.(*backupv1alpha1.Backup).Spec.InstanceName}
		}).
		Build()

	c := controller.NewContext(context.Background(), k8sClient, instance, "provider-percona-postgresql")

	pruned, err := pruneUnreferencedStorages(c)
	require.NoError(t, err)
	assert.True(t, pruned, "should have pruned orphaned storage")

	// Verify the instance now only has the active and referenced storages.
	assert.Len(t, c.Instance().Spec.Backup.Storages, 2)
	storageNames := make([]string, len(c.Instance().Spec.Backup.Storages))
	for i, s := range c.Instance().Spec.Backup.Storages {
		storageNames[i] = s.Name
	}
	assert.Contains(t, storageNames, "active-storage")
	assert.Contains(t, storageNames, "referenced-storage")
	assert.NotContains(t, storageNames, "orphaned-storage")
}

// TestPruneUnreferencedStorages_KeepsMainStorage demonstrates that a storage
// marked as Main is never pruned, even without schedules or backups.
func TestPruneUnreferencedStorages_KeepsMainStorage(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	require.NoError(t, backupv1alpha1.AddToScheme(scheme))

	instance := &corev1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "my-pg", Namespace: "default"},
		Spec: corev1alpha1.InstanceSpec{
			Provider: "provider-percona-postgresql",
			Backup: &corev1alpha1.InstanceBackupSpec{
				Enabled:  true,
				ClassRef: corev1alpha1.BackupClassReference{Name: "pg"},
				Storages: []corev1alpha1.InstanceBackupStorage{
					{
						Name:       "main-storage",
						StorageRef: corev1.LocalObjectReference{Name: "s3-primary"},
						Main:       true,
						// No schedules, no backups — but Main=true protects it.
					},
					{
						Name:       "orphaned-storage",
						StorageRef: corev1.LocalObjectReference{Name: "s3-temp"},
						// No schedules, no backups, not main — should be pruned.
					},
				},
			},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(instance).
		WithIndex(&backupv1alpha1.Backup{}, controller.IndexBackupInstanceName, func(obj client.Object) []string {
			return []string{obj.(*backupv1alpha1.Backup).Spec.InstanceName}
		}).
		Build()

	c := controller.NewContext(context.Background(), k8sClient, instance, "provider-percona-postgresql")

	pruned, err := pruneUnreferencedStorages(c)
	require.NoError(t, err)
	assert.True(t, pruned, "should have pruned orphaned-storage")

	assert.Len(t, c.Instance().Spec.Backup.Storages, 1)
	assert.Equal(t, "main-storage", c.Instance().Spec.Backup.Storages[0].Name)
}

// TestPruneUnreferencedStorages_NoPruneWhenAllReferenced verifies no changes
// happen when all storages are actively referenced.
func TestPruneUnreferencedStorages_NoPruneWhenAllReferenced(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	require.NoError(t, backupv1alpha1.AddToScheme(scheme))

	instance := &corev1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "my-pg", Namespace: "default"},
		Spec: corev1alpha1.InstanceSpec{
			Provider: "provider-percona-postgresql",
			Backup: &corev1alpha1.InstanceBackupSpec{
				Enabled:  true,
				ClassRef: corev1alpha1.BackupClassReference{Name: "pg"},
				Storages: []corev1alpha1.InstanceBackupStorage{
					{
						Name:       "storage-with-schedule",
						StorageRef: corev1.LocalObjectReference{Name: "s3-1"},
						Schedules: []corev1alpha1.InstanceBackupSchedule{
							{Name: "daily", Enabled: true, Cron: "0 0 * * *"},
						},
					},
					{
						Name:       "storage-with-backup",
						StorageRef: corev1.LocalObjectReference{Name: "s3-2"},
					},
				},
			},
		},
	}

	backup := &backupv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "backup-1", Namespace: "default"},
		Spec: backupv1alpha1.BackupSpec{
			InstanceName:    "my-pg",
			BackupClassName: "pg",
			StorageName:     "storage-with-backup",
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(instance, backup).
		WithIndex(&backupv1alpha1.Backup{}, controller.IndexBackupInstanceName, func(obj client.Object) []string {
			return []string{obj.(*backupv1alpha1.Backup).Spec.InstanceName}
		}).
		Build()

	c := controller.NewContext(context.Background(), k8sClient, instance, "provider-percona-postgresql")

	pruned, err := pruneUnreferencedStorages(c)
	require.NoError(t, err)
	assert.False(t, pruned, "nothing should be pruned when all storages are referenced")
	assert.Len(t, c.Instance().Spec.Backup.Storages, 2)
}
