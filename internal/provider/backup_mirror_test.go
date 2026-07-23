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

func TestMirrorScheduledBackup(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	require.NoError(t, pgv2.AddToScheme(scheme))

	instance := &corev1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "inst-0hc", Namespace: "default"},
		Spec: corev1alpha1.InstanceSpec{
			Provider: "provider-percona-postgresql",
			Backup: &corev1alpha1.InstanceBackupSpec{
				ClassRef: corev1alpha1.BackupClassReference{Name: "pg"},
			},
		},
	}

	pgCluster := &pgv2.PerconaPGCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "inst-0hc",
			Namespace: "default",
			Annotations: map[string]string{
				repoSlotMapAnnotation: `{"my-storage":0}`,
			},
		},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance, pgCluster).Build()

	repoName := "repo1"
	// This is how the PG operator actually creates scheduled backups:
	// - generateName is set (e.g. "inst-0hc-repo1-full-")
	// - pgv2.percona.com/pgbackrest-backup-job-type: manual
	// - no Backup CR owner reference
	opBackup := &pgv2.PerconaPGBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:         "inst-0hc-repo1-full-p9dz9",
			GenerateName: "inst-0hc-repo1-full-",
			Namespace:    "default",
			UID:          "11111111-1111-1111-1111-111111111111",
			Annotations: map[string]string{
				annotationPGBackrestBackupJobType: "manual",
			},
		},
		Spec: pgv2.PerconaPGBackupSpec{
			PGCluster: "inst-0hc",
			RepoName:  &repoName,
		},
	}

	mirror, err := (&Provider{}).Mirror(context.Background(), k8sClient, opBackup)
	require.NoError(t, err)
	require.NotNil(t, mirror, "should create a Backup CR for scheduled backup")
	require.Equal(t, "full", mirror.Spec.ScheduleName, "schedule name should be derived from backup name")
	require.Equal(t, "my-storage", mirror.Spec.StorageName, "storage name should be resolved from slot map")
	require.Equal(t, "inst-0hc", mirror.Spec.InstanceName)
	require.Equal(t, "pg", mirror.Spec.BackupClassName)
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
	require.NoError(t, pgv2.AddToScheme(scheme))

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

	isController := true
	repoName := "repo1"
	// On-demand backup: fixed Name, controller owner ref from Backup CR, no generateName.
	opBackup := &pgv2.PerconaPGBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "manual-backup",
			Namespace: "my-special-place",
			Annotations: map[string]string{
				annotationPGBackrestBackupJobType: "manual",
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: backupv1alpha1.GroupVersion.String(),
				Kind:       "Backup",
				Name:       "manual-backup",
				UID:        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
				Controller: &isController,
			}},
		},
		Spec: pgv2.PerconaPGBackupSpec{
			PGCluster: "inst-pg",
			RepoName:  &repoName,
		},
	}

	mirror, err := (&Provider{}).Mirror(context.Background(), k8sClient, opBackup)
	require.NoError(t, err)
	require.Nil(t, mirror, "should skip on-demand backups owned by a Backup CR")
}

func TestMirrorSkipsReplicaCreateBackup(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	require.NoError(t, pgv2.AddToScheme(scheme))

	instance := &corev1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "inst-pg", Namespace: "my-special-place"},
		Spec: corev1alpha1.InstanceSpec{
			Provider: "provider-percona-postgresql",
			Backup:   &corev1alpha1.InstanceBackupSpec{ClassRef: corev1alpha1.BackupClassReference{Name: "pg"}},
		},
	}

	pgCluster := &pgv2.PerconaPGCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "inst-pg",
			Namespace: "my-special-place",
			Annotations: map[string]string{
				repoSlotMapAnnotation: `{"my-storage":0}`,
			},
		},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance, pgCluster).Build()

	repoName := "repo1"
	// Replica-create backup: has generateName but job-type is "replica-create".
	opBackup := &pgv2.PerconaPGBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:         "inst-pg-backup-b4wc-4m29s",
			GenerateName: "inst-pg-backup-b4wc-",
			Namespace:    "my-special-place",
			Annotations: map[string]string{
				annotationPGBackrestBackupJobType: "replica-create",
			},
		},
		Spec: pgv2.PerconaPGBackupSpec{
			PGCluster: "inst-pg",
			RepoName:  &repoName,
		},
	}

	mirror, err := (&Provider{}).Mirror(context.Background(), k8sClient, opBackup)
	require.NoError(t, err)
	require.Nil(t, mirror, "should skip replica-create backups")
}

func TestMirrorSkipsWhenNoSlotMap(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	require.NoError(t, pgv2.AddToScheme(scheme))

	instance := &corev1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "inst-pg", Namespace: "my-special-place"},
		Spec: corev1alpha1.InstanceSpec{
			Provider: "provider-percona-postgresql",
			Backup:   &corev1alpha1.InstanceBackupSpec{ClassRef: corev1alpha1.BackupClassReference{Name: "pg"}},
		},
	}

	// PerconaPGCluster exists but has no slot map annotation.
	pgCluster := &pgv2.PerconaPGCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "inst-pg",
			Namespace: "my-special-place",
		},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance, pgCluster).Build()

	repoName := "repo1"
	opBackup := &pgv2.PerconaPGBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:         "inst-pg-repo1-full-xxxxx",
			GenerateName: "inst-pg-repo1-full-",
			Namespace:    "my-special-place",
			Annotations: map[string]string{
				annotationPGBackrestBackupJobType: "manual",
			},
		},
		Spec: pgv2.PerconaPGBackupSpec{
			PGCluster: "inst-pg",
			RepoName:  &repoName,
		},
	}

	mirror, err := (&Provider{}).Mirror(context.Background(), k8sClient, opBackup)
	require.NoError(t, err)
	require.Nil(t, mirror, "should skip mirroring when slot map cannot resolve repo to storage name")
}

// TestMirrorScheduledBackupIncremental verifies that Mirror correctly derives
// the schedule name for different backup types (incr, diff).
func TestMirrorScheduledBackupIncremental(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	require.NoError(t, pgv2.AddToScheme(scheme))

	instance := &corev1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Name: "inst-0hc", Namespace: "default"},
		Spec: corev1alpha1.InstanceSpec{
			Provider: "provider-percona-postgresql",
			Backup: &corev1alpha1.InstanceBackupSpec{
				ClassRef: corev1alpha1.BackupClassReference{Name: "pg"},
			},
		},
	}

	pgCluster := &pgv2.PerconaPGCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "inst-0hc",
			Namespace: "default",
			Annotations: map[string]string{
				repoSlotMapAnnotation: `{"bucket-1":0,"bucket-2":1}`,
			},
		},
	}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(instance, pgCluster).Build()

	repoName := "repo2"
	opBackup := &pgv2.PerconaPGBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:         "inst-0hc-repo2-incr-abc12",
			GenerateName: "inst-0hc-repo2-incr-",
			Namespace:    "default",
			UID:          "22222222-2222-2222-2222-222222222222",
			Annotations: map[string]string{
				annotationPGBackrestBackupJobType: "manual",
			},
		},
		Spec: pgv2.PerconaPGBackupSpec{
			PGCluster: "inst-0hc",
			RepoName:  &repoName,
		},
	}

	mirror, err := (&Provider{}).Mirror(context.Background(), k8sClient, opBackup)
	require.NoError(t, err)
	require.NotNil(t, mirror, "should create a Backup CR for scheduled incremental backup")
	require.Equal(t, "incr", mirror.Spec.ScheduleName, "schedule name should be 'incr'")
	require.Equal(t, "bucket-2", mirror.Spec.StorageName, "storage name should be resolved from slot map for repo2")
}

func TestDeriveScheduleName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		backupName  string
		clusterName string
		repoName    string
		want        string
	}{
		{
			name:        "full backup",
			backupName:  "inst-0hc-repo1-full-p9dz9",
			clusterName: "inst-0hc",
			repoName:    "repo1",
			want:        "full",
		},
		{
			name:        "incremental backup",
			backupName:  "inst-0hc-repo1-incr-abc12",
			clusterName: "inst-0hc",
			repoName:    "repo1",
			want:        "incr",
		},
		{
			name:        "differential backup",
			backupName:  "inst-0hc-repo2-diff-xyz99",
			clusterName: "inst-0hc",
			repoName:    "repo2",
			want:        "diff",
		},
		{
			name:        "no matching prefix",
			backupName:  "other-backup-name",
			clusterName: "inst-0hc",
			repoName:    "repo1",
			want:        "other-backup-name",
		},
		{
			name:        "generated name with extra suffix",
			backupName:  "inst-0hc-repo1-full-abcde-12345",
			clusterName: "inst-0hc",
			repoName:    "repo1",
			want:        "full",
		},
		{
			name:        "cronjob job name with timestamp suffix",
			backupName:  "inst-0hc-repo1-full-28472348-p9dz9",
			clusterName: "inst-0hc",
			repoName:    "repo1",
			want:        "full",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := deriveScheduleName(tt.backupName, tt.clusterName, tt.repoName)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestApplyRetentionConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		repoName  string
		schedules []corev1alpha1.InstanceBackupSchedule
		wantKeys  map[string]string
	}{
		{
			name:     "full backup with retention",
			repoName: "repo1",
			schedules: []corev1alpha1.InstanceBackupSchedule{
				{Name: "full", Enabled: true, Cron: "0 2 * * *", RetentionCopies: 3},
			},
			wantKeys: map[string]string{
				"repo1-retention-full":      "3",
				"repo1-retention-full-type": "count",
			},
		},
		{
			name:     "differential backup with retention",
			repoName: "repo2",
			schedules: []corev1alpha1.InstanceBackupSchedule{
				{Name: "differential", Enabled: true, Cron: "0 3 * * *", RetentionCopies: 5},
			},
			wantKeys: map[string]string{
				"repo2-retention-diff": "5",
			},
		},
		{
			name:     "zero retention means no config",
			repoName: "repo1",
			schedules: []corev1alpha1.InstanceBackupSchedule{
				{Name: "full", Enabled: true, Cron: "0 2 * * *", RetentionCopies: 0},
			},
			wantKeys: map[string]string{},
		},
		{
			name:     "disabled schedule skipped",
			repoName: "repo1",
			schedules: []corev1alpha1.InstanceBackupSchedule{
				{Name: "full", Enabled: false, Cron: "0 2 * * *", RetentionCopies: 3},
			},
			wantKeys: map[string]string{},
		},
		{
			name:     "default schedule name maps to full retention",
			repoName: "repo1",
			schedules: []corev1alpha1.InstanceBackupSchedule{
				{Name: "nightly", Enabled: true, Cron: "0 2 * * *", RetentionCopies: 1},
			},
			wantKeys: map[string]string{
				"repo1-retention-full":      "1",
				"repo1-retention-full-type": "count",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			globalConfig := make(map[string]string)
			applyRetentionConfig(globalConfig, tt.repoName, tt.schedules)
			for k, v := range tt.wantKeys {
				assert.Equal(t, v, globalConfig[k], "key %q", k)
			}
			// Ensure no unexpected keys were added.
			assert.Len(t, globalConfig, len(tt.wantKeys))
		})
	}
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

// TestAutoRegisterStorage_RegistersNewStorage demonstrates that when a Backup CR
// references a storage not yet on the Instance, the provider automatically adds
// it if a BackupStorage resource with that name exists.
func TestAutoRegisterStorage_RegistersNewStorage(t *testing.T) {
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
						Name:       "existing-storage",
						StorageRef: corev1.LocalObjectReference{Name: "existing-storage"},
					},
				},
			},
		},
	}

	// A BackupStorage resource exists for the new storage name.
	backupStorage := &backupv1alpha1.BackupStorage{
		ObjectMeta: metav1.ObjectMeta{Name: "new-storage", Namespace: "default"},
		Spec: backupv1alpha1.BackupStorageSpec{
			Type: backupv1alpha1.BackupStorageTypeS3,
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(instance, backupStorage).
		Build()

	c := controller.NewContext(context.Background(), k8sClient, instance, "provider-percona-postgresql")

	// Auto-register should succeed.
	registered, err := autoRegisterStorage(c, "new-storage")
	require.NoError(t, err)
	assert.True(t, registered)

	// Instance should now have 2 storages.
	assert.Len(t, c.Instance().Spec.Backup.Storages, 2)
	assert.Equal(t, "existing-storage", c.Instance().Spec.Backup.Storages[0].Name)
	assert.Equal(t, "new-storage", c.Instance().Spec.Backup.Storages[1].Name)
	assert.Equal(t, "new-storage", c.Instance().Spec.Backup.Storages[1].StorageRef.Name)
}

// TestAutoRegisterStorage_SkipsWhenBackupStorageNotFound verifies that
// auto-registration does nothing when no BackupStorage resource exists.
func TestAutoRegisterStorage_SkipsWhenBackupStorageNotFound(t *testing.T) {
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
						Name:       "existing-storage",
						StorageRef: corev1.LocalObjectReference{Name: "existing-storage"},
					},
				},
			},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(instance).
		Build()

	c := controller.NewContext(context.Background(), k8sClient, instance, "provider-percona-postgresql")

	// No BackupStorage named "nonexistent" → should return false without error.
	registered, err := autoRegisterStorage(c, "nonexistent")
	require.NoError(t, err)
	assert.False(t, registered)
	assert.Len(t, c.Instance().Spec.Backup.Storages, 1)
}

// TestAutoRegisterStorage_RespectsMaxRepos verifies that auto-registration
// won't exceed the 4-repo limit.
func TestAutoRegisterStorage_RespectsMaxRepos(t *testing.T) {
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
					{Name: "s1", StorageRef: corev1.LocalObjectReference{Name: "s1"}},
					{Name: "s2", StorageRef: corev1.LocalObjectReference{Name: "s2"}},
					{Name: "s3", StorageRef: corev1.LocalObjectReference{Name: "s3"}},
					{Name: "s4", StorageRef: corev1.LocalObjectReference{Name: "s4"}},
				},
			},
		},
	}

	backupStorage := &backupv1alpha1.BackupStorage{
		ObjectMeta: metav1.ObjectMeta{Name: "s5", Namespace: "default"},
		Spec:       backupv1alpha1.BackupStorageSpec{Type: backupv1alpha1.BackupStorageTypeS3},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(instance, backupStorage).
		Build()

	c := controller.NewContext(context.Background(), k8sClient, instance, "provider-percona-postgresql")

	// All 4 slots full — should not register.
	registered, err := autoRegisterStorage(c, "s5")
	require.NoError(t, err)
	assert.False(t, registered)
	assert.Len(t, c.Instance().Spec.Backup.Storages, 4)
}
