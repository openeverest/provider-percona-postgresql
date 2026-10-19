package provider

import (
	"context"
	"testing"

	corev1alpha1 "github.com/openeverest/openeverest/v2/api/core/v1alpha1"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPMMAdminParamsLabelTheInstance(t *testing.T) {
	t.Parallel()

	in := &corev1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: "my-pg", Namespace: "db"}}
	c := controller.NewContext(context.Background(), nil, in, "provider-percona-postgresql")

	assert.Equal(t,
		"--custom-labels=namespace=db,core_openeverest_io_instance=my-pg",
		pmmAdminParams(c))
}
