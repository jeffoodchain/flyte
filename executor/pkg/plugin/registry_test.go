package plugin

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	toolscache "k8s.io/client-go/tools/cache"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	executorConfig "github.com/flyteorg/flyte/v2/executor/pkg/config"
	pluginsCore "github.com/flyteorg/flyte/v2/flyteplugins/go/tasks/pluginmachinery/core"
	pluginsCoreMock "github.com/flyteorg/flyte/v2/flyteplugins/go/tasks/pluginmachinery/core/mocks"
	k8sPlugin "github.com/flyteorg/flyte/v2/flyteplugins/go/tasks/pluginmachinery/k8s"
	k8sMocks "github.com/flyteorg/flyte/v2/flyteplugins/go/tasks/pluginmachinery/k8s/mocks"
	"github.com/flyteorg/flyte/v2/flytestdlib/promutils"
)

// mockCorePlugin is a minimal pluginsCore.Plugin for use in tests.
type mockCorePlugin struct{ id string }

func (m *mockCorePlugin) GetID() string { return m.id }
func (m *mockCorePlugin) GetProperties() pluginsCore.PluginProperties {
	return pluginsCore.PluginProperties{}
}
func (m *mockCorePlugin) Handle(_ context.Context, _ pluginsCore.TaskExecutionContext) (pluginsCore.Transition, error) {
	return pluginsCore.Transition{}, nil
}
func (m *mockCorePlugin) Abort(_ context.Context, _ pluginsCore.TaskExecutionContext) error {
	return nil
}
func (m *mockCorePlugin) Finalize(_ context.Context, _ pluginsCore.TaskExecutionContext) error {
	return nil
}

// mockPluginRegistry implements PluginRegistryIface with configurable entries.
type mockPluginRegistry struct {
	corePlugins []pluginsCore.PluginEntry
	k8sPlugins  []k8sPlugin.PluginEntry
}

func (m *mockPluginRegistry) GetCorePlugins() []pluginsCore.PluginEntry { return m.corePlugins }
func (m *mockPluginRegistry) GetK8sPlugins() []k8sPlugin.PluginEntry    { return m.k8sPlugins }

// coreEntry builds a PluginEntry backed by a mockCorePlugin.
func coreEntry(id string, taskTypes ...string) pluginsCore.PluginEntry {
	p := &mockCorePlugin{id: id}
	return pluginsCore.PluginEntry{
		ID:                  id,
		RegisteredTaskTypes: taskTypes,
		LoadPlugin: func(_ context.Context, _ pluginsCore.SetupContext) (pluginsCore.Plugin, error) {
			return p, nil
		},
	}
}

func newTestRegistry(reg PluginRegistryIface) *Registry {
	setupCtx := NewSetupContext(nil, nil, nil, nil, nil, "TaskAction", promutils.NewTestScope())
	return NewRegistry(setupCtx, reg)
}

// All compiled-in plugins are loaded.
func TestRegistry_LoadsAllPlugins(t *testing.T) {
	reg := &mockPluginRegistry{
		corePlugins: []pluginsCore.PluginEntry{
			coreEntry("container", "container"),
			coreEntry("sidecar", "sidecar"),
		},
	}
	r := newTestRegistry(reg)
	require.NoError(t, r.Initialize(context.Background()))

	p, err := r.ResolvePlugin("container")
	require.NoError(t, err)
	assert.Equal(t, "container", p.GetID())

	p, err = r.ResolvePlugin("sidecar")
	require.NoError(t, err)
	assert.Equal(t, "sidecar", p.GetID())
}

// 3.5 DefaultForTaskTypes routes a task type to a different loaded plugin.
func TestRegistry_DefaultForTaskTypes_Override(t *testing.T) {
	require.NoError(t, executorConfig.SetTaskPluginConfig(&executorConfig.TaskPluginConfig{
		DefaultForTaskTypes: map[string]string{"sidecar": "container"},
	}))
	t.Cleanup(func() { _ = executorConfig.SetTaskPluginConfig(&executorConfig.TaskPluginConfig{}) })

	reg := &mockPluginRegistry{
		corePlugins: []pluginsCore.PluginEntry{
			coreEntry("container", "container"),
			coreEntry("sidecar", "sidecar"),
		},
	}
	r := newTestRegistry(reg)
	require.NoError(t, r.Initialize(context.Background()))

	// sidecar task type must now resolve to the container plugin
	p, err := r.ResolvePlugin("sidecar")
	require.NoError(t, err)
	assert.Equal(t, "container", p.GetID())

	// container task type is unaffected
	p, err = r.ResolvePlugin("container")
	require.NoError(t, err)
	assert.Equal(t, "container", p.GetID())
}

// 3.6 DefaultForTaskTypes with an unknown plugin ID does not panic and leaves routing unchanged.
func TestRegistry_DefaultForTaskTypes_UnknownPluginID(t *testing.T) {
	require.NoError(t, executorConfig.SetTaskPluginConfig(&executorConfig.TaskPluginConfig{
		DefaultForTaskTypes: map[string]string{"container": "nonexistent-plugin"},
	}))
	t.Cleanup(func() { _ = executorConfig.SetTaskPluginConfig(&executorConfig.TaskPluginConfig{}) })

	reg := &mockPluginRegistry{
		corePlugins: []pluginsCore.PluginEntry{
			coreEntry("container", "container"),
		},
	}
	r := newTestRegistry(reg)
	require.NoError(t, r.Initialize(context.Background()))

	// routing for container must remain as the original container plugin
	p, err := r.ResolvePlugin("container")
	require.NoError(t, err)
	assert.Equal(t, "container", p.GetID())
}

// countingInformer records how many handlers are added to the Event informer.
type countingInformer struct {
	ctrlcache.Informer
	handlers int
}

func (i *countingInformer) AddEventHandler(toolscache.ResourceEventHandler) (toolscache.ResourceEventHandlerRegistration, error) {
	i.handlers++
	return nil, nil
}

// singleInformerCache hands out the same informer for every GetInformer call, as the
// controller-runtime cache does for one kind, and counts the calls.
type singleInformerCache struct {
	ctrlcache.Cache
	informer *countingInformer
	calls    int
}

func (c *singleInformerCache) GetInformer(context.Context, client.Object, ...ctrlcache.InformerGetOption) (ctrlcache.Informer, error) {
	c.calls++
	return c.informer, nil
}

// Every k8s plugin manager shares one event watcher, so the Event informer gets a single
// handler however many k8s plugins are registered.
func TestRegistry_K8sPluginsShareOneEventWatcher(t *testing.T) {
	cache := &singleInformerCache{informer: &countingInformer{}}
	kubeClient := &pluginsCoreMock.KubeClient{}
	kubeClient.EXPECT().GetCache().Return(cache)

	reg := &mockPluginRegistry{
		k8sPlugins: []k8sPlugin.PluginEntry{
			{ID: "pod", RegisteredTaskTypes: []string{"container"}, Plugin: k8sMocks.NewPlugin(t)},
			{ID: "spark", RegisteredTaskTypes: []string{"spark"}, Plugin: k8sMocks.NewPlugin(t)},
			{ID: "ray", RegisteredTaskTypes: []string{"ray"}, Plugin: k8sMocks.NewPlugin(t)},
		},
	}
	setupCtx := NewSetupContext(kubeClient, nil, nil, nil, nil, "TaskAction", promutils.NewTestScope())
	r := NewRegistry(setupCtx, reg)
	require.NoError(t, r.Initialize(context.Background()))

	assert.Equal(t, 1, cache.calls)
	assert.Equal(t, 1, cache.informer.handlers)

	for _, taskType := range []string{"container", "spark", "ray"} {
		_, err := r.ResolvePlugin(taskType)
		require.NoError(t, err)
	}
}

// With no k8s plugins registered, no event watcher is built and the cache is never asked
// for the Event informer.
func TestRegistry_NoK8sPluginsNoEventWatcher(t *testing.T) {
	reg := &mockPluginRegistry{corePlugins: []pluginsCore.PluginEntry{coreEntry("container", "container")}}
	r := newTestRegistry(reg)
	require.NoError(t, r.Initialize(context.Background()))
}
