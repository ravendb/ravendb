using System.Net;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Quill.Auth;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class CdcRestartEndpointTests(ITestOutputHelper output) : QuillTestBase(output)
{
    private static async Task DisableCdcAsync(QuillApp app)
    {
        var task = await app.Store.Maintenance.SendAsync(
            new GetOngoingTaskInfoOperation($"{app.Slug}-cdc", OngoingTaskType.CdcSink));
        await app.Store.Maintenance.SendAsync(
            new ToggleOngoingTaskStateOperation(task.TaskId, OngoingTaskType.CdcSink, disable: true));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task CdcRestart_leaves_the_task_running()
    {
        await using var app = await NewAppAsync();

        await app.RestartCdcAsync();

        var cdc = await app.GetCdcAsync();
        Assert.False(cdc.Configuration.Disabled);
        Assert.Equal($"{app.Slug}-cdc", cdc.Configuration.Name);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task CdcRestart_keeps_the_configuration_intact()
    {
        await using var app = await NewAppAsync();

        var before = await app.GetCdcAsync();
        await app.RestartCdcAsync();
        var after = await app.GetCdcAsync();

        Assert.Equal(before.Configuration.ConnectionStringName, after.Configuration.ConnectionStringName);
        Assert.Equal(before.ConnectionString, after.ConnectionString);
        Assert.Equal(
            before.Configuration.Tables.Select(t => t.CollectionName),
            after.Configuration.Tables.Select(t => t.CollectionName));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task CdcRestart_refuses_a_disabled_task_and_leaves_it_disabled()
    {
        await using var app = await NewAppAsync();
        await DisableCdcAsync(app);

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.RestartCdcAsync());
        Assert.Equal(HttpStatusCode.Conflict, ex.StatusCode);

        var cdc = await app.GetCdcAsync();
        Assert.True(cdc.Configuration.Disabled);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task CdcRestart_returns_404_when_no_cdc()
    {
        await using var app = await NewAppAsync();

        var task = await app.Store.Maintenance.SendAsync(
            new GetOngoingTaskInfoOperation($"{app.Slug}-cdc", OngoingTaskType.CdcSink));
        await app.Store.Maintenance.SendAsync(new DeleteOngoingTaskOperation(task.TaskId, OngoingTaskType.CdcSink));

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.RestartCdcAsync());
        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task CdcRestart_returns_404_for_unknown_app()
    {
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.RestartCdcAsync("does-not-exist"));
        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task CdcRestart_requires_authentication()
    {
        // fresh client (not shared Host.Client) so other tests unaffected; auth runs before app lookup
        using var client = Host.Factory.CreateClient();
        client.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);

        var resp = await client.PostAsync(QuillRoutes.AppCdcRestart("my-app"), content: null);
        Assert.Equal(HttpStatusCode.Unauthorized, resp.StatusCode);
    }
}
