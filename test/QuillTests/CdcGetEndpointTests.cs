using System.Net;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Quill.Wizard;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class CdcGetEndpointTests(ITestOutputHelper output) : QuillTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task CdcGet_returns_current_configuration()
    {
        await using var app = await NewAppAsync();

        var cdc = await app.GetCdcAsync();
        Assert.Equal($"{app.Slug}-cdc", cdc.Configuration.Name);
        Assert.Equal("src", cdc.Configuration.ConnectionStringName);
        Assert.Equal("Host=localhost;Database=src", cdc.ConnectionString);
        var table = Assert.Single(cdc.Configuration.Tables);
        Assert.Equal("Orders", table.CollectionName);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task CdcGet_survives_a_wizard_state_reset()
    {
        await using var app = await NewAppAsync();

        // A later setup run for the same slug may wipe the wizard's scratch document (e.g. a
        // re-connect with a reformatted connection string); the provisioned app must not care.
        var session = Host.Config.OpenAsyncSession();
        var wizard = await session.LoadAsync<WizardState>(WizardState.DocumentIdFor(app.Slug));
        wizard.LastMapConfiguration = null;
        wizard.SourceConnectionString = null;
        await session.SaveChangesAsync();

        var cdc = await app.GetCdcAsync();
        Assert.Equal($"{app.Slug}-cdc", cdc.Configuration.Name);
        Assert.Equal("Host=localhost;Database=src", cdc.ConnectionString);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task CdcGet_returns_404_when_the_cdc_task_is_gone()
    {
        await using var app = await NewAppAsync();

        var task = await app.Store.Maintenance.SendAsync(
            new GetOngoingTaskInfoOperation($"{app.Slug}-cdc", OngoingTaskType.CdcSink));
        await app.Store.Maintenance.SendAsync(new DeleteOngoingTaskOperation(task.TaskId, OngoingTaskType.CdcSink));

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.GetCdcAsync());
        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task CdcGet_returns_404_for_unknown_slug()
    {
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.GetCdcAsync("nonexistent"));
        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }
}
