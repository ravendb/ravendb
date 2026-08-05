using System.Net;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

[Collection(QuillFanOutCollection.Name)]
public class DashboardAppsEndpointTests(ITestOutputHelper output, QuillCollectionHost collection)
    : QuillTestBase(output, collection)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task DashboardApps_enriches_each_app_with_counts_and_status()
    {
        await using var app = await NewAppAsync();

        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "support", Name = "Support", SystemPrompt = "You help.",
            ConnectionStringName = app.Host.ConnectionStringName,
        });
        await app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.IFrame, "support", Array.Empty<string>()));

        var appResp = (await Host.GetDashboardAppsAsync()).Single(a => a.Slug == app.Slug);

        Assert.Equal(app.Slug, appResp.Id);
        Assert.Equal(1, appResp.AgentsCount);
        Assert.Equal(1, appResp.ChannelsCount);
        Assert.Equal("running", appResp.Status);
        Assert.True(appResp.DocumentsCount >= 1);
        Assert.Equal("Web widget", appResp.ChannelsLabel);
        Assert.Equal(1, appResp.TablesCount);
        Assert.Equal("PostgreSQL", appResp.Source.Type);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task DashboardApps_status_is_setup_when_no_agents()
    {
        await using var app = await NewAppAsync();

        var appResp = (await Host.GetDashboardAppsAsync()).Single(a => a.Slug == app.Slug);
        Assert.Equal("setup", appResp.Status);
        Assert.Equal(0, appResp.AgentsCount);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task DashboardApp_single_returns_enriched_app_or_404()
    {
        await using var app = await NewAppAsync();
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "support", Name = "Support", SystemPrompt = "You help.",
            ConnectionStringName = app.Host.ConnectionStringName,
        });

        var appResp = await Host.GetDashboardAppAsync(app.Slug);
        Assert.Equal(app.Slug, appResp.Id);
        Assert.Equal(app.Slug, appResp.Slug);
        Assert.Equal(1, appResp.AgentsCount);

        var missing = await Assert.ThrowsAsync<QuillHttpException>(() => Host.GetDashboardAppAsync("does-not-exist"));
        Assert.Equal(HttpStatusCode.NotFound, missing.StatusCode);
    }
}
