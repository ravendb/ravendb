using System.Net;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Cdc;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.Metrics;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

[Collection(QuillFanOutCollection.Name)]
public class DashboardAppsEndpointTests(ITestOutputHelper output, QuillCollectionHost collection)
    : QuillTestBase(output, collection)
{
    // Status lives in the dedicated tests below: a test app's sink points at a source that does not exist,
    // so its live status races the sink's first failed connection attempt.
    [RavenFact(RavenTestCategory.Quill)]
    public async Task DashboardApps_enriches_each_app_with_counts_and_source()
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
        Assert.True(appResp.DocumentsCount >= 1);
        Assert.Equal("Web widget", appResp.ChannelsLabel);
        Assert.Equal(1, appResp.TablesCount);
        Assert.Equal("PostgreSQL", appResp.Source.Type);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task DashboardApps_status_becomes_error_when_the_source_is_unreachable()
    {
        await using var app = await NewAppAsync();

        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "support", Name = "Support", SystemPrompt = "You help.",
            ConnectionStringName = app.Host.ConnectionStringName,
        });

        await AssertWaitForValueAsync(
            async () => (await Host.GetDashboardAppsAsync()).Single(a => a.Slug == app.Slug).Status,
            "error");
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
    public void DashboardApps_status_is_running_when_the_sink_reports_no_errors()
    {
        var hasSyncErrors = CdcPerformanceShaper.HasErrors(new CdcSinkErrorsRaw
        {
            Results = [new CdcTaskErrorsRaw { TaskName = "cdc" }],
        });
        Assert.False(hasSyncErrors);

        var (status, subtitle) = MetricsReadService.DeriveAppStatus(
            agentsCount: 1, channelsCount: 1, enabledChannels: 1, cdcDisabled: false, hasSyncErrors);

        Assert.Equal("running", status);
        Assert.Null(subtitle);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void DashboardApps_status_is_error_when_the_sink_reports_errors()
    {
        var hasSyncErrors = CdcPerformanceShaper.HasErrors(new CdcSinkErrorsRaw
        {
            Results =
            [
                new CdcTaskErrorsRaw
                {
                    TaskName = "cdc",
                    ItemErrors =
                    [
                        new CdcTaskErrorRaw
                        {
                            TaskName = "cdc",
                            CreatedAt = new DateTime(2026, 6, 25, 12, 0, 0, DateTimeKind.Utc),
                            Step = "Transformation",
                            Error = "bad row",
                            DocumentId = "orders/1",
                        },
                    ],
                },
            ],
        });
        Assert.True(hasSyncErrors);

        var (status, subtitle) = MetricsReadService.DeriveAppStatus(
            agentsCount: 1, channelsCount: 1, enabledChannels: 1, cdcDisabled: false, hasSyncErrors);

        Assert.Equal("error", status);
        Assert.Equal("Sync errors detected", subtitle);
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
