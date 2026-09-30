using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class AppOverviewEndpointTests(ITestOutputHelper output) : QuillTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task App_overview_reports_documents_agents_and_channels()
    {
        await using var app = await NewAppAsync();

        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "support", Name = "Support", SystemPrompt = "You help.", ConnectionStringName = Host.ConnectionStringName,
        });
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "sales", Name = "Sales", SystemPrompt = "You help.", ConnectionStringName = Host.ConnectionStringName,
        });

        await app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.IFrame, "support", Array.Empty<string>()));
        await app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.IFrame, "support", Array.Empty<string>()));
        var gamma = await app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.IFrame, "support", Array.Empty<string>()));
        await app.UpdateChannelAsync(gamma.ChannelId, new UpdateChannelRequest(null, null, Enabled: false));

        var overview = await app.GetOverviewAsync();
        Assert.Equal(app.Slug, overview.Slug);
        Assert.Equal(2, overview.ConfiguredAgents);
        Assert.Equal(3, overview.Channels);
        Assert.Equal(2, overview.ActiveChannels);
        Assert.True(overview.Documents >= 3);
    }
}
