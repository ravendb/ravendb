using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class ChannelStatsEndpointsTests(ITestOutputHelper output) : QuillTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Channel_stats_counts_total_and_active()
    {
        await using var app = await NewAppAsync();

        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "agent", Name = "Agent", SystemPrompt = "You help.", ConnectionStringName = Host.ConnectionStringName,
        });

        await app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.IFrame, "agent", Array.Empty<string>()));
        await app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.IFrame, "agent", Array.Empty<string>()));
        var gamma = await app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.IFrame, "agent", Array.Empty<string>()));
        await app.UpdateChannelAsync(gamma.ChannelId, new UpdateChannelRequest(null, null, Enabled: false));

        var stats = await app.GetChannelStatsAsync();
        Assert.Equal(3, stats.Total);
        Assert.Equal(2, stats.Active);
    }
}
