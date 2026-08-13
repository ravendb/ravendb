using System.Net;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Contracts;
using Raven.Quill.Channels;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class AgentDeleteEndpointTests(ITestOutputHelper output) : QuillTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Delete_removes_agent_and_returns_204()
    {
        await using var app = await NewAppAsync();
        var agentId = await ProvisionAgentAsync(app);

        await app.DeleteAgentAsync(agentId);

        var agents = await app.GetAgentsAsync();
        Assert.Empty(agents);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Delete_returns_404_for_unknown_slug()
    {
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.DeleteAgentAsync("nonexistent", "whatever"));
        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Delete_returns_404_for_unknown_agent()
    {
        await using var app = await NewAppAsync();

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.DeleteAgentAsync("does-not-exist"));
        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Delete_returns_409_when_a_channel_is_bound()
    {
        await using var app = await NewAppAsync();
        var agentId = await ProvisionAgentAsync(app);

        await app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.IFrame, agentId, Array.Empty<string>()));

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.DeleteAgentAsync(agentId));
        Assert.Equal(HttpStatusCode.Conflict, ex.StatusCode);

        var agents = await app.Store.AI.ForDatabase(app.Slug).GetAgentsAsync();
        Assert.Single(agents.AiAgents);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Delete_conflict_counts_channels_past_the_first_load_page()
    {
        await using var app = await NewAppAsync();
        var agentId = await ProvisionAgentAsync(app);

        for (var i = 0; i < 30; i++)
            await app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.IFrame, agentId, Array.Empty<string>()));

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.DeleteAgentAsync(agentId));
        Assert.Equal(HttpStatusCode.Conflict, ex.StatusCode);
        Assert.Contains("30 channel(s)", ex.Body);
    }

    private static async Task<string> ProvisionAgentAsync(QuillApp app)
    {
        var resp = await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "support", Name = "Support", SystemPrompt = "You help.", ConnectionStringName = app.Host.ConnectionStringName,
        });
        return resp.AgentId;
    }
}
