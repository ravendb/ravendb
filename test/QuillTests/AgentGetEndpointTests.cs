using Raven.Client.Documents.Operations.AI.Agents;
using System.Net;
using QuillTests.E2E.Fixtures;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class AgentGetEndpointTests(ITestOutputHelper output) : QuillTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Get_returns_full_agent_configuration()
    {
        await using var app = await NewAppAsync();
        var agentId = (await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "support", Name = "Support", SystemPrompt = "You help.",
            ConnectionStringName = app.Host.ConnectionStringName,
        })).AgentId;

        var details = await app.GetAgentAsync(agentId);
        Assert.Equal(agentId, details.Configuration.Identifier);
        Assert.Equal("Support", details.Configuration.Name);
        Assert.Equal(Host.ConnectionStringName, details.Configuration.ConnectionStringName);
        Assert.Empty(details.ActionBindings);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Get_returns_404_for_unknown_slug()
    {
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.GetAgentAsync("nonexistent", "whatever"));
        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Get_returns_404_for_unknown_agent()
    {
        await using var app = await NewAppAsync();

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.GetAgentAsync("does-not-exist"));
        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }
}
