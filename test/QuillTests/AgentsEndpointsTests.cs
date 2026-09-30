using System.Net;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class AgentsEndpointsTests(ITestOutputHelper output) : QuillTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Agents_list_returns_provisioned_agent_with_model()
    {
        await using var app = await NewAppAsync();
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "support", Name = "Support", SystemPrompt = "You help.",
            ConnectionStringName = app.Host.ConnectionStringName,
        });

        var items = await app.GetAgentsAsync();
        var item = Assert.Single(items);
        Assert.Equal("Support", item.Name);
        Assert.Equal("llama3.1", item.Model);
        Assert.False(item.Disabled);
        Assert.False(string.IsNullOrEmpty(item.AgentId));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Agents_list_is_empty_for_app_with_no_agents()
    {
        await using var app = await NewAppAsync();

        var items = await app.GetAgentsAsync();
        Assert.Empty(items);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Agents_list_returns_404_for_unknown_slug()
    {
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.GetAgentsAsync("nonexistent"));
        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }
}
