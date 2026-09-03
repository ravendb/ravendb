using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Tests.Infrastructure;
using Xunit;
using static QuillTests.E2E.Fixtures.ConversationSeed;

namespace QuillTests;

public class AgentsListEndpointTests(ITestOutputHelper output) : QuillTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Agents_list_includes_invocations_and_last_invoked()
    {
        await using var app = await NewAppAsync();
        var agentId = (await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "support", Name = "Support", SystemPrompt = "You help.",
            ConnectionStringName = app.Host.ConnectionStringName,
        })).AgentId;

        var now = DateTime.UtcNow;
        await SeedConversationAsync(app.Store, app.Slug, "chats/a", agentId, now.AddHours(-1));
        await SeedConversationAsync(app.Store, app.Slug, "chats/b", agentId, now.AddHours(-2));

        var list = await app.GetAgentsAsync();
        var agent = list.Single(a => a.AgentId == agentId);

        Assert.Equal(2, agent.Conversations);
        Assert.Equal(2, agent.Messages);
        Assert.Equal("llama3.1", agent.Model);
        Assert.NotNull(agent.LastInvokedAt);
        Assert.Equal(DateTimeKind.Utc, agent.LastInvokedAt!.Value.Kind);
    }
}
