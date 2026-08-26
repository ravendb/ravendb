using System.Net;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class AgentEditEndpointTests(ITestOutputHelper output) : QuillTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Edit_updates_existing_agent_in_place()
    {
        await using var app = await NewAppAsync();
        var agentId = (await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "support", Name = "Support", SystemPrompt = "You help.",
            ConnectionStringName = app.Host.ConnectionStringName,
        })).AgentId;

        var edited = await app.EditAgentAsync(new AiAgentConfiguration
        {
            Identifier = agentId, Name = "Support", SystemPrompt = "You are the updated support agent.", ConnectionStringName = Host.ConnectionStringName,
        });
        Assert.Equal(agentId, edited.AgentId);

        var agents = await app.Store.Maintenance.ForDatabase(app.Slug).SendAsync(new GetAiAgentsOperation());
        var agent = Assert.Single(agents.AiAgents);
        Assert.Equal(agentId, agent.Identifier);
        Assert.Equal("Support", agent.Name);
        Assert.Equal("You are the updated support agent.", agent.SystemPrompt);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Edit_returns_400_when_renaming()
    {
        await using var app = await NewAppAsync();
        var agentId = (await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "support", Name = "Support", SystemPrompt = "You help.",
            ConnectionStringName = app.Host.ConnectionStringName,
        })).AgentId;

        // server binds identifier to name; renaming is refused
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.EditAgentAsync(new AiAgentConfiguration
        {
            Identifier = agentId, Name = "Renamed", SystemPrompt = "You help.", ConnectionStringName = Host.ConnectionStringName,
        }));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Edit_returns_404_for_unknown_agent()
    {
        await using var app = await NewAppAsync();
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "support", Name = "Support", SystemPrompt = "You help.",
            ConnectionStringName = app.Host.ConnectionStringName,
        });

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.EditAgentAsync(new AiAgentConfiguration
        {
            Identifier = "ghost-agent", Name = "Renamed", SystemPrompt = "You help.", ConnectionStringName = Host.ConnectionStringName,
        }));

        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Edit_returns_404_for_unknown_slug()
    {
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.EditAgentAsync("nonexistent", new AiAgentConfiguration
        {
            Identifier = "x", Name = "Y", SystemPrompt = "Z", ConnectionStringName = "demo-llm",
        }));

        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Edit_returns_400_when_identifier_missing()
    {
        await using var app = await NewAppAsync();

        // leave Identifier unset (null)
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.EditAgentAsync(new AiAgentConfiguration
        {
            Name = "Renamed", SystemPrompt = "You help.", ConnectionStringName = "demo-llm",
        }));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Raven_rejection_surfaces_the_reason_not_a_pointer_to_the_logs()
    {
        await using var app = await NewAppAsync();

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "bad-query", Name = "Bad Query", SystemPrompt = "You help.",
            ConnectionStringName = app.Host.ConnectionStringName,
            Queries =
            [
                new AiAgentToolQuery
                {
                    Name = "badQuery",
                    Description = "a query that cannot parse",
                    Query = "this is not rql",
                    ParametersSampleObject = "{}",
                },
            ],
        }));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
        Assert.DoesNotContain("see server logs", ex.Body);
        Assert.Contains("FROM", ex.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Edit_returns_400_when_name_missing()
    {
        await using var app = await NewAppAsync();
        var agentId = (await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "support", Name = "Support", SystemPrompt = "You help.",
            ConnectionStringName = app.Host.ConnectionStringName,
        })).AgentId;

        // leave Name unset (null)
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.EditAgentAsync(new AiAgentConfiguration
        {
            Identifier = agentId, SystemPrompt = "You help.", ConnectionStringName = Host.ConnectionStringName,
        }));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }
}
