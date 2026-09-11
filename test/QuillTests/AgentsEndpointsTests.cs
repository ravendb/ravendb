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
    public async Task Provision_persists_the_chat_trimming_thresholds_the_caller_set()
    {
        await using var app = await NewAppAsync();
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "support", Name = "Support", SystemPrompt = "You help.",
            ConnectionStringName = app.Host.ConnectionStringName,
            ChatTrimming = new AiAgentChatTrimmingConfiguration(new AiAgentSummarizationByTokens
            {
                MaxTokensBeforeSummarization = 20_000,
                MaxTokensAfterSummarization = 2_000,
            }),
        });

        var agents = await app.Store.Maintenance.ForDatabase(app.Slug).SendAsync(new GetAiAgentsOperation());
        var tokens = Assert.Single(agents.AiAgents).ChatTrimming.Tokens;
        Assert.Equal(20_000, tokens.MaxTokensBeforeSummarization);
        Assert.Equal(2_000, tokens.MaxTokensAfterSummarization);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_defaults_chat_trimming_to_a_threshold_a_single_turn_can_reach()
    {
        await using var app = await NewAppAsync();
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "support", Name = "Support", SystemPrompt = "You help.",
            ConnectionStringName = app.Host.ConnectionStringName,
        });

        var agents = await app.Store.Maintenance.ForDatabase(app.Slug).SendAsync(new GetAiAgentsOperation());
        var tokens = Assert.Single(agents.AiAgents).ChatTrimming.Tokens;
        Assert.Equal(32 * 1024, tokens.MaxTokensBeforeSummarization);
        Assert.Equal(4 * 1024, tokens.MaxTokensAfterSummarization);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Provision_returns_400_when_the_summary_is_not_smaller_than_the_threshold()
    {
        await using var app = await NewAppAsync();

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "support", Name = "Support", SystemPrompt = "You help.",
            ConnectionStringName = app.Host.ConnectionStringName,
            ChatTrimming = new AiAgentChatTrimmingConfiguration(new AiAgentSummarizationByTokens
            {
                MaxTokensBeforeSummarization = 4_000,
                MaxTokensAfterSummarization = 4_000,
            }),
        }));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
        Assert.Contains("maxTokensAfterSummarization must be smaller", ex.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Agents_list_returns_404_for_unknown_slug()
    {
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.GetAgentsAsync("nonexistent"));
        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }
}
