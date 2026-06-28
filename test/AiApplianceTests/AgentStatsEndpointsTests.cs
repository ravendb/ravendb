using System.Text.Json;
using System.Net.Http.Json;
using Raven.AiAppliance.Metrics;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

/// <summary>
/// Coverage for <c>GET /api/apps/{slug}/agents/stats</c>: the configured-agent
/// count (from the RavenDB AI agent registry) plus windowed usage totals and a
/// per-agent breakdown aggregated from the <see cref="ConversationMetricsIndex"/>.
/// </summary>
public class AgentStatsEndpointsTests(ITestOutputHelper output) : ApplianceMetricsTestBase(output)
{
    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Agent_stats_reports_configured_count_window_totals_and_per_agent_usage()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await new ConversationMetricsIndex().ExecuteAsync(store, database: perAppDb);

        await SeedAgentAsync(store, perAppDb, name: "Support");
        await SeedAgentAsync(store, perAppDb, name: "Sales");

        var now = DateTime.UtcNow;
        await SeedConversationAsync(store, perAppDb, "chats/a", "support", now.AddHours(-1), tokens: 100);
        await SeedConversationAsync(store, perAppDb, "chats/b", "support", now.AddHours(-2), tokens: 200);
        await SeedConversationAsync(store, perAppDb, "chats/c", "sales", now.AddHours(-3), tokens: 50);
        await Indexes.WaitForIndexingAsync(store, perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync("/api/apps/my-app/agents/stats");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(2, json.GetProperty("configuredAgents").GetInt32());
        Assert.Equal(3, json.GetProperty("last24h").GetProperty("conversations").GetInt64());
        Assert.Equal(350, json.GetProperty("last24h").GetProperty("tokens").GetInt64());

        var agents = json.GetProperty("agents");
        var support = FindAgent(agents, "support");
        Assert.Equal(2, support.GetProperty("conversations").GetInt64());
        Assert.Equal(300, support.GetProperty("tokens").GetInt64());
        var sales = FindAgent(agents, "sales");
        Assert.Equal(1, sales.GetProperty("conversations").GetInt64());
        Assert.Equal(50, sales.GetProperty("tokens").GetInt64());
    }

    private static JsonElement FindAgent(JsonElement agents, string agentId)
    {
        foreach (var agent in agents.EnumerateArray())
        {
            if (agent.GetProperty("agentId").GetString() == agentId)
                return agent;
        }

        throw new Xunit.Sdk.XunitException($"agent '{agentId}' not found in {agents}");
    }
}
