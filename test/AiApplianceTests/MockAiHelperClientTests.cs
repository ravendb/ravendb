using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.AiAppliance.AiHelper;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

public class MockAiHelperClientTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task SuggestCdc_returns_valid_northwind_config()
    {
        var client = new MockAiHelperClient();

        var result = await client.SuggestCdcAsync(schema: null, samples: null, "anything", CancellationToken.None);

        Assert.Equal(AiHelperStatus.Success, result.Status);
        Assert.NotNull(result.Configuration);

        // The wizard suggest endpoint re-runs this validation before returning the draft.
        Assert.True(
            result.Configuration!.Validate(out var errors, validateName: false, validateConnection: false),
            string.Join("; ", errors));

        Assert.Equal(
            new[] { "Customers", "Orders", "Products" },
            result.Configuration.Tables.Select(t => t.CollectionName).ToArray());
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task SuggestAgent_from_data_returns_three_valid_candidates()
    {
        var client = new MockAiHelperClient();

        var result = await client.SuggestAiAgentAsync(
            new CdcSinkConfiguration(), collectionsSample: null, "from-data", prompt: null, CancellationToken.None);

        Assert.Equal(AiHelperStatus.Success, result.Status);
        Assert.Equal(3, result.Configurations.Count);
        Assert.All(result.Configurations, agent =>
        {
            Assert.False(string.IsNullOrWhiteSpace(agent.Name));
            Assert.False(string.IsNullOrWhiteSpace(agent.SystemPrompt));
        });
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task SuggestAgent_from_prompt_returns_single_candidate_reflecting_prompt()
    {
        var client = new MockAiHelperClient();
        const string prompt = "help customers track their orders";

        var result = await client.SuggestAiAgentAsync(
            new CdcSinkConfiguration(), collectionsSample: null, "from-prompt", prompt, CancellationToken.None);

        Assert.Equal(AiHelperStatus.Success, result.Status);
        var agent = Assert.Single(result.Configurations);
        Assert.False(string.IsNullOrWhiteSpace(agent.Name));
        Assert.Contains(prompt, agent.SystemPrompt);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Suggested_agents_provision_against_a_real_server()
    {
        // The suggest endpoint only checks structural validity (Name + SystemPrompt), so a mock the
        // server's ValidateConfiguration rejects would still pass review and only fail at provision
        // time. Round-trip every suggested candidate through store.AI.CreateAgentAsync (the same call
        // AiAgentRegistrar makes) to guard two regressions: the "defined on both the agent level and
        // the query level" rejection, and the missing-SampleObject gap.
        using var store = GetDocumentStore();

        await store.Maintenance.SendAsync(new PutConnectionStringOperation<AiConnectionString>(
            new AiConnectionString
            {
                Name = "demo-llm",
                ModelType = AiModelType.Chat,
                // localhost Ollama is never contacted: agent creation only validates config structure.
                OllamaSettings = new OllamaSettings("http://localhost:11434/", "llama3.1"),
            }));

        var mock = new MockAiHelperClient();
        var dataMode = await mock.SuggestAiAgentAsync(
            new CdcSinkConfiguration(), collectionsSample: null, "from-data", prompt: null, CancellationToken.None);
        var promptMode = await mock.SuggestAiAgentAsync(
            new CdcSinkConfiguration(), collectionsSample: null, "from-prompt", "help shoppers find orders", CancellationToken.None);

        var candidates = dataMode.Configurations.Concat(promptMode.Configurations).ToList();
        Assert.NotEmpty(candidates);

        foreach (var candidate in candidates)
        {
            candidate.ConnectionStringName = "demo-llm";

            // Must not throw: before the NorthwindSampleConfigs fix this raised a RavenException
            // ("Parameter customerId is defined on both the agent level and the query level ...").
            var result = await store.AI.CreateAgentAsync(candidate);
            Assert.False(string.IsNullOrEmpty(result.Identifier));
        }

        var agents = await store.AI.GetAgentsAsync();

        // Every candidate persisted (distinct identifiers, no accidental upsert collisions) ...
        Assert.Equal(candidates.Count, agents.AiAgents.Count());

        // ... and each carries its own non-empty SampleObject rather than relying on the appliance's
        // provision-time {"reply":""} fallback.
        Assert.All(agents.AiAgents, agent => Assert.False(string.IsNullOrWhiteSpace(agent.SampleObject)));
    }
}
