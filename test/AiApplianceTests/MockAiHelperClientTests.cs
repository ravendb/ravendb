using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.AiAppliance.AiHelper;
using Raven.Client.Documents.Operations.CdcSink;
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
}
