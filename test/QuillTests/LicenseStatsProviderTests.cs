using System.Text.Json;
using FastTests;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Quill.AiHelper;
using Raven.Quill.Contracts;
using Raven.Quill.Licensing;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class LicenseStatsProviderTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Rows_for_one_database_are_merged_into_one()
    {
        // The license server reports one row per period; they collapse into a single row per database.
        var usage = await GetUsageAsync(
            Row("t2", "quill-config", 20),
            Row("t2", "quill-config", 21));

        var merged = Assert.Single(usage.PerApplication);
        Assert.Equal(41, merged.Usage);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Same_named_databases_on_different_appliances_stay_separate()
    {
        // One license can cover several appliances, and re-provisioning one reports under a fresh
        // topology id - so the same name arrives many times over. Only the topology id tells them apart.
        var usage = await GetUsageAsync(
            Row("t1", "huetopia", 40),
            Row("t2", "huetopia", 12),
            Row("t3", "support-copilot", 5200));

        Assert.Equal(3, usage.PerApplication.Count);
        Assert.Equal(
            ["t1", "t2"],
            usage.PerApplication.Where(a => a.ApplicationName == "huetopia").Select(a => a.TopologyId).Order());
        Assert.Equal(5252, usage.PerApplication.Sum(a => a.Usage));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task An_unparseable_usage_body_yields_an_empty_response()
    {
        var provider = new LicenseStatsProvider(new StubAiHelperClient("not json"));

        var usage = await provider.GetUsageAsync(2026, month: 6, day: null, CancellationToken.None);

        Assert.Empty(usage.PerApplication);
        Assert.Empty(usage.ByPeriod);
    }

    private static async Task<QuillUsageResponse> GetUsageAsync(params object[] perApplication)
    {
        var payload = JsonSerializer.Serialize(new { PerApplication = perApplication, ByPeriod = Array.Empty<object>() });
        var provider = new LicenseStatsProvider(new StubAiHelperClient(payload));

        return await provider.GetUsageAsync(2026, month: 6, day: null, CancellationToken.None);
    }

    private static object Row(string topologyId, string applicationName, long usage) => new
    {
        TopologyId = topologyId,
        ApplicationName = applicationName,
        From = "2026-06-01T00:00:00Z",
        To = "2026-06-30T23:59:59Z",
        Usage = usage,
    };

    /// Answers every upstream call with one canned body, so the test drives only the projection.
    private sealed class StubAiHelperClient(string content) : IAiHelperClient
    {
        public Task<(AiHelperStatus Transport, string Content)> SendAsync(string path, string method, object request, CancellationToken ct) =>
            Task.FromResult((AiHelperStatus.Success, content));

        public Task<T> DeserializeAsync<T>(string json, CancellationToken ct) where T : class
        {
            try
            {
                return Task.FromResult(JsonSerializer.Deserialize<T>(json, new JsonSerializerOptions { PropertyNameCaseInsensitive = true })!);
            }
            catch (JsonException)
            {
                return Task.FromResult<T>(null!);
            }
        }

        public Task<SuggestCdcInternalResult> SuggestCdcAsync(object? schema, object? samples, string prompt, CancellationToken ct) =>
            throw new NotSupportedException();

        public Task<SuggestAiAgentInternalResult> SuggestAiAgentAsync(CdcSinkConfiguration cdcConfig, object? collectionsSample, string mode, string? prompt, CancellationToken ct) =>
            throw new NotSupportedException();

        public Task<HttpResponseMessage> SendChatAsync(string message, string? conversationId, CancellationToken ct) =>
            throw new NotSupportedException();

        public Task<AiHelperStatus> CheckConsentAsync(CancellationToken ct) => throw new NotSupportedException();

        public Task<AiHelperStatus> GiveConsentAsync(CancellationToken ct) => throw new NotSupportedException();
    }
}
