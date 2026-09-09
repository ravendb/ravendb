using System.Text.Json;
using FastTests;
using Microsoft.Extensions.Options;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Quill.AiHelper;
using Raven.Quill.Contracts;
using Raven.Quill.Hosting;
using Raven.Quill.Licensing;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class LicenseStatsProviderTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Config_database_row_is_flagged_as_system_by_the_configured_name()
    {
        // The config database name is configurable, so the flag must follow ApplianceOptions - not the
        // "quill-config" default. Here that default belongs to a *user* app and must stay unflagged.
        var usage = await GetUsageAsync("acme-config",
            Row("t1", "support-copilot", 5200),
            Row("t2", "acme-config", 41),
            Row("t3", "quill-config", 900));

        Assert.False(Row(usage, "support-copilot").IsSystem);
        Assert.True(Row(usage, "acme-config").IsSystem);
        Assert.False(Row(usage, "quill-config").IsSystem);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task System_row_keeps_its_usage_and_stays_in_the_list()
    {
        // The license server charges the config database like any other, so dropping the row would leave
        // the per-app rows short of the total shown beside them. It is labelled, never removed.
        var usage = await GetUsageAsync("quill-config",
            Row("t1", "support-copilot", 5200),
            Row("t2", "quill-config", 41));

        Assert.Equal(2, usage.PerApplication.Count);
        Assert.Equal(41, Row(usage, "quill-config").Usage);
        Assert.Equal(5241, usage.PerApplication.Sum(a => a.Usage));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task System_row_is_matched_regardless_of_case()
    {
        // Database names are case-insensitive in RavenDB, so a differently-cased report still matches.
        var usage = await GetUsageAsync("Quill-Config", Row("t1", "quill-config", 41));

        Assert.True(Row(usage, "quill-config").IsSystem);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Every_appliance_reporting_under_one_license_is_flagged()
    {
        // One license can cover several appliances, and re-provisioning one reports under a fresh
        // topology id - so the config database's name arrives many times over, and every one of them
        // has to be flagged. Only the topology id tells them apart.
        var usage = await GetUsageAsync("quill-config",
            Row("t1", "quill-config", 40),
            Row("t2", "quill-config", 12),
            Row("t3", "support-copilot", 5200));

        Assert.Equal(3, usage.PerApplication.Count);
        Assert.Equal(
            ["t1", "t2"],
            usage.PerApplication.Where(a => a.IsSystem).Select(a => a.TopologyId).Order());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Rows_for_one_database_are_merged_before_being_flagged()
    {
        // The license server reports one row per period; they collapse into a single row per database,
        // and the merged row is still recognized as the config database.
        var usage = await GetUsageAsync("quill-config",
            Row("t2", "quill-config", 20),
            Row("t2", "quill-config", 21));

        var system = Assert.Single(usage.PerApplication);
        Assert.True(system.IsSystem);
        Assert.Equal(41, system.Usage);
    }

    private static async Task<QuillUsageResponse> GetUsageAsync(string configDatabase, params object[] perApplication)
    {
        var payload = JsonSerializer.Serialize(new { PerApplication = perApplication, ByPeriod = Array.Empty<object>() });
        var provider = new LicenseStatsProvider(
            new StubAiHelperClient(payload),
            Options.Create(new ApplianceOptions { ConfigDatabase = configDatabase }));

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

    private static QuillApplicationUsage Row(QuillUsageResponse usage, string applicationName) =>
        Assert.Single(usage.PerApplication, a => a.ApplicationName == applicationName);

    /// Answers every upstream call with one canned body, so the test drives only the projection.
    private sealed class StubAiHelperClient(string content) : IAiHelperClient
    {
        public Task<(AiHelperStatus Transport, string Content)> SendAsync(string path, string method, object request, CancellationToken ct) =>
            Task.FromResult((AiHelperStatus.Success, content));

        public Task<T> DeserializeAsync<T>(string json, CancellationToken ct) where T : class =>
            Task.FromResult(JsonSerializer.Deserialize<T>(json, new JsonSerializerOptions { PropertyNameCaseInsensitive = true })!);

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
