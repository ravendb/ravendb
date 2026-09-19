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
    public async Task Quiet_hours_are_reported_as_zero_buckets()
    {
        // The license server only reports hours that saw writes; the chart needs every hour of the
        // day so quiet ones show as empty bars rather than disappearing.
        var usage = await GetUsageForDayAsync(2026, 6, 15,
            Period("2026-06-15T06:00:00Z", "2026-06-15T07:00:00Z", 1),
            Period("2026-06-15T07:00:00Z", "2026-06-15T08:00:00Z", 5),
            Period("2026-06-15T09:00:00Z", "2026-06-15T10:00:00Z", 12));

        Assert.Equal(24, usage.ByPeriod.Count);
        Assert.Equal(Enumerable.Range(0, 24).Select(h => new DateTime(2026, 6, 15, h, 0, 0, DateTimeKind.Utc)),
            usage.ByPeriod.Select(p => p.From));
        Assert.Equal(new DateTime(2026, 6, 15, 7, 0, 0, DateTimeKind.Utc), usage.ByPeriod[6].To);
        Assert.Equal(1, usage.ByPeriod[6].Usage);
        Assert.Equal(5, usage.ByPeriod[7].Usage);
        Assert.Equal(0, usage.ByPeriod[8].Usage);
        Assert.Equal(12, usage.ByPeriod[9].Usage);
        Assert.Equal(18, usage.ByPeriod.Sum(p => p.Usage));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Rows_outside_the_requested_period_are_dropped()
    {
        var usage = await GetUsageForDayAsync(2026, 6, 15,
            Period("2026-06-14T23:00:00Z", "2026-06-15T00:00:00Z", 7),
            Period("2026-06-15T03:00:00Z", "2026-06-15T04:00:00Z", 2));

        Assert.Equal(24, usage.ByPeriod.Count);
        Assert.Equal(2, usage.ByPeriod.Sum(p => p.Usage));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Upstream_failure_throws_instead_of_reporting_zero_usage()
    {
        // A non-2xx from the license server carries an error body that still deserializes into an empty
        // response, which would otherwise render as a full period of zero writes.
        var errorBody = JsonSerializer.Serialize(new { Type = "LicenseLimitException", Message = "not a Quill license" });
        var provider = new LicenseStatsProvider(new StubAiHelperClient(errorBody, AiHelperStatus.InternalError));

        await Assert.ThrowsAsync<LicenseUsageUnavailableException>(() =>
            provider.GetUsageAsync(2026, month: 6, day: 15, CancellationToken.None));
    }

    private static Task<QuillUsageResponse> GetUsageAsync(params object[] perApplication) =>
        GetUsageAsync(perApplication, [], 2026, month: 6, day: null);

    private static Task<QuillUsageResponse> GetUsageForDayAsync(int year, int month, int day, params object[] byPeriod) =>
        GetUsageAsync([], byPeriod, year, month, day);

    private static async Task<QuillUsageResponse> GetUsageAsync(
        object[] perApplication, object[] byPeriod, int year, int? month, int? day)
    {
        var payload = JsonSerializer.Serialize(new { PerApplication = perApplication, ByPeriod = byPeriod });
        var provider = new LicenseStatsProvider(new StubAiHelperClient(payload));

        return await provider.GetUsageAsync(year, month, day, CancellationToken.None);
    }

    private static object Period(string from, string to, long usage) => new { From = from, To = to, Usage = usage };

    private static object Row(string topologyId, string applicationName, long usage) => new
    {
        TopologyId = topologyId,
        ApplicationName = applicationName,
        From = "2026-06-01T00:00:00Z",
        To = "2026-06-30T23:59:59Z",
        Usage = usage,
    };

    /// Answers every upstream call with one canned body, so the test drives only the projection.
    private sealed class StubAiHelperClient(string content, AiHelperStatus transport = AiHelperStatus.Success) : IAiHelperClient
    {
        public Task<(AiHelperStatus Transport, string Content)> SendAsync(string path, string method, object request, CancellationToken ct) =>
            Task.FromResult((transport, content));

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
