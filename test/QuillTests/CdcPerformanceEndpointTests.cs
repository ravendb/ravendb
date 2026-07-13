using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Raven.Quill.Auth;
using Raven.Quill.Cdc;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

/// <summary>
/// Coverage for the CDC performance snapshot — the pure <see cref="CdcPerformanceShaper"/>
/// folding (verifiable without a live CDC source) and the
/// <c>GET /api/apps/{slug}/cdc/performance</c> endpoint's not-configured / auth behaviour.
/// The populated end-to-end path depends on the server collecting CDC stats (RavenDB-26780 /
/// ravendb#23046) plus a live source, exercised in the gated Postgres E2E lane.
/// </summary>
public class CdcPerformanceEndpointTests(ITestOutputHelper output) : ApplianceMetricsTestBase(output)
{
    private static CdcSinkPerformanceRaw Raw(params CdcPerfBatchRaw[] batches) =>
        new()
        {
            Results =
            [
                new CdcPerfTaskRaw
                {
                    TaskId = 1,
                    TaskName = "cdc",
                    Stats = [new CdcPerfProcessRaw { Performance = [.. batches] }],
                },
            ],
        };

    [RavenFact(RavenTestCategory.Quill)]
    public void Shape_aggregates_recent_activity_and_marks_active()
    {
        var now = new DateTime(2026, 6, 25, 12, 0, 0, DateTimeKind.Utc);
        var raw = Raw(
            new CdcPerfBatchRaw { Id = 1, Started = now.AddMinutes(-5), Completed = now.AddMinutes(-5).AddSeconds(2), NumberOfReadMessages = 10, NumberOfProcessedMessages = 8 },
            new CdcPerfBatchRaw { Id = 2, Started = now.AddSeconds(-30), Completed = null, NumberOfReadMessages = 4, NumberOfProcessedMessages = 2 });

        var snap = CdcPerformanceShaper.Shape(raw, configured: true, disabled: false, now);

        Assert.True(snap.Enabled);
        Assert.Equal("active", snap.Status);          // one batch still in-progress
        Assert.Equal(14, snap.RecentReads);
        Assert.Equal(10, snap.RecentWrites);
        Assert.Equal(0, snap.ErrorCount);
        Assert.Equal(2, snap.RecentBatches.Length);
        Assert.NotNull(snap.LastSyncAt);
        Assert.Equal(DateTimeKind.Utc, snap.LastSyncAt!.Value.Kind);   // serializes with Z
        Assert.Equal(now.AddMinutes(-5).AddSeconds(2), snap.LastSyncAt);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Shape_idle_when_last_batch_is_old()
    {
        var now = new DateTime(2026, 6, 25, 12, 0, 0, DateTimeKind.Utc);
        var raw = Raw(new CdcPerfBatchRaw { Id = 1, Started = now.AddMinutes(-10), Completed = now.AddMinutes(-10), NumberOfProcessedMessages = 5 });

        var snap = CdcPerformanceShaper.Shape(raw, configured: true, disabled: false, now);

        Assert.Equal("idle", snap.Status);
        Assert.Equal(600, snap.LagSeconds);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Shape_reflects_config_state_and_errors()
    {
        var now = new DateTime(2026, 6, 25, 12, 0, 0, DateTimeKind.Utc);

        var notConfigured = CdcPerformanceShaper.Shape(new CdcSinkPerformanceRaw(), configured: false, disabled: false, now);
        Assert.False(notConfigured.Enabled);
        Assert.Equal("not-configured", notConfigured.Status);

        var disabled = CdcPerformanceShaper.Shape(new CdcSinkPerformanceRaw(), configured: true, disabled: true, now);
        Assert.False(disabled.Enabled);
        Assert.Equal("disabled", disabled.Status);

        var withError = CdcPerformanceShaper.Shape(
            Raw(new CdcPerfBatchRaw { Id = 1, Started = now.AddSeconds(-10), Completed = now.AddSeconds(-10), ScriptProcessingErrorCount = 1 }),
            configured: true, disabled: false, now);
        Assert.Equal("error", withError.Status);
        Assert.Equal(1, withError.ErrorCount);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task CdcPerformance_returns_not_configured_snapshot_when_no_cdc()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var json = await client.GetFromJsonAsync<JsonElement>("/api/apps/my-app/cdc/performance");
        Assert.False(json.GetProperty("enabled").GetBoolean());
        Assert.Equal("not-configured", json.GetProperty("status").GetString());
        Assert.Equal(0, json.GetProperty("recentWrites").GetInt64());
        Assert.Equal(0, json.GetProperty("recentBatches").GetArrayLength());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task CdcPerformance_requires_authentication()
    {
        var store = GetDocumentStore();
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        client.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);

        var resp = await client.GetAsync("/api/apps/my-app/cdc/performance");
        Assert.Equal(HttpStatusCode.Unauthorized, resp.StatusCode);
    }
}
