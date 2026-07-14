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

        // Durable totals from the persisted @cdc-states doc are passed through unchanged and normalized to UTC.
        var snap = CdcPerformanceShaper.Shape(raw, disabled: false, now, lastActivityAt: now.AddMinutes(-2));

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

        var snap = CdcPerformanceShaper.Shape(raw, disabled: false, now, lastActivityAt: null);

        Assert.Equal("idle", snap.Status);
        Assert.Equal(600, snap.LagSeconds);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Shape_reflects_config_state_and_errors()
    {
        var now = new DateTime(2026, 6, 25, 12, 0, 0, DateTimeKind.Utc);

        // A configured-but-idle sink (no batches yet) shapes as enabled/idle. "not-configured"
        // is an endpoint concern (no CDC task at all → CdcPerformanceShaper.NotConfigured()),
        // not something Shape infers from an empty batch window.
        var idle = CdcPerformanceShaper.Shape(new CdcSinkPerformanceRaw(), disabled: false, now, lastActivityAt: null);
        Assert.True(idle.Enabled);
        Assert.Equal("idle", idle.Status);

        var disabled = CdcPerformanceShaper.Shape(new CdcSinkPerformanceRaw(), disabled: true, now, lastActivityAt: null);
        Assert.False(disabled.Enabled);
        Assert.Equal("disabled", disabled.Status);

        var withError = CdcPerformanceShaper.Shape(
            Raw(new CdcPerfBatchRaw { Id = 1, Started = now.AddSeconds(-10), Completed = now.AddSeconds(-10), ScriptProcessingErrorCount = 1 }),
            disabled: false, now, lastActivityAt: null);
        Assert.Equal("error", withError.Status);
        Assert.Equal(1, withError.ErrorCount);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void ShapeErrors_flattens_process_and_item_errors_newest_first()
    {
        var t0 = new DateTime(2026, 6, 25, 12, 0, 0, DateTimeKind.Unspecified);
        var raw = new CdcSinkErrorsRaw
        {
            Results =
            [
                new CdcTaskErrorsRaw
                {
                    TaskName = "cdc",
                    ProcessErrors = [new CdcTaskErrorRaw { TaskName = "cdc", CreatedAt = t0, Step = "Load", Error = "load failed", AffectedDocumentsCount = 7 }],
                    ItemErrors = [new CdcTaskErrorRaw { TaskName = "cdc", CreatedAt = t0.AddMinutes(5), Step = "Transformation", Error = "bad row", DocumentId = "orders/1" }],
                },
            ],
        };

        var errors = CdcPerformanceShaper.ShapeErrors(raw);

        Assert.Equal(2, errors.Length);
        // Newest first: the item error (t0+5m) precedes the process error (t0).
        Assert.Equal("orders/1", errors[0].DocumentId);
        Assert.Null(errors[0].AffectedDocumentsCount);
        Assert.Equal("Transformation", errors[0].Step);
        Assert.Equal("bad row", errors[0].Error);
        Assert.Equal(DateTimeKind.Utc, errors[0].CreatedAt.Kind);   // normalized so it serializes with Z

        Assert.Null(errors[1].DocumentId);
        Assert.Equal(7, errors[1].AffectedDocumentsCount);
        Assert.Equal("Load", errors[1].Step);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void ShapeErrors_is_empty_for_empty_store()
    {
        Assert.Empty(CdcPerformanceShaper.ShapeErrors(new CdcSinkErrorsRaw()));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void ShapeErrors_caps_at_25()
    {
        var t0 = new DateTime(2026, 6, 25, 12, 0, 0, DateTimeKind.Utc);
        var items = new List<CdcTaskErrorRaw>();
        for (var i = 0; i < 40; i++)
            items.Add(new CdcTaskErrorRaw { TaskName = "cdc", CreatedAt = t0.AddSeconds(i), Step = "Transformation", Error = $"e{i}" });
        var raw = new CdcSinkErrorsRaw { Results = [new CdcTaskErrorsRaw { TaskName = "cdc", ItemErrors = items }] };

        var errors = CdcPerformanceShaper.ShapeErrors(raw);

        Assert.Equal(25, errors.Length);
        Assert.Equal("e39", errors[0].Error);   // newest kept
        Assert.Equal("e15", errors[24].Error);  // oldest 15 dropped
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task CdcPerformance_returns_404_when_no_cdc()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        // No CDC sink configured on the app → 404 (no snapshot to shape).
        var resp = await client.GetAsync("/api/apps/my-app/cdc/performance");
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
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

    [RavenFact(RavenTestCategory.Quill)]
    public async Task CdcErrors_returns_empty_list_when_no_cdc()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var json = await client.GetFromJsonAsync<JsonElement>("/api/apps/my-app/cdc/errors");
        Assert.Equal(JsonValueKind.Array, json.ValueKind);
        Assert.Equal(0, json.GetArrayLength());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task CdcErrors_returns_404_for_unknown_app()
    {
        var store = GetDocumentStore();
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync("/api/apps/does-not-exist/cdc/errors");
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }
}
