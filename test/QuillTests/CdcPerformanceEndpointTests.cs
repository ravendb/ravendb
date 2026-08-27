using System.Net;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Quill.Auth;
using Raven.Quill.Cdc;
using Raven.Quill.Contracts;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class CdcPerformanceEndpointTests(ITestOutputHelper output) : QuillTestBase(output)
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

        var snap = CdcPerformanceShaper.Shape(raw, disabled: false, now, lastActivityAt: now.AddMinutes(-2));

        Assert.True(snap.Enabled);
        Assert.Equal("active", snap.Status);
        Assert.Equal(14, snap.RecentReads);
        Assert.Equal(10, snap.RecentWrites);
        Assert.Equal(0, snap.ErrorCount);
        Assert.Equal(2, snap.RecentBatches.Length);
        Assert.NotNull(snap.LastSyncAt);
        Assert.Equal(DateTimeKind.Utc, snap.LastSyncAt!.Value.Kind);
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

        // empty batches → idle; "not-configured" is an endpoint concern, not inferred by Shape
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

    /// A source the sink cannot even reach never produces a batch, so the rolling window stays clean
    /// while the persisted store fills up. Reading only the window is what let a dead task look idle.
    [RavenFact(RavenTestCategory.Quill)]
    public void Shape_reports_errors_that_never_reached_a_batch()
    {
        var now = new DateTime(2026, 6, 25, 12, 0, 0, DateTimeKind.Utc);

        var snap = CdcPerformanceShaper.Shape(
            new CdcSinkPerformanceRaw(), disabled: false, now, lastActivityAt: null, storedErrorCount: 3);

        Assert.Equal("error", snap.Status);
        Assert.Equal(3, snap.ErrorCount);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Shape_keeps_the_larger_of_the_batch_and_stored_error_counts()
    {
        var now = new DateTime(2026, 6, 25, 12, 0, 0, DateTimeKind.Utc);
        var raw = Raw(new CdcPerfBatchRaw { Id = 1, Started = now.AddSeconds(-10), Completed = now.AddSeconds(-10), ScriptProcessingErrorCount = 5 });

        // the same failure is counted in both places, so summing them would double-report it
        var snap = CdcPerformanceShaper.Shape(raw, disabled: false, now, lastActivityAt: null, storedErrorCount: 2);

        Assert.Equal(5, snap.ErrorCount);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Shape_leaves_a_paused_task_paused_whatever_it_recorded_before()
    {
        var now = new DateTime(2026, 6, 25, 12, 0, 0, DateTimeKind.Utc);

        var snap = CdcPerformanceShaper.Shape(
            new CdcSinkPerformanceRaw(), disabled: true, now, lastActivityAt: null, storedErrorCount: 4);

        Assert.Equal("disabled", snap.Status);
    }

    /// The report that prompted this: the sink could not reach its source at all, so /cdc/errors filled
    /// while /cdc/performance answered "idle, 0 errors" next to a dialog listing the failures.
    [RavenFact(RavenTestCategory.Quill)]
    public async Task CdcPerformance_reports_the_failures_the_error_list_holds()
    {
        await using var app = await NewAppAsync();

        var errors = await WaitForCdcErrorsAsync(app.Slug);

        var performance = await Host.GetCdcPerformanceAsync(app.Slug);

        Assert.Equal("error", performance.Status);
        Assert.True(performance.ErrorCount >= errors.Count,
            $"performance reported {performance.ErrorCount} errors, the list holds {errors.Count}");
    }

    private async Task<IReadOnlyList<CdcError>> WaitForCdcErrorsAsync(string slug)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (true)
        {
            var errors = await Host.GetCdcErrorsAsync(slug);
            if (errors.Count > 0)
                return errors;

            if (DateTime.UtcNow > deadline)
                throw new TimeoutException($"no CDC error recorded for '{slug}' within 30s");

            await Task.Delay(200);
        }
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
        Assert.Equal("orders/1", errors[0].DocumentId);
        Assert.Null(errors[0].AffectedDocumentsCount);
        Assert.Equal("Transformation", errors[0].Step);
        Assert.Equal("bad row", errors[0].Error);
        // input was Unspecified → normalized to Utc (serializes with Z)
        Assert.Equal(DateTimeKind.Utc, errors[0].CreatedAt.Kind);

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
        Assert.Equal("e39", errors[0].Error);
        Assert.Equal("e15", errors[24].Error);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task CdcPerformance_returns_404_when_no_cdc()
    {
        await using var app = await NewAppAsync();

        var r = await app.Store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation($"{app.Slug}-cdc", OngoingTaskType.CdcSink));
        await app.Store.Maintenance.SendAsync(new DeleteOngoingTaskOperation(r.TaskId, OngoingTaskType.CdcSink));

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.GetCdcPerformanceAsync());
        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task CdcPerformance_requires_authentication()
    {
        // fresh client (not shared Host.Client) so other tests unaffected; auth runs before app lookup
        using var client = Host.Factory.CreateClient();
        client.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);

        var resp = await client.GetAsync(QuillRoutes.AppCdcPerformance("my-app"));
        Assert.Equal(HttpStatusCode.Unauthorized, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task CdcErrors_returns_empty_list_when_no_cdc()
    {
        await using var app = await NewAppAsync();

        var r = await app.Store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation($"{app.Slug}-cdc", OngoingTaskType.CdcSink));
        await app.Store.Maintenance.SendAsync(new DeleteOngoingTaskOperation(r.TaskId, OngoingTaskType.CdcSink));

        var errors = await app.GetCdcErrorsAsync();
        Assert.Empty(errors);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task CdcErrors_returns_404_for_unknown_app()
    {
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.GetCdcErrorsAsync("does-not-exist"));
        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }
}
