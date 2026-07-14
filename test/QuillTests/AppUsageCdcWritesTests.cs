using Raven.Quill.Cdc;
using Raven.Quill.Contracts;
using Raven.Quill.Metrics;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

/// <summary>
/// Pure coverage for <see cref="MetricsReadService.BuildCdcWrites"/> — the fold from RavenDB's
/// rolling per-batch CDC perf window into the App Usage <c>cdcWrites</c> series. Verifiable
/// without a live CDC source (mirrors <see cref="CdcPerformanceEndpointTests"/>'s Shape tests);
/// the populated end-to-end path needs a real source (the gated Postgres E2E lane).
/// </summary>
public class AppUsageCdcWritesTests
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
    public void BuildCdcWrites_buckets_processed_messages_by_completion()
    {
        var start = new DateTime(2026, 6, 25, 0, 0, 0, DateTimeKind.Utc);
        var period = new UsagePeriod(2026, 6, 25);   // a specific day → 24 hourly buckets
        var buckets = period.Buckets();

        var raw = Raw(
            new CdcPerfBatchRaw { Id = 1, Started = start.AddMinutes(10), Completed = start.AddMinutes(12), NumberOfProcessedMessages = 5 },
            new CdcPerfBatchRaw { Id = 2, Started = start.AddMinutes(40), Completed = start.AddMinutes(41), NumberOfProcessedMessages = 3 },  // same (00:00) bucket
            new CdcPerfBatchRaw { Id = 3, Started = start.AddHours(2).AddMinutes(5), Completed = start.AddHours(2).AddMinutes(6), NumberOfProcessedMessages = 7 });

        var points = MetricsReadService.BuildCdcWrites(raw, buckets, period);

        Assert.Equal(24, points.Length);
        Assert.Equal(8, points[0].Writes);   // 5 + 3 in the first hour
        Assert.Equal(0, points[1].Writes);
        Assert.Equal(7, points[2].Writes);   // third hour
        Assert.Equal("2026-06-25T00:00", points[0].T);  // hourly bucket label
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void BuildCdcWrites_attributes_running_batch_by_start_and_ignores_out_of_window()
    {
        var start = new DateTime(2026, 6, 25, 0, 0, 0, DateTimeKind.Utc);
        var period = new UsagePeriod(2026, 6, 25);   // a specific day → 24 hourly buckets
        var buckets = period.Buckets();

        var raw = Raw(
            new CdcPerfBatchRaw { Id = 1, Started = start.AddMinutes(30), Completed = null, NumberOfProcessedMessages = 4 },  // running → bucket by Started
            new CdcPerfBatchRaw { Id = 2, Started = start.AddHours(-5), Completed = start.AddHours(-5), NumberOfProcessedMessages = 99 });  // previous day → ignored

        var points = MetricsReadService.BuildCdcWrites(raw, buckets, period);

        Assert.Equal(24, points.Length);
        Assert.Equal(4, points[0].Writes);  // the running batch's Started falls in bucket 0
        Assert.Equal(0, points[1].Writes);
        Assert.Equal(4, points.Sum(p => p.Writes));  // only the running batch counted; the pre-day 99 was dropped
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void BuildCdcWrites_returns_all_zero_series_when_no_batches()
    {
        var period = new UsagePeriod(2026, 6, null);   // a month → every day of June
        var buckets = period.Buckets();

        var points = MetricsReadService.BuildCdcWrites(new CdcSinkPerformanceRaw(), buckets, period);

        Assert.Equal(buckets.Count, points.Length);
        Assert.All(points, p => Assert.Equal(0, p.Writes));
    }
}
