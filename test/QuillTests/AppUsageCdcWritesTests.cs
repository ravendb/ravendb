using FastTests;
using Raven.Quill.Cdc;
using Raven.Quill.Contracts;
using Raven.Quill.Metrics;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class AppUsageCdcWritesTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    // fixed "now" after June 2026 so UsagePeriod doesn't clamp these historical periods
    private static readonly DateTime AfterPeriod = new(2026, 7, 1, 0, 0, 0, DateTimeKind.Utc);

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
        var period = new UsagePeriod(2026, 6, 25, nowUtc: AfterPeriod);
        var buckets = period.Buckets();

        var raw = Raw(
            new CdcPerfBatchRaw { Id = 1, Started = start.AddMinutes(10), Completed = start.AddMinutes(12), NumberOfProcessedMessages = 5 },
            new CdcPerfBatchRaw { Id = 2, Started = start.AddMinutes(40), Completed = start.AddMinutes(41), NumberOfProcessedMessages = 3 },
            new CdcPerfBatchRaw { Id = 3, Started = start.AddHours(2).AddMinutes(5), Completed = start.AddHours(2).AddMinutes(6), NumberOfProcessedMessages = 7 });

        var points = MetricsReadService.BuildCdcWrites(raw, buckets, period);

        Assert.Equal(24, points.Length);
        Assert.Equal(8, points[0].Writes);
        Assert.Equal(0, points[1].Writes);
        Assert.Equal(7, points[2].Writes);
        Assert.Equal("2026-06-25T00:00", points[0].T);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void BuildCdcWrites_attributes_running_batch_by_start_and_ignores_out_of_window()
    {
        var start = new DateTime(2026, 6, 25, 0, 0, 0, DateTimeKind.Utc);
        var period = new UsagePeriod(2026, 6, 25, nowUtc: AfterPeriod);
        var buckets = period.Buckets();

        var raw = Raw(
            new CdcPerfBatchRaw { Id = 1, Started = start.AddMinutes(30), Completed = null, NumberOfProcessedMessages = 4 },
            new CdcPerfBatchRaw { Id = 2, Started = start.AddHours(-5), Completed = start.AddHours(-5), NumberOfProcessedMessages = 99 });

        var points = MetricsReadService.BuildCdcWrites(raw, buckets, period);

        Assert.Equal(24, points.Length);
        Assert.Equal(4, points[0].Writes);
        Assert.Equal(0, points[1].Writes);
        Assert.Equal(4, points.Sum(p => p.Writes));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void BuildCdcWrites_returns_all_zero_series_when_no_batches()
    {
        var period = new UsagePeriod(2026, 6, null, nowUtc: AfterPeriod);
        var buckets = period.Buckets();

        var points = MetricsReadService.BuildCdcWrites(new CdcSinkPerformanceRaw(), buckets, period);

        Assert.Equal(buckets.Count, points.Length);
        Assert.All(points, p => Assert.Equal(0, p.Writes));
    }
}
