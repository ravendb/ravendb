using FastTests;
using Raven.Quill.Metrics;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class UsagePeriodTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public void Current_month_clamps_end_and_buckets_to_now()
    {
        var now = new DateTime(2026, 6, 15, 10, 0, 0, DateTimeKind.Utc);
        var period = new UsagePeriod(2026, 6, null, nowUtc: now);

        Assert.Equal(now, period.End);

        var buckets = period.Buckets();
        Assert.Equal(15, buckets.Count);
        Assert.Equal(new DateTime(2026, 6, 15, 0, 0, 0, DateTimeKind.Utc), buckets[^1]);

        Assert.Equal(14, period.IndexOf(new DateTime(2026, 6, 15, 9, 0, 0, DateTimeKind.Utc)));
        Assert.Equal(-1, period.IndexOf(new DateTime(2026, 6, 20, 0, 0, 0, DateTimeKind.Utc)));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Current_day_clamps_hourly_buckets_to_now()
    {
        var now = new DateTime(2026, 6, 15, 10, 30, 0, DateTimeKind.Utc);
        var period = new UsagePeriod(2026, 6, 15, nowUtc: now);

        var buckets = period.Buckets();
        Assert.Equal(11, buckets.Count);                          // hours 0..10 = 11 (10:00 has started)
        Assert.Equal(new DateTime(2026, 6, 15, 10, 0, 0, DateTimeKind.Utc), buckets[^1]);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Past_month_is_not_clamped()
    {
        var now = new DateTime(2026, 8, 1, 0, 0, 0, DateTimeKind.Utc);
        var period = new UsagePeriod(2026, 6, null, nowUtc: now);

        Assert.Equal(new DateTime(2026, 7, 1, 0, 0, 0, DateTimeKind.Utc), period.End);
        Assert.Equal(30, period.Buckets().Count);
    }
}
