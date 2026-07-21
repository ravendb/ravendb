using FastTests;
using Raven.Quill.Metrics;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

/// <summary>
/// Pure coverage for <see cref="UsagePeriod"/>'s clamping of <c>End</c> and <c>Buckets</c> to a
/// reference "now": the current period stops at the present — no future entries, no empty trailing
/// buckets — while a fully past period stays unclamped.
/// </summary>
public class UsagePeriodTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public void Current_month_clamps_end_and_buckets_to_now()
    {
        var now = new DateTime(2026, 6, 15, 10, 0, 0, DateTimeKind.Utc);
        var period = new UsagePeriod(2026, 6, null, nowUtc: now);   // June, viewed on the 15th

        Assert.Equal(now, period.End);                             // clamped to now, not July 1

        var buckets = period.Buckets();
        Assert.Equal(15, buckets.Count);                          // days 1..15 only, no future days
        Assert.Equal(new DateTime(2026, 6, 15, 0, 0, 0, DateTimeKind.Utc), buckets[^1]);

        Assert.Equal(14, period.IndexOf(new DateTime(2026, 6, 15, 9, 0, 0, DateTimeKind.Utc)));  // today's bucket kept
        Assert.Equal(-1, period.IndexOf(new DateTime(2026, 6, 20, 0, 0, 0, DateTimeKind.Utc)));  // future day dropped
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Current_day_clamps_hourly_buckets_to_now()
    {
        var now = new DateTime(2026, 6, 15, 10, 30, 0, DateTimeKind.Utc);
        var period = new UsagePeriod(2026, 6, 15, nowUtc: now);    // June 15, viewed at 10:30

        var buckets = period.Buckets();
        Assert.Equal(11, buckets.Count);                          // hours 0..10 (10:00 has started)
        Assert.Equal(new DateTime(2026, 6, 15, 10, 0, 0, DateTimeKind.Utc), buckets[^1]);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public void Past_month_is_not_clamped()
    {
        var now = new DateTime(2026, 8, 1, 0, 0, 0, DateTimeKind.Utc);
        var period = new UsagePeriod(2026, 6, null, nowUtc: now);   // June, viewed in August

        Assert.Equal(new DateTime(2026, 7, 1, 0, 0, 0, DateTimeKind.Utc), period.End);  // full month
        Assert.Equal(30, period.Buckets().Count);                 // all of June
    }
}
