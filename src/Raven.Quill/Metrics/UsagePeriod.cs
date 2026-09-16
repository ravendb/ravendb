namespace Raven.Quill.Metrics;

/// <summary>
/// A calendar period selected by how many components the caller supplies — the granularity
/// is implied, so there's no separate "window" flag:
/// <list type="bullet">
///   <item><description><c>year</c> only → the 12 months of that year (monthly buckets).</description></item>
///   <item><description><c>year</c> + <c>month</c> → every day of that month (daily buckets).</description></item>
///   <item><description><c>year</c> + <c>month</c> + <c>day</c> → the 24 hours of that day (hourly buckets).</description></item>
/// </list>
/// Components are clamped/normalized on construction (best-effort, never throws): the year to
/// a valid range, the month to 1-12, the day to the month's length, and a day without a month
/// is dropped (a day is meaningless without one). So a caller can't 400 the endpoint with an
/// out-of-range value.
/// The period never extends past <c>now</c>: <see cref="End"/> and <see cref="Buckets"/> stop at
/// the present, so the current period reports no future entries or empty trailing buckets.
/// </summary>
internal readonly struct UsagePeriod
{
    public int Year { get; }
    public int? Month { get; }
    public int? Day { get; }

    // Reference "now" (UTC) used to clamp End/Buckets to the present. Defaults to the wall clock;
    // tests inject a fixed value for deterministic bucket layouts.
    private readonly DateTime _nowUtc;

    public UsagePeriod(int year, int? month, int? day, DateTime? nowUtc = null)
    {
        _nowUtc = nowUtc ?? DateTime.UtcNow;
        Year = Math.Clamp(year, 1, 9999);
        if (month is null)
        {
            // A day can't be placed without a month, so drop it and fall back to the year view.
            Month = null;
            Day = null;
            return;
        }
        Month = Math.Clamp(month.Value, 1, 12);
        Day = day is null ? null : Math.Clamp(day.Value, 1, DateTime.DaysInMonth(Year, Month.Value));
    }

    private bool Hourly => Day is not null;
    private bool Daily => Day is null && Month is not null;

    /// <summary>Inclusive lower bound of the period (UTC): the day at 00:00 (hourly), the 1st
    /// of the month (daily), or Jan 1 (monthly). Also the lower bound for the metric-row query.</summary>
    public DateTime Start =>
        Hourly ? new DateTime(Year, Month!.Value, Day!.Value, 0, 0, 0, DateTimeKind.Utc) :
        Daily ? new DateTime(Year, Month!.Value, 1, 0, 0, 0, DateTimeKind.Utc) :
        new DateTime(Year, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    /// <summary>Exclusive upper bound: the start of the next period (calendar arithmetic handles
    /// variable month/year lengths), clamped to <c>now</c> so the current period stops at the
    /// present instead of spilling into future buckets.</summary>
    public DateTime End
    {
        get
        {
            var next = Shift(1);
            return next < _nowUtc ? next : _nowUtc;
        }
    }

    /// <summary>Start of the preceding equal period — the baseline for delta-vs-previous.</summary>
    public DateTime PreviousStart => Shift(-1);

    private DateTime Shift(int periods) =>
        Hourly ? Start.AddDays(periods) :
        Daily ? Start.AddMonths(periods) :
        Start.AddYears(periods);

    /// <summary>The contiguous, zero-fillable bucket layout up to <see cref="End"/>: 24 hours, the
    /// month's days, or 12 months — with any bucket starting at or after <c>now</c> dropped, so the
    /// current period carries no empty future buckets.</summary>
    public List<DateTime> Buckets()
    {
        var start = Start;  // local copies: struct lambdas can't capture `this`
        var end = End;
        var all = Hourly ? Enumerable.Range(0, 24).Select(h => start.AddHours(h)) :
            Daily ? Enumerable.Range(0, DateTime.DaysInMonth(Year, Month!.Value)).Select(d => start.AddDays(d)) :
            Enumerable.Range(0, 12).Select(m => start.AddMonths(m));
        return all.Where(b => b < end).ToList();
    }

    /// <summary>Maps a timestamp to its bucket slot (hour-of-day / day-of-month / month-of-year),
    /// or -1 if it falls outside this period so callers drop it.</summary>
    public int IndexOf(DateTime t)
    {
        if (t >= End) return -1;   // at or past now: outside the clamped period, so it has no bucket
        var (idx, count) =
            Hourly ? (t.Date == Start.Date ? t.Hour : -1, 24) :
            Daily ? (t.Year == Year && t.Month == Month ? t.Day - 1 : -1, DateTime.DaysInMonth(Year, Month!.Value)) :
            (t.Year == Year ? t.Month - 1 : -1, 12);
        return idx >= 0 && idx < count ? idx : -1;
    }

    /// <summary>The x-axis label for a bucket: <c>yyyy-MM-ddTHH:00</c> (hourly), <c>yyyy-MM-dd</c>
    /// (daily), or <c>yyyy-MM</c> (monthly).</summary>
    public string Label(DateTime bucketUtc) =>
        Hourly ? bucketUtc.ToString("yyyy-MM-ddTHH:00") :
        Daily ? bucketUtc.ToString("yyyy-MM-dd") :
        bucketUtc.ToString("yyyy-MM");
}
