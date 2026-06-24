namespace Raven.AiAppliance.Contracts;

/// <summary>
/// Monthly writes-vs-quota for the <c>/settings/usage</c> page — the prototype's
/// <c>getMonthlyWrites({year,month})</c>. MOCK-backed: there is no real per-DB write
/// counter yet (gap #4 / Track-A), so <c>MonthlyUsed</c>/<c>Days</c> are mocked,
/// swappable for the real counter later. <c>month</c> is 1-based (1 = January).
/// </summary>
public sealed record MonthlyWritesResponse(
    DayWrites[] Days,
    long MonthlyQuota,
    long MonthlyUsed,
    string MonthLabel,
    string QuotaResetsOn,
    int TrialDaysLeft,
    bool IsCurrent);

public sealed record DayWrites(string Label, string Date, long Writes);
