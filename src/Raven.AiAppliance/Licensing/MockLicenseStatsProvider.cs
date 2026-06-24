using System.Globalization;
using Raven.AiAppliance.Contracts;

namespace Raven.AiAppliance.Licensing;

/// <summary>
/// Mock <see cref="ILicenseStatsProvider"/> — deterministic stand-in for the real
/// license API. Mirrors the prototype's trial values/plans and a fixed write-quota
/// profile so the License &amp; Usage pages render with plausible data until the
/// real license API (RavenDB-26661/26783) and a per-DB write counter (gap #4) land.
/// </summary>
internal sealed class MockLicenseStatsProvider : ILicenseStatsProvider
{
    private const int TrialLengthDays = 90;
    private const long MonthlyQuota = 2_000_000;
    private const string Api = "api.ravendb.ai";

    private static readonly LicensePlan[] Plans =
    [
        new("developer", "Developer", "Single-app, low-volume", "$49", "/mo", false,
            ["1 app, 100K writes/mo", "BYO LLM key", "Community support"]),
        new("team", "Team", "Production workloads", "$499", "/mo", true,
            ["5 apps, 2M writes/mo", "BYO LLM key + local Ollama", "2h SLA support", "SSO & audit retention 1y"]),
        new("enterprise", "Enterprise", "Air-gapped, high-volume", "Custom", "", false,
            ["Unlimited apps & writes", "Air-gapped license JSON", "Dedicated CSM", "Custom contract terms"]),
    ];

    private static readonly string[] Includes =
    [
        "Unlimited databases — Add as many databases as your hardware allows.",
        "Unlimited writes — No write cap during trial.",
        "All channels — iframe widget, Telegram, WhatsApp.",
        "All AI providers — BYO key (Anthropic, OpenAI) or local Ollama.",
        "Full audit log — Same as paid plans during trial.",
        "Community support — Discord + docs · ~24h response.",
    ];

    // Per-day write samples (cycled) for the mocked monthly chart.
    private static readonly long[] WriteSamples =
    [
        28_000, 41_000, 75_000, 49_000, 37_000, 61_000, 28_000, 82_000, 53_000, 39_000,
        41_000, 75_000, 49_000, 37_000, 28_000, 22_000, 47_000, 53_000, 39_000, 41_000,
        75_000, 49_000, 88_000, 71_000, 42_000, 36_000, 18_000, 22_000, 47_000, 82_000, 64_000,
    ];

    public LicenseResponse GetLicense(string? demoState)
    {
        var state = (demoState ?? "healthy").ToLowerInvariant() switch
        {
            "expiring" => "expiring",
            "expired" => "expired",
            _ => "healthy",
        };

        return state switch
        {
            "expiring" => new LicenseResponse(
                State: "expiring", Tier: "Trial", DaysLeft: 5, DaysElapsed: 85, TrialLengthDays: TrialLengthDays,
                TrialStartedLabel: "Feb 23", TrialEndsLabel: "May 24, 2026", GraceHoursLeft: null, GraceEndsLabel: null,
                Api: Api, ApiHealthy: true, ConnectivityOK: true, TierHealthy: true, LastRefreshedLabel: "4 min ago",
                Plans: Plans, Includes: Includes, Stops: null, Keeps: null),

            "expired" => new LicenseResponse(
                State: "expired", Tier: "Expired", DaysLeft: 0, DaysElapsed: TrialLengthDays, TrialLengthDays: TrialLengthDays,
                TrialStartedLabel: "Apr 6", TrialEndsLabel: "Jul 5, 2026", GraceHoursLeft: 14, GraceEndsLabel: "Jul 6, 09:14 UTC",
                Api: Api, ApiHealthy: true, ConnectivityOK: true, TierHealthy: false, LastRefreshedLabel: "just now",
                Plans: Plans, Includes: Includes,
                Stops:
                [
                    "Agents stop answering — In 14h, on Jul 6, 09:14 UTC.",
                    "New conversations blocked — Visitors see a maintenance message.",
                    "Channels go offline — iframe widget, Telegram, WhatsApp stop replying.",
                ],
                Keeps:
                [
                    "Data sources keep streaming — Ingestion never pauses.",
                    "Conversations stay readable — History and audit log remain intact.",
                    "Reinstating is instant — Paste a paid key, agents resume immediately.",
                ]),

            _ => new LicenseResponse(
                State: "healthy", Tier: "Trial", DaysLeft: 47, DaysElapsed: 43, TrialLengthDays: TrialLengthDays,
                TrialStartedLabel: "Apr 6", TrialEndsLabel: "Jul 5, 2026", GraceHoursLeft: null, GraceEndsLabel: null,
                Api: Api, ApiHealthy: true, ConnectivityOK: true, TierHealthy: true, LastRefreshedLabel: "4 min ago",
                Plans: Plans, Includes: Includes, Stops: null, Keeps: null),
        };
    }

    public MonthlyWritesResponse GetMonthlyWrites(int year, int month, DateTime nowUtc)
    {
        var daysInMonth = DateTime.DaysInMonth(year, month);
        var isCurrent = year == nowUtc.Year && month == nowUtc.Month;

        var days = new DayWrites[daysInMonth];
        long used = 0;
        for (var d = 1; d <= daysInMonth; d++)
        {
            var date = new DateTime(year, month, d, 0, 0, 0, DateTimeKind.Utc);
            var inFuture = isCurrent && d > nowUtc.Day;
            var writes = inFuture ? 0 : WriteSamples[(d - 1) % WriteSamples.Length];
            used += writes;
            days[d - 1] = new DayWrites(date.ToString("MMM d", CultureInfo.InvariantCulture),
                date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), writes);
        }

        var resetsOn = new DateTime(year, month, 1, 0, 0, 0, DateTimeKind.Utc).AddMonths(1);
        return new MonthlyWritesResponse(
            Days: days,
            MonthlyQuota: MonthlyQuota,
            MonthlyUsed: used,
            MonthLabel: new DateTime(year, month, 1).ToString("MMMM yyyy", CultureInfo.InvariantCulture),
            QuotaResetsOn: resetsOn.ToString("MMMM d, yyyy", CultureInfo.InvariantCulture),
            TrialDaysLeft: 19,
            IsCurrent: isCurrent);
    }
}
