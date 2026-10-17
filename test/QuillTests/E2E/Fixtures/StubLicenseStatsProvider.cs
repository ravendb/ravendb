using Raven.Quill.Contracts;
using Raven.Quill.Licensing;

namespace QuillTests.E2E.Fixtures;

/// An <see cref="ILicenseStatsProvider"/> that answers usage from a canned payload (or throws when a test flips
/// <see cref="IsUsageUnavailable"/>) while still forwarding the license read to the real provider, so a test
/// server does not need a Quill license for the usage endpoints to work.
internal sealed class StubLicenseStatsProvider(ILicenseStatsProvider real) : ILicenseStatsProvider
{
    public static readonly QuillUsageResponse DefaultUsage = new(
        PerApplication:
        [
            new QuillApplicationUsage("topology-1", "support-copilot",
                new DateTime(2026, 5, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(2026, 5, 31, 23, 59, 59, DateTimeKind.Utc), 5200),
        ],
        ByPeriod:
        [
            new QuillPeriodUsage(new DateTime(2026, 5, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(2026, 5, 2, 0, 0, 0, DateTimeKind.Utc), 5200),
        ]);

    public QuillUsageResponse Usage { get; set; } = DefaultUsage;

    public bool IsUsageUnavailable { get; set; }

    public (int Year, int? Month, int? Day)? LastUsagePeriod { get; private set; }

    public Task<LicenseResponse> GetLicenseAsync(CancellationToken token) => real.GetLicenseAsync(token);

    public Task<QuillUsageResponse> GetUsageAsync(int year, int? month, int? day, CancellationToken token)
    {
        LastUsagePeriod = (year, month, day);
        if (IsUsageUnavailable)
            throw new LicenseUsageUnavailableException("stubbed license server outage");
        return Task.FromResult(Usage);
    }

    public void Reset()
    {
        Usage = DefaultUsage;
        IsUsageUnavailable = false;
        LastUsagePeriod = null;
    }
}
