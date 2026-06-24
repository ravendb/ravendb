using Raven.AiAppliance.Contracts;

namespace Raven.AiAppliance.Licensing;

/// <summary>
/// Source for the License &amp; Usage pages. Implemented today by
/// <see cref="MockLicenseStatsProvider"/> (the real signed license from the
/// license API — RavenDB-26661/26783 — isn't wired yet); swap the registration in
/// <c>Program.cs</c> when it lands.
/// </summary>
public interface ILicenseStatsProvider
{
    /// <param name="demoState">Optional demo override: <c>healthy</c> (default) /
    /// <c>expiring</c> / <c>expired</c> — mirrors the prototype's demo switcher.</param>
    LicenseResponse GetLicense(string? demoState);

    /// <param name="month">1-based month (1 = January).</param>
    MonthlyWritesResponse GetMonthlyWrites(int year, int month, DateTime nowUtc);
}
