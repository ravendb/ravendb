namespace Raven.AiAppliance.Contracts;

/// <summary>
/// License surface for the <c>/settings/license</c> page — the prototype's
/// <c>getLicense()</c>. MOCK-backed for now (see <c>MockLicenseStatsProvider</c>):
/// the real signed license from the license API (RavenDB-26661/26783) isn't wired
/// yet, and the appliance's local <c>ApplianceLicense</c> only carries Id/Name/Keys.
/// </summary>
public sealed record LicenseResponse(
    string State,
    string Tier,
    int DaysLeft,
    int DaysElapsed,
    int TrialLengthDays,
    string TrialStartedLabel,
    string TrialEndsLabel,
    int? GraceHoursLeft,
    string? GraceEndsLabel,
    string Api,
    bool ApiHealthy,
    bool ConnectivityOK,
    bool TierHealthy,
    string LastRefreshedLabel,
    LicensePlan[] Plans,
    string[] Includes,
    string[]? Stops,
    string[]? Keeps);

public sealed record LicensePlan(
    string Slug,
    string Name,
    string Tagline,
    string PriceLabel,
    string PriceSuffix,
    bool Featured,
    string[] Features);
