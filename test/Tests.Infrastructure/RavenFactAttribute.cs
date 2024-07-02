using System;
using xRetry;
using Xunit.Sdk;

namespace Tests.Infrastructure;

[TraitDiscoverer("Tests.Infrastructure.XunitExtensions.RavenTraitDiscoverer", "Tests.Infrastructure")]
public class RavenFactAttribute : RetryFactAttribute, ITraitAttribute
{
    private string _skip;
    private readonly RavenTestCategory _category;

    public RavenFactAttribute(RavenTestCategory category) : base(maxRetries: 1)
    {
        _category = category;
    }

    public RavenFactAttribute(RavenTestCategory category, bool retryable, int maxRetries = 3, int delayBetweenRetriesMs = 1000, params Type[] skipOnExceptions)
        : base(retryable ? maxRetries : 1, delayBetweenRetriesMs, skipOnExceptions)
    {
        _category = category;
    }

    public bool LicenseRequired { get; set; }

    public bool MsSqlRequired { get; set; }

    public bool ElasticSearchRequired { get; set; }

    public bool NightlyBuildRequired { get; set; }

    public override string Skip
    {
        get
        {
            return RavenFactSkip(_skip,  _category,  licenseRequired: LicenseRequired,  nightlyBuildRequired: NightlyBuildRequired,  msSqlRequired: MsSqlRequired,  elasticSearchRequired: ElasticSearchRequired);
        }

        set => _skip = value;
    }

    internal static string RavenFactSkip(string skip, RavenTestCategory category, bool licenseRequired, bool nightlyBuildRequired, bool msSqlRequired, bool elasticSearchRequired)
    {
        var s = RavenAttributeSkip(skip, category, licenseRequired: licenseRequired, nightlyBuildRequired: nightlyBuildRequired);
        if (s != null)
            return s;

        if (msSqlRequired && RequiresMsSqlFactAttribute.ShouldSkip(out skip))
            return skip;

        if (elasticSearchRequired && RequiresElasticSearchRetryFactAttribute.ShouldSkip(out skip))
            return skip;

        return null;
    }

    internal static string RavenAttributeSkip(string skip, RavenTestCategory category, bool licenseRequired, bool nightlyBuildRequired)
    {
        if (skip != null)
            return skip;

        if (RavenDataAttributeBase.Is32Bit)
        {
            if (category.HasFlag(RavenTestCategory.Sharding))
                return RavenDataAttributeBase.ShardingSkipMessage;
        }

        if (licenseRequired && LicenseRequiredFactAttribute.ShouldSkip())
            return LicenseRequiredFactAttribute.SkipMessage;

        if (nightlyBuildRequired && NightlyBuildFactAttribute.ShouldSkip(out skip))
            return skip;

        return null;
    }
}
