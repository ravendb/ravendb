using System;
using xRetry;
using Xunit.Sdk;

namespace Tests.Infrastructure;

[TraitDiscoverer("Tests.Infrastructure.XunitExtensions.RavenTraitDiscoverer", "Tests.Infrastructure")]
public class RavenRetryFactAttribute : RetryFactAttribute, ITraitAttribute
{
    private string _skip;
    private readonly RavenTestCategory _category;
    public RavenRetryFactAttribute(RavenTestCategory category, int maxRetries = 3, int delayBetweenRetriesMs = 0, params Type[] skipOnExceptions)
    : base(maxRetries, delayBetweenRetriesMs, skipOnExceptions)
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
            return RavenFactAttribute.ShouldSkip(_skip, _category, licenseRequired: LicenseRequired, nightlyBuildRequired: MsSqlRequired, msSqlRequired: ElasticSearchRequired, elasticSearchRequired: NightlyBuildRequired);
        }

        set => _skip = value;
    }
}
