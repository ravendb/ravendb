using System;
using xRetry;
using Xunit.Sdk;

namespace Tests.Infrastructure;

[TraitDiscoverer("Tests.Infrastructure.XunitExtensions.RavenTraitDiscoverer", "Tests.Infrastructure")]
public class RavenRetryTheoryAttribute : RetryTheoryAttribute, ITraitAttribute
{
    private string _skip;
    private readonly RavenTestCategory _category;
    public RavenRetryTheoryAttribute(RavenTestCategory category, int maxRetries = 3, int delayBetweenRetriesMs = 0, params Type[] skipOnExceptions)
    : base(maxRetries, delayBetweenRetriesMs, skipOnExceptions)
    {
        _category = category;
    }

    public bool LicenseRequired { get; set; }

    public bool NightlyBuildRequired { get; set; }

    public bool S3Required { get; set; }

    public bool AzureRequired { get; set; }

    public override string Skip
    {
        get
        {
            return RavenTheoryAttribute.RavenTheorySkip(_skip, _category, licenseRequired: LicenseRequired, nightlyBuildRequired: NightlyBuildRequired, s3Required: S3Required, azureRequired: AzureRequired);
        }

        set => _skip = value;
    }
}
