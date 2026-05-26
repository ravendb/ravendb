using System;
using System.Collections.Generic;
using xRetry.v3;
using Xunit.v3;

namespace Tests.Infrastructure;


public class RavenRetryTheoryAttribute : RetryTheoryAttribute, ITraitAttribute, Xunit.v3.IFactAttribute
{
    string Xunit.v3.IFactAttribute.Skip => this.Skip;

    private string _skip;
    private readonly RavenTestCategory _category;
    public RavenRetryTheoryAttribute(RavenTestCategory category, int maxRetries = 3, int delayBetweenRetriesMs = 0)
    : base(maxRetries, delayBetweenRetriesMs)
    {
        _category = category;
    }

    public bool LicenseRequired { get; set; }

    public bool NightlyBuildRequired { get; set; }

    public bool S3Required { get; set; }

    public bool AzureRequired { get; set; }

    public RavenServiceRequirement Requires { get; set; } = RavenServiceRequirement.None;

    public IReadOnlyCollection<KeyValuePair<string, string>> GetTraits() =>
        XunitExtensions.RavenTraitHelper.GetTraitsFor(_category);

    public new string Skip
    {
        get
        {
            return RavenTheoryAttribute.ShouldSkip(_skip, _category, licenseRequired: LicenseRequired, nightlyBuildRequired: NightlyBuildRequired, serviceRequirement: Requires, s3Required: S3Required, azureRequired: AzureRequired);
        }

        set => _skip = value;
    }
}
