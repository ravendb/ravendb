using Xunit;
using Xunit.Sdk;

namespace Tests.Infrastructure;

[TraitDiscoverer("Tests.Infrastructure.XunitExtensions.RavenTraitDiscoverer", "Tests.Infrastructure")]
public class RavenTheoryAttribute : TheoryAttribute, ITraitAttribute
{
    private string _skip;
    private readonly RavenTestCategory _category;

    public RavenTheoryAttribute(RavenTestCategory category)
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
            return ShouldSkip(_skip, _category, licenseRequired: LicenseRequired, nightlyBuildRequired: NightlyBuildRequired, s3Required: S3Required, azureRequired: AzureRequired);
        }

        set => _skip = value;
    }

    internal static string ShouldSkip(string skip, RavenTestCategory category, bool licenseRequired, bool nightlyBuildRequired, bool s3Required, bool azureRequired)
    {
        var s = RavenFactAttribute.ShouldSkip(skip, category, licenseRequired: licenseRequired, nightlyBuildRequired: nightlyBuildRequired);
        if (s != null)
            return s;

        if (s3Required && AmazonS3RetryTheoryAttribute.ShouldSkip(out skip))
            return skip;

        if (azureRequired && AzureRetryTheoryAttribute.ShouldSkip(out skip))
            return skip;

        return null;
    }

}
