using Xunit;
using Xunit.Sdk;

namespace Tests.Infrastructure;

[TraitDiscoverer("Tests.Infrastructure.XunitExtensions.RavenTraitDiscoverer", "Tests.Infrastructure")]
public class RavenTheoryAttribute : TheoryAttribute, ITraitAttribute
{
    internal const string CoraxSkipMessage = $"Corax tests are skipped on v5.4";

    public readonly RavenTestCategory Category;
    private string _skip;

    public RavenTheoryAttribute(RavenTestCategory category)
    {
        Category = category;
    }

    public bool LicenseRequired { get; set; }

    public bool NightlyBuildRequired { get; set; }

    public bool S3Required { get; set; }

    public bool AzureRequired { get; set; }

    public override string Skip
    {
        get
        {
            var skip = _skip;
            if (skip != null)
                return skip;

            if (Category.HasFlag(RavenTestCategory.Corax))
            {
                return CoraxSkipMessage;
            }

            if (LicenseRequiredFactAttribute.ShouldSkip(LicenseRequired))
                return LicenseRequiredFactAttribute.SkipMessage;

            if (NightlyBuildRequired && NightlyBuildFactAttribute.ShouldSkip(out skip))
                return skip;

            if (S3Required && AmazonS3RetryTheoryAttribute.ShouldSkip(out skip))
                return skip;

            if (AzureRequired && AzureRetryTheoryAttribute.ShouldSkip(out skip))
                return skip;

            return null;
        }

        set => _skip = value;
    }
}
