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
            var skip = _skip;
            if (skip != null)
                return skip;

            if (RavenDataAttributeBase.Is32Bit)
            {
                if (_category.HasFlag(RavenTestCategory.Sharding))
                    return RavenDataAttributeBase.ShardingSkipMessage;
            }

            if (LicenseRequired && LicenseRequiredFactAttribute.ShouldSkip())
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
